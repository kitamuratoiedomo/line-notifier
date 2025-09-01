# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視・通知バッチ（完全修正版 v2025-09-01A）
- 発走時刻 = listページから抽出（detailは使わない）
- 通知基準 = 発走時刻 - CUTOFF_OFFSET_MIN
- 窓判定 = target ± (WINDOW_BEFORE/AFTER_MIN) ± GRACE_SECONDS
- 通知: 窓内1回のみ（TTL管理）/ Google Sheets 永続化
- ログ: notify_log と bets を両方記録
- 券種: 常に「三連単」で記録
"""

import os, re, json, time, random, logging, socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any

import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from strategy_rules import eval_strategy

# ===== JSTユーティリティ =====
JST = timezone(timedelta(hours=9))
def jst_now() -> datetime: return datetime.now(JST)
def jst_today() -> str: return jst_now().strftime("%Y%m%d")

# ===== ENV =====
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "ja,en-US;q=0.9",
})
TIMEOUT = (10, 25); RETRY = 3; SLEEP_BETWEEN = (0.6, 1.2)

LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"

START_HOUR = int(os.getenv("START_HOUR", "10"))
END_HOUR   = int(os.getenv("END_HOUR",   "22"))
DRY_RUN    = os.getenv("DRY_RUN", "False").lower() == "true"
FORCE_RUN  = os.getenv("FORCE_RUN", "0") == "1"

NOTIFY_ENABLED      = os.getenv("NOTIFY_ENABLED", "1") == "1"
NOTIFY_TTL_SEC      = int(os.getenv("NOTIFY_TTL_SEC", "3600"))
NOTIFY_COOLDOWN_SEC = int(os.getenv("NOTIFY_COOLDOWN_SEC", "1800"))

CUTOFF_OFFSET_MIN   = int(os.getenv("CUTOFF_OFFSET_MIN", "12"))
WINDOW_BEFORE_MIN   = int(os.getenv("WINDOW_BEFORE_MIN", "3"))
WINDOW_AFTER_MIN    = int(os.getenv("WINDOW_AFTER_MIN", "2"))
GRACE_SECONDS       = int(os.getenv("GRACE_SECONDS", "0"))

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "")
LINE_USER_ID      = os.getenv("LINE_USER_ID", "")

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_SHEET_ID   = os.getenv("GOOGLE_SHEET_ID", "")
SHEET_NOTIFY_LOG_TAB  = os.getenv("SHEET_NOTIFY_LOG_TAB", "notify_log")
BETS_SHEET_TAB    = os.getenv("BETS_SHEET_TAB", "bets")

DAILY_SUMMARY_HHMM = os.getenv("DAILY_SUMMARY_HHMM", "21:02")
ALWAYS_NOTIFY_DAILY_SUMMARY = os.getenv("ALWAYS_NOTIFY_DAILY_SUMMARY", "1") == "1"

UNIT_STAKE_YEN = int(os.getenv("UNIT_STAKE_YEN", "100"))
DEBUG_RACEIDS  = [s.strip() for s in os.getenv("DEBUG_RACEIDS","").split(",") if s.strip()]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ===== Google Sheets 基本 =====
def _sheet_service():
    info  = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets","v4",credentials=creds, cache_discovery=False)

def _resolve_sheet_title(svc, tab: str) -> str:
    meta = svc.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == tab:
            return tab
    svc.spreadsheets().batchUpdate(
        spreadsheetId=GOOGLE_SHEET_ID,
        body={"requests":[{"addSheet":{"properties":{"title": tab}}}]}
    ).execute()
    return tab

def _sheet_get(svc, title: str, a1: str) -> List[List[str]]:
    res = svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"'{title}'!{a1}").execute()
    return res.get("values", [])

def _sheet_put(svc, title: str, a1: str, values: List[List[str]]):
    svc.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID, range=f"'{title}'!{a1}",
        valueInputOption="RAW", body={"values": values}
    ).execute()

# ===== notify_log 追記 =====
def _notify_log_header():
    return ["date","ts_epoch","race_id","venue","race_no","strategy_id",
            "target_hhmm","window_from","window_to","send_ok","send_last","url"]

def sheet_append_notify_log(date_ymd:str, ts:float, race_id:str, venue:str, race_no:str,
                            strategy_id:str, target:str, win_from:str, win_to:str,
                            send_ok:int, send_last:str, url:str):
    svc   = _sheet_service()
    title = _resolve_sheet_title(svc, SHEET_NOTIFY_LOG_TAB)
    rows  = _sheet_get(svc, title, "A:L") or []
    if not rows: rows = [_notify_log_header()]
    rows.append([date_ymd, str(ts), race_id, venue, race_no, strategy_id,
                 target, win_from, win_to, str(send_ok), send_last[:160], url])
    _sheet_put(svc, title, "A:L", rows)

# ===== bets 追記（常に三連単で記録） =====
def _bets_header():
    return ["date","race_id","venue","race_no","strategy_id","bet_kind","tickets_umaban_csv","points","unit_stake","total_stake"]

def sheet_append_bet_record(date_ymd:str, race_id:str, venue:str, race_no:str, strategy_id:str, tickets_umaban:List[str]):
    svc=_sheet_service(); title=_resolve_sheet_title(svc, BETS_SHEET_TAB)
    rows=_sheet_get(svc, title, "A:J") or []
    if not rows: rows=[_bets_header()]
    points=len(tickets_umaban); unit=UNIT_STAKE_YEN; total=points*unit
    rows.append([date_ymd, race_id, venue, race_no, strategy_id, "三連単",
                 ",".join(tickets_umaban), str(points), str(unit), str(total)])
    _sheet_put(svc, title, "A:J", rows)

# ===== 発走時刻抽出（list専用） =====
RACEID_RE   = re.compile(r"/RACEID/(\d{18})")
PLACEHOLDER = re.compile(r"\d{8}0000000000$")

def fetch(url:str) -> str:
    last=None
    for i in range(1, RETRY+1):
        try:
            r=SESSION.get(url, timeout=TIMEOUT); r.raise_for_status()
            r.encoding="utf-8"; return r.text
        except Exception as e:
            last=e; time.sleep(random.uniform(*SLEEP_BETWEEN))
    raise last

def _extract_rids_from_html(html: str) -> list[str]:
    rids=set()
    soup=BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        m=RACEID_RE.search(a["href"])
        if m:
            rid=m.group(1)
            if not PLACEHOLDER.search(rid): rids.add(rid)
    return sorted(rids)

def list_raceids_today_and_next() -> list[str]:
    today = jst_today()
    y,m,d = int(today[:4]), int(today[4:6]), int(today[6:8])
    t0 = datetime(y,m,d,tzinfo=JST)
    next_ymd = (t0 + timedelta(days=1)).strftime("%Y%m%d")

    rids=[]
    for ymd in (today, next_ymd):
        url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
        try:
            html = fetch(url)
            rids += _extract_rids_from_html(html)
        except Exception as e:
            logging.warning("[WARN] RID一覧取得失敗: %s (%s)", e, url)
    return sorted(set(rids))

def _extract_start_hhmm_from_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    txt = soup.get_text(" ", strip=True)
    m = re.search(r'(?:発走|発走予定|発走時刻)\s*([0-2]?\d)[:：]([0-5]\d)', txt)
    if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
    m = re.search(r'([0-2]?\d)時([0-5]\d)分.*発走', txt)
    if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
    return None

def get_start_time_dt(rid: str) -> Optional[datetime]:
    url=f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{rid}"
    try:
        html=fetch(url)
        hhmm=_extract_start_hhmm_from_html(html)
        if hhmm:
            y,m,d = int(rid[:4]), int(rid[4:6]), int(rid[6:8])
            return datetime(y,m,d,int(hhmm[:2]),int(hhmm[3:]),tzinfo=JST)
    except Exception as e:
        logging.warning("[WARN] 発走抽出失敗 rid=%s err=%s", rid, e)
    return None
    
# ===== LINE送信 =====
def push_line_text(user_ids: List[str], message: str)->Tuple[int,str]:
    if DRY_RUN or not NOTIFY_ENABLED:
        logging.info("[DRY] LINE送信: %s", message.replace("\n"," / "))
        return 200,"DRY"
    if not LINE_ACCESS_TOKEN: return 0,"NO_TOKEN"
    headers={"Authorization": f"Bearer {LINE_ACCESS_TOKEN}", "Content-Type":"application/json"}
    ok=0; last=""
    for uid in user_ids:
        body={"to": uid, "messages":[{"type":"text","text": message[:5000]}]}
        r=SESSION.post(LINE_PUSH_URL, headers=headers, json=body, timeout=TIMEOUT)
        last=f"{r.status_code} {r.text[:160]}"
        if r.status_code==200: ok+=1
        elif r.status_code==429: time.sleep(NOTIFY_COOLDOWN_SEC)
    return ok, last

# ===== 通知処理 =====
def process_race(rid:str, post_dt:datetime, meta:Dict, strat:Dict, target_dt:datetime):
    msg = build_line_notification(meta, strat, rid, target_dt, "list", meta.get("venue_race",""), meta.get("now",""))
    ok,last = push_line_text([LINE_USER_ID], msg)
    # notify_log
    m=re.search(r"\b(\d{1,2})R\b", meta.get("venue_race","") or "")
    race_no = (m.group(1)+"R") if m else ""
    win_from=(target_dt-timedelta(minutes=WINDOW_BEFORE_MIN)).strftime("%H:%M:%S")
    win_to=(target_dt+timedelta(minutes=WINDOW_AFTER_MIN)).strftime("%H:%M:%S")
    sheet_append_notify_log(jst_today(), time.time(), rid, meta.get("venue_race","").split()[0], race_no,
                            strat.get("id","Sx"), target_dt.strftime("%H:%M:%S"), win_from, win_to,
                            ok, str(last), meta.get("url",""))
    # bets（三連単固定）
    pop2num={h["pop"]:h.get("num") for h in meta["horses"]}
    def _to_umaban(tk:str)->str:
        try: a,b,c=[int(x) for x in tk.split("-")]
        except: return tk
        return f"{pop2num.get(a,'-')}-{pop2num.get(b,'-')}-{pop2num.get(c,'-')}"
    tickets_umaban=[_to_umaban(t) for t in strat.get("tickets",[])]
    sheet_append_bet_record(jst_today(), rid, meta.get("venue_race","").split()[0], race_no, strat.get("id","Sx"), tickets_umaban)

# ===== main =====
def main():
    logging.info("[BOOT] host=%s pid=%s", socket.gethostname(), os.getpid())
    rids = list_raceids_today_and_next()

    # candidates.json / ENV RIDS / DEBUG_RACEIDS も追加
    extra=[]
    try:
        p=Path("/var/data/candidates.json")
        if p.exists():
            data=json.loads(p.read_text())
            cand=[str(x.get("rid")).strip() for x in data if isinstance(x,dict) and x.get("rid")]
            extra += [rid for rid in cand if rid]
    except Exception as e:
        logging.warning("[CAND] file read fail: %s", e)
    env_rids=[s.strip() for s in (os.getenv("RIDS","") or "").split(",") if s.strip()]
    if env_rids: extra+=env_rids
    if DEBUG_RACEIDS: extra+=DEBUG_RACEIDS
    if extra: rids=sorted(set(rids+extra))

    if not rids:
        logging.info("[INFO] RIDが0件のため終了")
        return

    for rid in rids:
        post_dt=get_start_time_dt(rid)
        if not post_dt: continue
        target_dt=post_dt - timedelta(minutes=CUTOFF_OFFSET_MIN)
        now=jst_now()
        lo=target_dt - timedelta(minutes=WINDOW_BEFORE_MIN, seconds=GRACE_SECONDS)
        hi=target_dt + timedelta(minutes=WINDOW_AFTER_MIN,  seconds=GRACE_SECONDS)
        if not(lo<=now<=hi) and not FORCE_RUN: continue

        meta=check_tanfuku_page(rid)
        if not meta: continue
        strat=eval_strategy(meta["horses"], logger=logging)
        if not strat or not strat.get("match"): continue

        process_race(rid, post_dt, meta, strat, target_dt)

    logging.info("[INFO] ジョブ終了")

def run_watcher_forever(sleep_sec: int = 60):
    logging.info("[INFO] watcher.start (sleep=%ss)", sleep_sec)
    while True:
        try: main()
        except Exception as e: logging.exception("[FATAL] loop error: %s", e)
        time.sleep(max(10, sleep_sec))
