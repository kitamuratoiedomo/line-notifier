# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視・通知バッチ（発走-オフセット固定 v2025-08-31B）
- 発走時刻 = listページから厳密抽出（detailは使わない）
- 通知基準 = 発走時刻 - CUTOFF_OFFSET_MIN
- 窓判定 = [発走-15, 発走-10]を許容 (ENVで調整)
- 通知: 窓内1回のみ / Google Sheetsへログとbetsを記録
"""

import os, re, json, time, random, logging, socket
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Set, Any

import requests
from bs4 import BeautifulSoup, Tag
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
LINE_USER_IDS     = [s.strip() for s in os.getenv("LINE_USER_IDS","").split(",") if s.strip()]

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
    
# ===== per-RID 発走取得 =====
def _extract_start_hhmm_from_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    txt = soup.get_text(" ", strip=True)
    m = re.search(r'(?:発走|発走予定|発走時刻)\s*([0-2]\d)[:：]([0-5]\d)', txt)
    if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
    m = re.search(r'([0-2]\d)時([0-5]\d)分.*発走', txt)
    if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
    return None

def get_start_time_hhmm(rid: str) -> Optional[str]:
    url=f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{rid}"
    html=fetch(url)
    hhmm=_extract_start_hhmm_from_html(html)
    return hhmm

# ===== 通知送信＆記録 =====
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
        try:
            a,b,c=[int(x) for x in tk.split("-")]
            return f"{pop2num.get(a,'-')}-{pop2num.get(b,'-')}-{pop2num.get(c,'-')}"
        except: return tk
    tickets_umaban=[_to_umaban(t) for t in strat.get("tickets",[])]
    sheet_append_bet_record(jst_today(), rid, meta.get("venue_race","").split()[0], race_no, strat.get("id","Sx"), tickets_umaban)

# ===== main =====
def main():
    logging.info("[BOOT] host=%s pid=%s", socket.gethostname(), os.getpid())
    # RID列挙 → per-RID発走取得
    rids=[...]  # 略: listから取得
    post_map={rid: get_start_time_hhmm(rid) for rid in rids}
    # 判定 → 通知 → 記録
    for rid,post in post_map.items():
        ...
        process_race(rid, post_dt, meta, strat, target_dt)

