# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視・通知バッチ（近いレース優先処理版 v2025-09-04B）
- 発走時刻 = listページ基準、取れなければ開催一覧でRID近傍から抽出
- 通知基準 = 発走 - CUTOFF_OFFSET_MIN
- 窓判定   = target ± (WINDOW_BEFORE/AFTER_MIN) ± GRACE_SECONDS
- RID列挙  = 当日/翌日 + /var/data/candidates.json + ENV RIDS + DEBUG_RACEIDS
- 通知     = 窓内1回のみ（TTL: Google Sheets 'notified'）
- 記録     = notify_log と bets（betsは常に「三連単」）
- オッズ   = tanfuku → 失敗なら win でフォールバック、ヘッダ無し表にも対応
- ★ 近いレース（|target-now| が小さい順）を **優先処理**（取りこぼし低減）
"""

import os, re, json, time, random, logging, socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any

import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from strategy_rules import eval_strategy  # horses -> {match,id,label,tickets,...}

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

# 15〜10分前の通知窓（例）：CUTOFF=12 / BEFORE=3 / AFTER=2
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
GOOGLE_SHEET_TAB  = os.getenv("GOOGLE_SHEET_TAB", "notified")  # TTL保存先（tab名 or gid）

DAILY_SUMMARY_HHMM = os.getenv("DAILY_SUMMARY_HHMM", "21:02")
ALWAYS_NOTIFY_DAILY_SUMMARY = os.getenv("ALWAYS_NOTIFY_DAILY_SUMMARY", "1") == "1"

UNIT_STAKE_YEN = int(os.getenv("UNIT_STAKE_YEN", "100"))
DEBUG_RACEIDS  = [s.strip() for s in os.getenv("DEBUG_RACEIDS","").split(",") if s.strip()]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ===== fetchユーティリティ =====
def fetch(url: str) -> str:
    last = None
    for _ in range(RETRY):
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:
            last = e
            time.sleep(random.uniform(*SLEEP_BETWEEN))
    raise last

# ===== Google Sheets 基本処理 =====
def _sheet_service():
    if not GOOGLE_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        raise RuntimeError("Google Sheets 環境変数不足")
    info  = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets","v4",credentials=creds, cache_discovery=False)

def _resolve_sheet_title(svc, tab_or_gid: str) -> str:
    meta = svc.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    sheets = meta.get("sheets", [])
    if tab_or_gid.isdigit() and len(tab_or_gid)>3:
        gid = int(tab_or_gid)
        for s in sheets:
            if s["properties"]["sheetId"] == gid:
                return s["properties"]["title"]
    for s in sheets:
        if s["properties"]["title"] == tab_or_gid:
            return tab_or_gid
    svc.spreadsheets().batchUpdate(
        spreadsheetId=GOOGLE_SHEET_ID,
        body={"requests":[{"addSheet":{"properties":{"title": tab_or_gid}}}]}
    ).execute()
    return tab_or_gid

def _sheet_get(svc, title: str, a1: str) -> List[List[str]]:
    res = svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"'{title}'!{a1}").execute()
    return res.get("values", [])

def _sheet_put(svc, title: str, a1: str, values: List[List[str]]):
    svc.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID, range=f"'{title}'!{a1}",
        valueInputOption="RAW", body={"values": values}
    ).execute()

# ===== TTL（再送抑止） =====
def sheet_load_notified() -> Dict[str,float]:
    svc = _sheet_service(); title = _resolve_sheet_title(svc, GOOGLE_SHEET_TAB)
    values = _sheet_get(svc, title, "A:C")
    start = 1 if values and values[0] and str(values[0][0]).upper() in ("KEY","RACEID","RID","ID") else 0
    out: Dict[str,float] = {}
    for row in values[start:]:
        if not row or len(row)<2: continue
        key=str(row[0]).strip()
        try: out[key]=float(row[1])
        except: pass
    return out

def sheet_upsert_notified(key: str, ts: float, note: str="") -> None:
    svc = _sheet_service(); title = _resolve_sheet_title(svc, GOOGLE_SHEET_TAB)
    values = _sheet_get(svc, title, "A:C")
    header = ["KEY","TS_EPOCH","NOTE"]
    if not values:
        _sheet_put(svc, title, "A:C", [header, [key, ts, note]]); return
    start_row = 1 if values and values[0] and values[0][0] in header else 0
    found = None
    for i,row in enumerate(values[start_row:], start=start_row):
        if row and str(row[0]).strip()==key:
            found = i; break
    if found is None: values.append([key, ts, note])
    else:             values[found]=[key, ts, note]
    _sheet_put(svc, title, "A:C", values)

# ===== notify_log / bets =====
def _notify_log_header():
    return ["date","ts_epoch","race_id","venue","race_no","strategy_id",
            "target_hhmm","window_from","window_to","send_ok","send_last","url"]

def sheet_append_notify_log(date_ymd:str, ts:float, race_id:str, venue:str, race_no:str,
                            strategy_id:str, target:str, win_from:str, win_to:str,
                            send_ok:int, send_last:str, url:str):
    svc   = _sheet_service()
    title = _resolve_sheet_title(svc, os.getenv("SHEET_NOTIFY_LOG_TAB","notify_log"))
    rows  = _sheet_get(svc, title, "A:L") or []
    if not rows: rows = [_notify_log_header()]
    rows.append([date_ymd, str(ts), race_id, venue, race_no, strategy_id,
                 target, win_from, win_to, str(send_ok), (send_last or "")[:160], url or ""])
    _sheet_put(svc, title, "A:L", rows)

def _bets_header():
    return ["date","race_id","venue","race_no","strategy_id","bet_kind",
            "tickets_umaban_csv","points","unit_stake","total_stake"]

def sheet_append_bet_record(date_ymd:str, race_id:str, venue:str, race_no:str,
                            strategy_id:str, tickets_umaban:List[str]):
    svc=_sheet_service(); title=_resolve_sheet_title(svc, os.getenv("BETS_SHEET_TAB","bets"))
    rows=_sheet_get(svc, title, "A:J") or []
    if not rows: rows=[_bets_header()]
    points=len(tickets_umaban); unit=int(os.getenv("UNIT_STAKE_YEN","100") or "100"); total=points*unit
    rows.append([date_ymd, race_id, venue, race_no, strategy_id, "三連単",
                 ",".join(tickets_umaban), str(points), str(unit), str(total)])
    _sheet_put(svc, title, "A:J", rows)
    
# ===== RID列挙（当日/翌日） =====
RACEID_RE   = re.compile(r"/RACEID/(\d{18})")
PLACEHOLDER = re.compile(r"\d{8}0000000000$")

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
            logging.info("[GET] %s http=200 bytes=%s", url, len(html))
            rids += _extract_rids_from_html(html)
        except Exception as e:
            logging.warning("[WARN] RID一覧取得失敗: %s (%s)", e, url)
    rids = sorted(set(rids))
    logging.info("[RIDS] today+next=%d", len(rids))
    return rids

# ===== 発走時刻抽出（list専用 + 開催一覧近傍フォールバック） =====
def _extract_start_hhmm_from_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    txt = soup.get_text(" ", strip=True)
    m = re.search(r'(?:発走|発走予定|発走時刻)\s*([0-2]?\d)\s*[:：]\s*([0-5]\d)', txt)
    if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
    m = re.search(r'([0-2]?\d)\s*時\s*([0-5]\d)\s*分.*?(?:発走|発走予定|発走時刻)', txt)
    if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
    m = re.search(r'([0-2]?\d)\s*[:：]\s*([0-5]\d)', txt)
    if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
    return None

def _extract_start_hhmm_near_rid_from_daylist(html: str, rid: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    a = soup.find("a", href=re.compile(re.escape(rid)))
    if not a: return None
    ancestors = []
    node = a
    for _ in range(6):
        ancestors.append(node)
        node = getattr(node, "parent", None)
        if not node: break

    def _scan(el) -> Optional[str]:
        for t in el.find_all("time"):
            for attr in ("datetime","data-time","title","aria-label"):
                v=t.get(attr)
                if v:
                    m = re.search(r'([0-2]?\d)[:：]([0-5]\d)', str(v))
                    if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
            txt_t = t.get_text(" ", strip=True)
            m = re.search(r'([0-2]?\d)[:：]([0-5]\d)', txt_t)
            if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
        txt = el.get_text(" ", strip=True)
        for pat in [r'(?:発走|予定)\s*([0-2]?\d)[:：]([0-5]\d)',
                    r'([0-2]?\d)\s*時\s*([0-5]\d)\s*分',
                    r'([0-2]?\d)[:：]([0-5]\d)']:
            m = re.search(pat, txt)
            if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
        return None

    for host in ancestors:
        hhmm = _scan(host)
        if hhmm: return hhmm

    sibs = list(ancestors[0].parent.children) if ancestors and ancestors[0].parent else []
    if sibs:
        try: idx = sibs.index(ancestors[0])
        except ValueError: idx = -1
        rng = sibs[max(0, idx-2): idx] + sibs[idx+1: idx+3] if idx != -1 else []
        for s in rng:
            if not hasattr(s, "get_text"): continue
            hhmm = _scan(s)
            if hhmm: return hhmm
    return None

def get_start_time_dt(rid: str) -> Optional[datetime]:
    # A) 直接 list ページ
    url_list = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{rid}"
    try:
        html = fetch(url_list)
        hhmm = _extract_start_hhmm_from_html(html)
        if hhmm:
            y,m,d = int(rid[:4]), int(rid[4:6]), int(rid[6:8])
            return datetime(y,m,d,int(hhmm[:2]),int(hhmm[3:]), tzinfo=JST)
    except Exception as e:
        logging.warning("[WARN] list抽出失敗 rid=%s err=%s", rid, e)

    # B) 開催一覧（RID近傍）
    ymd = rid[:8]
    url_day = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    try:
        day_html = fetch(url_day)
        hhmm2 = _extract_start_hhmm_near_rid_from_daylist(day_html, rid)
        if hhmm2:
            y,m,d = int(ymd[:4]), int(ymd[4:6]), int(ymd[6:8])
            return datetime(y,m,d,int(hhmm2[:2]),int(hhmm2[3:]), tzinfo=JST)
    except Exception as e:
        logging.warning("[WARN] daylist近傍抽出失敗 rid=%s err=%s", rid, e)

    return None
    
# ===== オッズ（単勝）抽出：自動推論パーサ =====
def _as_float(text:str)->Optional[float]:
    if not text: return None
    t=text.replace(",", " ").strip()
    m=re.search(r"\d+(?:\.\d+)?", t)
    return float(m.group(0)) if m else None

def _find_popular_odds_table(soup:BeautifulSoup):
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2: 
            continue
        body = table.find("tbody") or table
        cand_rows = [tr for tr in body.find_all("tr") if tr.find_all(["td","th"])]
        cand_rows = cand_rows[:8]
        if len(cand_rows) < 2:
            continue
        max_cols = max(len(r.find_all(["td","th"])) for r in cand_rows)
        numeric_hits = [0]*max_cols
        float_hits   = [0]*max_cols
        for r in cand_rows:
            tds=r.find_all(["td","th"])
            for ci in range(max_cols):
                if ci>=len(tds): continue
                txt = tds[ci].get_text(" ", strip=True)
                if txt.isdigit(): numeric_hits[ci]+=1
                if re.search(r"\d+\.\d+", txt.replace(","," ")): float_hits[ci]+=1

        # 人気：連番傾向列
        pop_idx=None; best_seq=-1
        for ci in range(max_cols):
            last=0; ok=0
            for r in cand_rows:
                tds=r.find_all(["td","th"])
                if ci>=len(tds): continue
                txt=tds[ci].get_text(" ", strip=True)
                if txt.isdigit():
                    v=int(txt)
                    if v>last: ok+=1; last=v
            if ok>best_seq: best_seq=ok; pop_idx=ci

        # 単勝：小数点ヒット多い列
        win_idx=None
        if any(float_hits):
            win_idx=max(range(max_cols), key=lambda i: float_hits[i])
        else:
            order=sorted(range(max_cols), key=lambda i: numeric_hits[i], reverse=True)
            for ci in order:
                if ci!=pop_idx: win_idx=ci; break

        # 馬番：数値多い列から人気/単勝以外
        num_idx=None
        order=sorted(range(max_cols), key=lambda i: numeric_hits[i], reverse=True)
        for ci in order:
            if ci!=pop_idx and ci!=win_idx:
                num_idx=ci; break

        if pop_idx is None or win_idx is None: 
            continue
        return table, {"pop":pop_idx, "win":win_idx, "num":num_idx if num_idx is not None else -1}
    return None, {}

def parse_odds_table(soup:BeautifulSoup)->Tuple[List[Dict[str,float]], Optional[str], Optional[str]]:
    venue_race=(soup.find("h1").get_text(strip=True) if soup.find("h1") else None)
    nowtime=soup.select_one(".withUpdate .nowTime") or soup.select_one(".nowTime")
    now_label=nowtime.get_text(strip=True) if nowtime else None

    table, idx=_find_popular_odds_table(soup)
    if not table: return [], venue_race, now_label

    pop_idx=idx["pop"]; win_idx=idx["win"]; num_idx=idx.get("num",-1)
    horses=[]; body=table.find("tbody") or table
    for tr in body.find_all("tr"):
        tds=tr.find_all(["td","th"])
        if len(tds)<=max(pop_idx,win_idx): continue
        pop_txt=tds[pop_idx].get_text(" ", strip=True)
        win_txt=tds[win_idx].get_text(" ", strip=True)
        if not pop_txt.isdigit(): continue
        pop=int(pop_txt)
        odds=_as_float(win_txt)
        if odds is None: continue
        rec={"pop":pop, "odds":float(odds)}
        if 0<=num_idx<len(tds):
            num_txt=tds[num_idx].get_text(" ", strip=True)
            m=re.search(r"\d+", num_txt)
            if m: rec["num"]=int(m.group(0))
        horses.append(rec)

    uniq={}; 
    for h in sorted(horses, key=lambda x:x["pop"]): uniq[h["pop"]]=h
    horses=[uniq[k] for k in sorted(uniq.keys())]
    return horses, venue_race, now_label

def check_tanfuku_page(race_id: str)->Optional[Dict[str, Any]]:
    # A) tanfuku
    url1=f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    try:
        soup=BeautifulSoup(fetch(url1), "lxml")
        horses, venue_race, now_label = parse_odds_table(soup)
        if horses:
            return {"race_id":race_id,"url":url1,"horses":horses,"venue_race":venue_race or "地方競馬","now":now_label or ""}
    except Exception as e:
        logging.warning("[ODDS] tanfuku失敗 rid=%s err=%s", race_id, e)
    # B) win
    url2=f"https://keiba.rakuten.co.jp/odds/win/RACEID/{race_id}"
    try:
        soup=BeautifulSoup(fetch(url2), "lxml")
        horses, venue_race, now_label = parse_odds_table(soup)
        if horses:
            return {"race_id":race_id,"url":url2,"horses":horses,"venue_race":venue_race or "地方競馬","now":now_label or ""}
    except Exception as e:
        logging.warning("[ODDS] win失敗 rid=%s err=%s", race_id, e)
    return None

# ===== LINE送信 =====
def push_line_text(user_ids: List[str], message: str)->Tuple[int,str]:
    if DRY_RUN or not NOTIFY_ENABLED:
        logging.info("[DRY] LINE送信: %s", message.replace("\n"," / "))
        return 200,"DRY"
    if not LINE_ACCESS_TOKEN: return 0,"NO_TOKEN"
    headers={"Authorization": f"Bearer {LINE_ACCESS_TOKEN}", "Content-Type":"application/json"}
    ok=0; last=""
    targets = user_ids or [LINE_USER_ID]
    for uid in targets:
        body={"to": uid, "messages":[{"type":"text","text": message[:5000]}]}
        r=SESSION.post(LINE_PUSH_URL, headers=headers, json=body, timeout=TIMEOUT)
        last=f"{r.status_code} {r.text[:160]}"
        if r.status_code==200: ok+=1
        elif r.status_code==429: time.sleep(NOTIFY_COOLDOWN_SEC)
    return ok, last

# ===== 通知本文 =====
def build_line_notification(meta:Dict, strat:Dict, rid:str, target_dt:datetime, via:str, venue_race:str, now_label:str)->str:
    horses=meta.get("horses", [])
    url=meta.get("url","")
    strat_id=str(strat.get("id","Sx"))
    pop2num={h["pop"]:h.get("num") for h in horses if isinstance(h.get("pop"),int)}
    def _to_num(tk:str)->str:
        try: a,b,c=[int(x) for x in tk.split("-")]
        except: return tk
        return f"{pop2num.get(a,'-')}-{pop2num.get(b,'-')}-{pop2num.get(c,'-')}"
    tickets=strat.get("tickets",[]) or []
    head_pop=" / ".join(tickets[:8])+(" …" if len(tickets)>8 else "")
    head_num=" / ".join([_to_num(t) for t in tickets[:8]])+(" …" if len(tickets)>8 else "")
    lines=[f"{venue_race or ''} / RID:{rid[-6:]} / ターゲット={target_dt.strftime('%H:%M')}（via:{via}）",
           f"{strat.get('label','戦略')}",
           f"買い目（人気）: {head_pop}",
           f"買い目（馬番）: {head_num}",
           "", "上位オッズ:"]
    def _fmt(h): 
        num = f"{int(h.get('num'))}" if isinstance(h.get('num'),int) else "-"
        return f"  馬番{num}  単勝{h['odds']:.1f}倍（{h['pop']}人気）"
    for h in sorted(horses, key=lambda x:x.get("pop",999))[:5]: lines.append(_fmt(h))
    if now_label: lines.append(f"更新:{now_label}")
    if url: lines.append(url)
    return "\n".join(lines)

# ===== 通知処理（1件） =====
def process_race(rid:str, post_dt:datetime, meta:Dict, strat:Dict, target_dt:datetime):
    ok,last = push_line_text([LINE_USER_ID] + (LINE_USER_IDS or []),
                             build_line_notification(meta, strat, rid, target_dt, "list",
                                                     meta.get("venue_race",""), meta.get("now","")))
    # notify_log
    m=re.search(r"\b(\d{1,2})R\b", meta.get("venue_race","") or "")
    race_no = (m.group(1)+"R") if m else ""
    win_from=(target_dt-timedelta(minutes=WINDOW_BEFORE_MIN)).strftime("%H:%M:%S")
    win_to=(target_dt+timedelta(minutes=WINDOW_AFTER_MIN)).strftime("%H:%M:%S")
    sheet_append_notify_log(jst_today(), time.time(), rid, meta.get("venue_race","").split()[0], race_no,
                            strat.get("id","Sx"), target_dt.strftime("%H:%M:%S"),
                            win_from, win_to, ok, str(last), meta.get("url",""))
    # bets（三連単固定）
    pop2num={h["pop"]:h.get("num") for h in meta["horses"]}
    def _to_umaban(tk:str)->str:
        try: a,b,c=[int(x) for x in tk.split("-")]
        except: return tk
        return f"{pop2num.get(a,'-')}-{pop2num.get(b,'-')}-{pop2num.get(c,'-')}"
    tickets_umaban=[_to_umaban(t) for t in (strat.get("tickets",[]) or [])]
    sheet_append_bet_record(jst_today(), rid, meta.get("venue_race","").split()[0], race_no,
                            strat.get("id","Sx"), tickets_umaban)
    # TTL
    ttl_key=f"{rid}:{target_dt.strftime('%H%M')}:{strat.get('id','Sx')}"
    sheet_upsert_notified(ttl_key, time.time(), f"{meta.get('venue_race','')} {target_dt.strftime('%H:%M')}")

# ===== main（★近いレース優先） =====
def main():
    logging.info("[BOOT] host=%s pid=%s", socket.gethostname(), os.getpid())
    hour = jst_now().hour
    if not (START_HOUR <= hour <= END_HOUR) and not FORCE_RUN:
        logging.info("[INFO] 運用時間外: %02d-%02d", START_HOUR, END_HOUR)
        return

    # RID列挙
    rids = list_raceids_today_and_next()

    # 追加ソース（candidates.json / ENV RIDS / DEBUG_RACEIDS）
    extra=[]
    try:
        p=Path("/var/data/candidates.json")
        if p.exists():
            data=json.loads(p.read_text())
            cand=[str(x.get("rid")).strip() for x in data if isinstance(x,dict) and x.get("rid")]
            extra += [rid for rid in cand if rid]
            logging.info("[CAND] file=%d", len(cand))
    except Exception as e:
        logging.warning("[CAND] file read fail: %s", e)
    env_rids=[s.strip() for s in (os.getenv("RIDS","") or "").split(",") if s.strip()]
    if env_rids: extra+=env_rids; logging.info("[CAND] env=%d", len(env_rids))
    if DEBUG_RACEIDS: extra+=DEBUG_RACEIDS; logging.info("[CAND] debug=%d", len(DEBUG_RACEIDS))
    if extra:
        rids = sorted(set(rids + extra))
    if not rids:
        logging.info("[INFO] RIDが0件のため終了"); return

    notified = sheet_load_notified()

    # ★ target までの距離（秒）の昇順で優先度付け
    items = []
    now = jst_now()
    for rid in rids:
        post_dt = get_start_time_dt(rid)
        if not post_dt:
            continue
        target_dt = post_dt - timedelta(minutes=CUTOFF_OFFSET_MIN)
        delta = abs((target_dt - now).total_seconds())
        items.append((delta, rid, post_dt, target_dt))
    # 近いレースから処理（取りこぼし軽減）
    items.sort(key=lambda x: x[0])
    logging.info("[PRIO] queue=%d (closest Δ=%.1fs)", len(items), items[0][0] if items else -1)

    for _, rid, post_dt, target_dt in items:
        now = jst_now()
        lo  = target_dt - timedelta(minutes=WINDOW_BEFORE_MIN, seconds=GRACE_SECONDS)
        hi  = target_dt + timedelta(minutes=WINDOW_AFTER_MIN,  seconds=GRACE_SECONDS)
        ok  = (lo <= now <= hi) or FORCE_RUN
        logging.info("[WIND] rid=%s start=%s target=%s window=%s~%s ok=%s",
                     rid, post_dt.strftime("%H:%M"), target_dt.strftime("%H:%M"),
                     lo.strftime("%H:%M:%S"), hi.strftime("%H:%M:%S"), ok)
        if not ok:
            continue

        # TTL既送チェック
        recent = [k for k in notified if k.startswith(f"{rid}:{target_dt.strftime('%H%M')}")]
        if recent and not FORCE_RUN:
            logging.info("[DEDUP] TTL内スキップ: %s", recent[0]); 
            continue

        meta = check_tanfuku_page(rid)
        if not meta:
            logging.info("[SKIP] tanfuku/win パース失敗 rid=%s", rid)
            continue

        try:
            strat = eval_strategy(meta["horses"], logger=logging)
        except Exception as e:
            logging.warning("[WARN] eval_strategy 例外 rid=%s: %s", rid, e)
            continue
        if not strat or not strat.get("match"):
            continue

        process_race(rid, post_dt, meta, strat, target_dt)

    logging.info("[INFO] ジョブ終了")