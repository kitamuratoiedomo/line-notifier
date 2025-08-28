# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視・通知バッチ（安定版ベース + 払戻厳密突合 v2025-08-28S）
- 発走時刻：一覧ページ優先（必要時のみlist/tanfukuフォールバック）
- 窓判定：発走 - CUTOFF_OFFSET_MIN（分）をターゲットに、WINDOW_* で1回通知
- 通知：Google SheetでTTL永続 / 429はクールダウン
- 送信先：シート「1」のH列から userId を収集（ENVフォールバック可）
- bets：通知成立時に「券種・馬番チケット・点数・投資」をシートに追記
- 払戻：レース確定後に payoff ページと厳密突合して 的中率/回収率 を日次サマリで出力
"""

import os, re, json, time, random, logging, pathlib, hashlib
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Set

import requests
from bs4 import BeautifulSoup, Tag
from strategy_rules import eval_strategy

# ========= JST / 共通 =========
JST = timezone(timedelta(hours=9))
def now_jst() -> datetime: return datetime.now(JST)
def today_str() -> str:    return now_jst().strftime("%Y%m%d")

# ========= HTTP =========
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ja,en-US;q=0.9",
})
TIMEOUT = (10, 25)
RETRY   = 3
SLEEP_BETWEEN = (0.6, 1.2)

def fetch(url: str) -> str:
    last = None
    for i in range(1, RETRY+1):
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:
            last = e
            wait = random.uniform(*SLEEP_BETWEEN)
            logging.warning(f"[WARN] fetch失敗({i}/{RETRY}) {e} -> {wait:.1f}s待機: {url}")
            time.sleep(wait)
    raise last

# ========= ENV =========
START_HOUR  = int(os.getenv("START_HOUR", "10"))
END_HOUR    = int(os.getenv("END_HOUR",   "22"))
DRY_RUN     = os.getenv("DRY_RUN", "False").lower() == "true"
FORCE_RUN   = os.getenv("FORCE_RUN", "0") == "1"

NOTIFY_ENABLED      = os.getenv("NOTIFY_ENABLED", "1") == "1"
NOTIFY_TTL_SEC      = int(os.getenv("NOTIFY_TTL_SEC", "3600"))
NOTIFY_COOLDOWN_SEC = int(os.getenv("NOTIFY_COOLDOWN_SEC", "1800"))

# ★ 通知を“発走の n 分前”に固定したい場合 → CUTOFF_OFFSET_MIN=n
WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "0"))  # 通常0（ターゲット時刻ピンポイント）
WINDOW_AFTER_MIN  = int(os.getenv("WINDOW_AFTER_MIN",  "0"))  # 通常0（±GRACEで吸収）
CUTOFF_OFFSET_MIN = int(os.getenv("CUTOFF_OFFSET_MIN", "12")) # 例：常に発走12分前

GRACE_SECONDS = int(os.getenv("GRACE_SECONDS", "60"))

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "")
LINE_USER_ID      = os.getenv("LINE_USER_ID", "")
LINE_USER_IDS     = [s.strip() for s in os.getenv("LINE_USER_IDS", "").split(",") if s.strip()]

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_SHEET_ID   = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_SHEET_TAB  = os.getenv("GOOGLE_SHEET_TAB", "notified")

USERS_SHEET_NAME  = os.getenv("USERS_SHEET_NAME", "1")
USERS_USERID_COL  = os.getenv("USERS_USERID_COL", "H")

BETS_SHEET_TAB    = os.getenv("BETS_SHEET_TAB", "bets")
UNIT_STAKE_YEN    = int(os.getenv("UNIT_STAKE_YEN", "100"))
# 戦略→券種（日次サマリの突合に使用）
_DEFAULT_BET_KIND = {"S1":"馬連","S2":"馬単","S3":"三連単","S4":"三連複"}
try:
    STRATEGY_BET_KIND = json.loads(os.getenv("STRATEGY_BET_KIND_JSON","")) or _DEFAULT_BET_KIND
except Exception:
    STRATEGY_BET_KIND = _DEFAULT_BET_KIND

DAILY_SUMMARY_HHMM = os.getenv("DAILY_SUMMARY_HHMM", "21:02")
ALWAYS_NOTIFY_DAILY_SUMMARY = os.getenv("ALWAYS_NOTIFY_DAILY_SUMMARY","1") == "1"

DEBUG_RACEIDS = [s.strip() for s in os.getenv("DEBUG_RACEIDS", "").split(",") if s.strip()]

LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"

def within_operating_hours() -> bool:
    if FORCE_RUN: return True
    h = now_jst().hour
    return START_HOUR <= h < END_HOUR

# ========= Google Sheets =========
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

def _sheet_service():
    if not GOOGLE_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        raise RuntimeError("Google Sheets 環境変数不足")
    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets","v4",credentials=creds, cache_discovery=False)

def _resolve_sheet_title(svc, tab_or_gid: str) -> str:
    meta = svc.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    sheets = meta.get("sheets", [])
    # 数値(gid)はタイトルに解決
    if tab_or_gid.isdigit() and len(tab_or_gid)>3:
        gid = int(tab_or_gid)
        for s in sheets:
            if s["properties"]["sheetId"]==gid:
                return s["properties"]["title"]
        raise RuntimeError(f"指定gidシートなし: {gid}")
    for s in sheets:
        if s["properties"]["title"]==tab_or_gid:
            return tab_or_gid
    # 無ければ作成
    body={"requests":[{"addSheet":{"properties":{"title":tab_or_gid}}}]}
    svc.spreadsheets().batchUpdate(spreadsheetId=GOOGLE_SHEET_ID, body=body).execute()
    return tab_or_gid

def _sheet_get_range_values(svc, title: str, a1: str) -> List[List[str]]:
    res = svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"'{title}'!{a1}").execute()
    return res.get("values", [])

def _sheet_update_range_values(svc, title: str, a1: str, values: List[List[str]]):
    svc.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"'{title}'!{a1}",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

# TTL（通知済み）マップ
def sheet_load_notified() -> Dict[str, float]:
    svc   = _sheet_service(); title = _resolve_sheet_title(svc, GOOGLE_SHEET_TAB)
    rows  = _sheet_get_range_values(svc, title, "A:C")
    start = 1 if rows and rows[0] and str(rows[0][0]).upper() in ("KEY","RACEID","RID","ID") else 0
    d: Dict[str,float] = {}
    for r in rows[start:]:
        if not r or len(r)<2: continue
        k=str(r[0]).strip()
        try: d[k]=float(r[1])
        except: pass
    return d

def sheet_upsert_notified(key: str, ts: float, note: str="") -> None:
    svc   = _sheet_service(); title = _resolve_sheet_title(svc, GOOGLE_SHEET_TAB)
    rows  = _sheet_get_range_values(svc, title, "A:C") or []
    header = ["KEY","TS_EPOCH","NOTE"]
    if not rows:
        _sheet_update_range_values(svc, title, "A:C", [header, [key, ts, note]]); return
    start = 1 if rows and rows[0] and rows[0][0] in header else 0
    idx = None
    for i,r in enumerate(rows[start:], start=start):
        if r and str(r[0]).strip()==key: idx=i; break
    if idx is None: rows.append([key, ts, note])
    else: rows[idx]=[key, ts, note]
    _sheet_update_range_values(svc, title, "A:C", rows)

# 送信先ユーザー（タブ「1」のH列）
def load_user_ids_from_simple_col() -> List[str]:
    svc=_sheet_service(); title = _resolve_sheet_title(svc, USERS_SHEET_NAME)
    col=(USERS_USERID_COL or "H").upper()
    rows=_sheet_get_range_values(svc, title, f"{col}:{col}")
    uids: List[str] = []
    for i,r in enumerate(rows):
        v=(r[0].strip() if r and r[0] is not None else "")
        if not v: continue
        low=v.replace(" ","").lower()
        if i==0 and ("userid" in low or "user id" in low or "line" in low): continue
        if not v.startswith("U"): continue
        if v not in uids: uids.append(v)
    if not uids:
        fallback=[s.strip() for s in (os.getenv("LINE_USER_IDS","") or "").split(",") if s.strip()]
        if not fallback and LINE_USER_ID: fallback=[LINE_USER_ID]
        uids=fallback
    logging.info("[INFO] 送信ターゲット数: %d", len(uids))
    return uids

# ========= HTMLユーティリティ =========
RACEID_RE   = re.compile(r"/RACEID/(\d{18})")
PLACEHOLDER = re.compile(r"\d{8}0000000000$")
TIME_PATS = [
    re.compile(r"\b(\d{1,2}):(\d{2})\b"),
    re.compile(r"\b(\d{1,2})：(\d{2})\b"),
    re.compile(r"\b(\d{1,2})\s*時\s*(\d{1,2})\s*分\b"),
]

def _norm_hhmm_from_text(text: str) -> Optional[Tuple[int,int,str]]:
    if not text: return None
    s=str(text)
    for pat,tag in zip(TIME_PATS, ("half","full","kanji")):
        m=pat.search(s)
        if m:
            hh=int(m.group(1)); mm=int(m.group(2))
            if 0<=hh<=23 and 0<=mm<=59: return hh,mm,tag
    return None

def _make_dt_from_hhmm(rid: str, hh: int, mm: int) -> Optional[datetime]:
    try:
        y=int(rid[0:4]); m=int(rid[4:6]); d=int(rid[6:8])
        return datetime(y,m,d,hh,mm,tzinfo=JST)
    except: return None

def _find_time_nearby(el: Tag) -> Tuple[Optional[str], str]:
    t=el.find("time")
    if t:
        for attr in ("datetime","data-time","title","aria-label"):
            v=t.get(attr)
            if v:
                got=_norm_hhmm_from_text(v)
                if got:
                    hh,mm,why=got
                    return f"{hh:02d}:{mm:02d}", f"time@{attr}/{why}"
        got=_norm_hhmm_from_text(t.get_text(" ", strip=True))
        if got:
            hh,mm,why=got
            return f"{hh:02d}:{mm:02d}", f"time@text/{why}"
    for node in el.find_all(True, recursive=True):
        for attr in ("data-starttime","data-start-time","data-time","title","aria-label"):
            v=node.get(attr)
            if not v: continue
            got=_norm_hhmm_from_text(v)
            if got:
                hh,mm,why=got
                return f"{hh:02d}:{mm:02d}", f"data:{attr}/{why}"
    for sel in [".startTime",".cellStartTime",".raceTime",".time",".start-time"]:
        node=el.select_one(sel)
        if node:
            got=_norm_hhmm_from_text(node.get_text(" ", strip=True))
            if got:
                hh,mm,why=got
                return f"{hh:02d}:{mm:02d}", f"sel:{sel}/{why}"
    got=_norm_hhmm_from_text(el.get_text(" ", strip=True))
    if got:
        hh,mm,why=got
        return f"{hh:02d}:{mm:02d}", f"row:text/{why}"
    return None, "-"
    
# ========= 発走時刻（一覧ページ）解析 / 収集 =========
def _extract_raceids_from_soup(soup: BeautifulSoup) -> List[str]:
    rids=[]
    for a in soup.find_all("a", href=True):
        m=RACEID_RE.search(a["href"])
        if m:
            rid=m.group(1)
            if not PLACEHOLDER.search(rid):
                rids.append(rid)
    return sorted(set(rids))

def parse_post_times_from_table_like(root: Tag) -> Dict[str, datetime]:
    post_map={}
    # テーブル
    for table in root.find_all("table"):
        thead=table.find("thead")
        if thead:
            head_text="".join(thead.stripped_strings)
            if not any(k in head_text for k in ("発走","発走時刻","レース")):
                continue
        body=table.find("tbody") or table
        for tr in body.find_all("tr"):
            rid=None
            link=tr.find("a", href=True)
            if link:
                m=RACEID_RE.search(link["href"])
                if m: rid=m.group(1)
            if not rid or PLACEHOLDER.search(rid): continue
            hhmm,_=_find_time_nearby(tr)
            if not hhmm: continue
            hh,mm=map(int, hhmm.split(":"))
            dt=_make_dt_from_hhmm(rid, hh, mm)
            if dt: post_map[rid]=dt
    # カード型
    for a in root.find_all("a", href=True):
        m=RACEID_RE.search(a["href"])
        if not m: continue
        rid=m.group(1)
        if PLACEHOLDER.search(rid) or rid in post_map: continue
        # 近い先祖
        host=None; depth=0
        for parent in a.parents:
            if isinstance(parent, Tag) and parent.name in ("tr","li","div","section","article"):
                host=parent; break
            depth+=1
            if depth>=6: break
        host=host or a
        hhmm,_=_find_time_nearby(host)
        if not hhmm:
            sib_text=" ".join([x.get_text(" ", strip=True) for x in a.find_all_next(limit=4) if isinstance(x, Tag)])
            got=_norm_hhmm_from_text(sib_text)
            if got:
                hh,mm,_=got
                hhmm=f"{hh:02d}:{mm:02d}"
        if not hhmm: continue
        hh,mm=map(int, hhmm.split(":"))
        dt=_make_dt_from_hhmm(rid, hh, mm)
        if dt: post_map[rid]=dt
    return post_map

def collect_post_time_map(ymd: str, ymd_next: str) -> Dict[str, datetime]:
    post_map={}
    def _merge(url: str, label: str):
        try:
            soup=BeautifulSoup(fetch(url),"lxml")
            post_map.update(parse_post_times_from_table_like(soup))
        except Exception as e:
            logging.warning(f"[WARN] 発走一覧読み込み失敗: {e} ({label})")
    _merge(f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000",   "today")
    _merge(f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd_next}0000000000","tomorrow")
    logging.info(f"[INFO] 発走時刻取得: {len(post_map)}件")
    return post_map

# ========= オッズ解析（人気/単勝/馬番） =========
def _clean(s: str) -> str: return re.sub(r"\s+","", s or "")
def _as_float(text: str) -> Optional[float]:
    if not text: return None
    t=text.replace(",","").strip()
    if "%" in t or "-" in t or "～" in t or "~" in t: return None
    m=re.search(r"\d+(?:\.\d+)?", t); return float(m.group(0)) if m else None
def _as_int(text: str) -> Optional[int]:
    if not text: return None
    m=re.search(r"\d+", text); return int(m.group(0)) if m else None

def _find_popular_odds_table(soup: BeautifulSoup) -> Tuple[Optional[BeautifulSoup], Dict[str,int]]:
    for table in soup.find_all("table"):
        thead=table.find("thead")
        if not thead: continue
        headers=[_clean(th.get_text()) for th in thead.find_all(["th","td"])]
        if not headers: continue
        pop_idx=win_idx=num_idx=None
        for i,h in enumerate(headers):
            if h in ("人気","順位") or ("人気" in h and "順" not in h): pop_idx=i; break
        win_c=[]
        for i,h in enumerate(headers):
            if ("複" in h) or ("率" in h) or ("%" in h): continue
            if   h=="単勝": win_c.append((0,i))
            elif "単勝" in h: win_c.append((1,i))
            elif "オッズ" in h: win_c.append((2,i))
        win_idx=sorted(win_c, key=lambda x:x[0])[0][1] if win_c else None
        for i,h in enumerate(headers):
            if "馬番" in h: num_idx=i; break
        if num_idx is None:
            for i,h in enumerate(headers):
                if ("馬" in h) and ("馬名" not in h) and (i!=pop_idx): num_idx=i; break
        if pop_idx is None or win_idx is None: continue
        # 妥当性（人気1→2…の昇順が2行以上）
        body=table.find("tbody") or table
        rows=body.find_all("tr")
        seq_ok,last=0,0
        for tr in rows[:6]:
            tds=tr.find_all(["td","th"])
            if len(tds)<=max(pop_idx,win_idx): continue
            s=tds[pop_idx].get_text(strip=True)
            if not s.isdigit(): break
            v=int(s)
            if v<=last: break
            last=v; seq_ok+=1
        if seq_ok>=2:
            return table, {"pop":pop_idx,"win":win_idx,"num":num_idx if num_idx is not None else -1}
    return None, {}

def parse_odds_table(soup: BeautifulSoup) -> Tuple[List[Dict[str,float]], Optional[str], Optional[str]]:
    venue = (soup.find("h1").get_text(strip=True) if soup.find("h1") else None)
    nowtime=soup.select_one(".withUpdate .nowTime") or soup.select_one(".nowTime")
    now_label=nowtime.get_text(strip=True) if nowtime else None
    table, idx = _find_popular_odds_table(soup)
    if not table: return [], venue, now_label
    pop_idx, win_idx, num_idx = idx["pop"], idx["win"], idx.get("num",-1)
    horses=[]
    body=table.find("tbody") or table
    for tr in body.find_all("tr"):
        tds=tr.find_all(["td","th"])
        if len(tds)<=max(pop_idx,win_idx): continue
        pop_txt=tds[pop_idx].get_text(strip=True)
        if not pop_txt.isdigit(): continue
        pop=int(pop_txt)
        if not (1<=pop<=30): continue
        odds=_as_float(tds[win_idx].get_text(" ", strip=True))
        if odds is None: continue
        rec={"pop":pop,"odds":float(odds)}
        if 0<=num_idx<len(tds):
            num=_as_int(tds[num_idx].get_text(" ", strip=True))
            if num is not None: rec["num"]=num
        horses.append(rec)
    uniq={}
    for h in sorted(horses, key=lambda x:x["pop"]): uniq[h["pop"]]=h
    horses=[uniq[k] for k in sorted(uniq.keys())]
    return horses, venue, now_label

def check_tanfuku_page(race_id: str) -> Optional[Dict]:
    url=f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    html=fetch(url); soup=BeautifulSoup(html,"lxml")
    horses, venue_race, now_label = parse_odds_table(soup)
    if not horses:
        logging.info(f"[INFO] オッズテーブル未検出: {url}")
        return None
    if not venue_race: venue_race="地方競馬"
    return {"race_id": race_id, "url": url, "horses": horses,
            "venue_race": venue_race, "now": now_label or ""}

# ========= RID列挙 / ターゲット時刻 =========
def list_raceids_today_and_next() -> Tuple[List[str], Dict[str, datetime]]:
    ymd=today_str()
    dt=datetime.strptime(ymd,"%Y%m%d").replace(tzinfo=JST)
    ymd_next=(dt+timedelta(days=1)).strftime("%Y%m%d")
    urls=[
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000",
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd_next}0000000000",
    ]
    rids=set()
    for u in urls:
        try:
            soup=BeautifulSoup(fetch(u),"lxml")
            rids.update(_extract_raceids_from_soup(soup))
        except Exception as e:
            logging.warning(f"[WARN] RID一覧取得失敗: {e} ({u})")
    post_map=collect_post_time_map(ymd, ymd_next)
    return sorted(rids), post_map

def target_dt_from_post(post_dt: datetime) -> datetime:
    """ターゲット時刻 = 発走 - CUTOFF_OFFSET_MIN（分）"""
    return post_dt - timedelta(minutes=CUTOFF_OFFSET_MIN)

def in_window(target_dt: datetime) -> bool:
    now=now_jst()
    start=target_dt - timedelta(minutes=WINDOW_BEFORE_MIN)
    end  =target_dt + timedelta(minutes=WINDOW_AFTER_MIN)
    return (start - timedelta(seconds=GRACE_SECONDS)) <= now <= (end + timedelta(seconds=GRACE_SECONDS))

# ========= LINE送信 =========
def push_line_text(to_user_ids: List[str], message: str) -> Tuple[int, str]:
    if DRY_RUN or not NOTIFY_ENABLED:
        logging.info("[DRY] %s", message.replace("\n"," / "))
        return 200, "DRY"
    if not LINE_ACCESS_TOKEN:
        return 0, "NO_TOKEN"
    headers={"Authorization": f"Bearer {LINE_ACCESS_TOKEN}", "Content-Type":"application/json"}
    ok=0; last=""
    for uid in to_user_ids:
        body={"to": uid, "messages":[{"type":"text","text": message[:5000]}]}
        try:
            r=SESSION.post(LINE_PUSH_URL, headers=headers, json=body, timeout=TIMEOUT)
            last=f"{r.status_code} {r.text[:160]}"
            if r.status_code==200: ok+=1
            elif r.status_code==429:
                logging.warning("[WARN] LINE 429 cooldown=%ss", NOTIFY_COOLDOWN_SEC)
                time.sleep(NOTIFY_COOLDOWN_SEC)
            else:
                logging.warning("[WARN] LINE送信失敗 uid=%s code=%s", uid, r.status_code)
        except Exception as e:
            last=str(e)
            logging.warning("[WARN] LINE送信例外 uid=%s e=%s", uid, e)
        time.sleep(0.1)
    return ok, last

# ========= 払戻スクレイピング / 正規化 / bets =========
_PAYOUT_KIND_KEYS = ["単勝","複勝","枠連","馬連","ワイド","馬単","三連複","三連単"]

def fetch_payoff_map(race_id:str) -> Dict[str, List[Tuple[str,int]]]:
    url=f"https://keiba.rakuten.co.jp/race/payoff/RACEID/{race_id}"
    html=fetch(url); soup=BeautifulSoup(html,"lxml")
    result: Dict[str,List[Tuple[str,int]]] = {}
    for kind in _PAYOUT_KIND_KEYS:
        blocks=soup.find_all(string=re.compile(kind))
        if not blocks: continue
        items=[]
        for b in blocks:
            box=getattr(b,"parent",None) or soup
            text=" ".join((box.get_text(" ", strip=True) or "").split())
            for m in re.finditer(r"(\d+(?:-\d+){0,2})\s*([\d,]+)\s*円", text):
                comb=m.group(1); pay=int(m.group(2).replace(",",""))
                items.append((comb, pay))
        if items: result[kind]=items
    return result

def _normalize_ticket_for_kind(ticket:str, kind:str) -> str:
    parts=[int(x) for x in ticket.split("-") if x.strip().isdigit()]
    if kind in ("馬連","三連複"):
        parts=sorted(parts)
    return "-".join(str(x) for x in parts)

def _bets_header() -> List[str]:
    return ["date","race_id","venue","race_no","strategy_id","bet_kind",
            "tickets_umaban_csv","points","unit_stake","total_stake"]

def sheet_append_bet_record(date_ymd:str, race_id:str, venue:str, race_no:str,
                            strategy_id:str, bet_kind:str, tickets_umaban:List[str]):
    svc=_sheet_service(); title=_resolve_sheet_title(svc, BETS_SHEET_TAB)
    rows=_sheet_get_range_values(svc, title, "A:J") or []
    if not rows: rows=[_bets_header()]
    pts=len(tickets_umaban); unit=UNIT_STAKE_YEN; total=pts*unit
    rows.append([date_ymd, race_id, venue, race_no, strategy_id, bet_kind,
                 ",".join(tickets_umaban), str(pts), str(unit), str(total)])
    _sheet_update_range_values(svc, title, "A:J", rows)

# ========= 通知文（人気+馬番表記） =========
_CIRCLED="①②③④⑤⑥⑦⑧⑨"
def _circled(n:int)->str: return _CIRCLED[n-1] if 1<=n<=9 else f"{n}."
def _parse_ticket_as_pops(ticket: str) -> List[int]:
    parts=[p.strip() for p in re.split(r"[-→>〜~]", str(ticket)) if p.strip()]
    pops=[]
    for p in parts:
        m=re.search(r"\d+", p)
        if m:
            try: pops.append(int(m.group(0)))
            except: pass
    return pops
def _format_bets_pop_and_umanum(bets: List[str], horses: List[Dict[str,float]]) -> List[str]:
    pop2num={h["pop"]:h.get("num") for h in horses if isinstance(h.get("pop"),int)}
    out=[]
    for bet in bets:
        pops=_parse_ticket_as_pops(bet)
        if not pops: out.append(bet); continue
        seg=[]
        for p in pops:
            n=pop2num.get(p)
            seg.append(f"{p}番人気（馬番 {n}）" if isinstance(n,int) else f"{p}番人気")
        out.append(" - ".join(seg))
    return out

def build_line_notification(venue_race: str, post_dt: datetime, tickets: List[str], horses: List[Dict], url: str) -> str:
    # ヘッダ
    m=re.search(r"\b(\d{1,2})R\b", venue_race or "")
    race_no=(m.group(1)+"R") if m else ""
    venue=(venue_race or "").split()[0] or "地方競馬"
    lines=[f"【戦略ヒット】{venue} {race_no}（発走 {post_dt.strftime('%H:%M')}）", ""]
    # 買い目（人気/馬番）
    pretty=_format_bets_pop_and_umanum(tickets, horses)
    lines.append("買い目:")
    for i,bet in enumerate(pretty,1):
        lines.append(f"{_circled(i)} {bet}")
    lines.append("")
    lines.append("上位オッズ:")
    for h in sorted(horses, key=lambda x:x.get("pop",999))[:5]:
        num = f"{int(h['num'])}" if isinstance(h.get('num'),int) else "-"
        odd = f"{h['odds']:.1f}" if isinstance(h.get('odds'),(int,float)) else "—"
        lines.append(f"  馬番{num}  単勝{odd}倍")
    if url: lines.append(url)
    return "\n".join(lines)
    
# ========= スキャン & 通知 =========
def scan_and_notify_once() -> Tuple[int,int]:
    if not within_operating_hours() and not FORCE_RUN:
        logging.info("[INFO] 運用時間外 %02d-%02d", START_HOUR, END_HOUR)
        return 0,0

    user_ids = load_user_ids_from_simple_col()
    notified = sheet_load_notified()
    hits=0; matches=0

    rids, post_map = list_raceids_today_and_next()
    for rid in DEBUG_RACEIDS:
        if rid and rid not in rids: rids.append(rid)

    for rid in rids:
        post_dt = post_map.get(rid)
        if not post_dt: continue
        tgt_dt  = target_dt_from_post(post_dt)
        if not in_window(tgt_dt) and not FORCE_RUN:
            continue

        meta = check_tanfuku_page(rid)
        if not meta: continue
        hits += 1

        # 戦略判定
        try:
            strat = eval_strategy(meta["horses"], logger=logging)
        except Exception as e:
            logging.warning("[WARN] eval_strategy例外 rid=%s: %s", rid, e)
            strat = {"match": False}

        if not strat or not strat.get("match"):
            continue

        # TTLキー: rid:HHMM:戦略ID
        strat_id = str(strat.get("id","S3"))
        ttl_key  = f"{rid}:{tgt_dt.strftime('%H%M')}:{strat_id}"
        last_ts  = notified.get(ttl_key, 0.0)
        if (time.time()-last_ts) < NOTIFY_TTL_SEC and not FORCE_RUN:
            continue

        # 通知本文
        tickets = strat.get("tickets", []) or []
        message = build_line_notification(meta["venue_race"], post_dt, tickets, meta["horses"], meta["url"])
        ok, last = push_line_text(user_ids, message)
        logging.info("[INFO] LINE push ok=%s last=%s", ok, last[:140])

        # TTL保存
        sheet_upsert_notified(ttl_key, time.time(), f"{meta['venue_race']} {tgt_dt.strftime('%H:%M')} (発走-オフセット{CUTOFF_OFFSET_MIN}分)")

        # bets追記（ROI用）— 人気→馬番に変換して保存（S3は馬番そのまま）
        try:
            pop2num={h["pop"]:h.get("num") for h in meta["horses"] if isinstance(h.get("pop"),int)}
            def _to_umaban(tk:str)->str:
                try:
                    a,b,c=[int(x) for x in tk.split("-")]
                    return f"{pop2num.get(a,'-')}-{pop2num.get(b,'-')}-{pop2num.get(c,'-')}"
                except: return tk
            tickets_umaban = tickets if strat_id=="S3" else [_to_umaban(t) for t in tickets]
            m=re.search(r"\b(\d{1,2})R\b", meta["venue_race"] or "")
            race_no=(m.group(1)+"R") if m else ""
            bet_kind = STRATEGY_BET_KIND.get(strat_id, "三連単")
            sheet_append_bet_record(today_str(), rid, (meta["venue_race"] or "").split()[0], race_no, strat_id, bet_kind, tickets_umaban)
        except Exception as e:
            logging.warning("[WARN] bets記録失敗 rid=%s: %s", rid, e)

        matches += 1
        time.sleep(0.3)

    return hits, matches

# ========= 日次サマリ（bets×払戻 厳密突合） =========
def _daily_summary_due(now: datetime) -> bool:
    if not ALWAYS_NOTIFY_DAILY_SUMMARY: return False
    if not re.match(r"^\d{1,2}:\d{2}$", DAILY_SUMMARY_HHMM): return False
    h,m = map(int, DAILY_SUMMARY_HHMM.split(":"))
    due = now.replace(hour=h, minute=m, second=0, microsecond=0)
    return abs((now - due).total_seconds()) <= 300  # ±5分

def summarize_today_and_notify():
    now=now_jst()
    if not _daily_summary_due(now): return
    key=f"DAILY_SUMMARY:{now.strftime('%Y%m%d')}"
    if key in sheet_load_notified():
        logging.info("[INFO] 日次サマリ既送信: %s", key); return

    svc=_sheet_service(); title=_resolve_sheet_title(svc, BETS_SHEET_TAB)
    rows=_sheet_get_range_values(svc, title, "A:J") or []
    body=rows[1:] if len(rows)>=2 else []
    today=now.strftime("%Y%m%d")
    recs=[r for r in body if len(r)>=10 and r[0]==today]

    per={ sid:{"races":0,"bets":0,"hits":0,"stake":0,"return":0} for sid in ("S1","S2","S3","S4") }
    seen=set()

    for r in recs:
        date_ymd,race_id,venue,race_no,sid,bet_kind,t_csv,points,unit,total=r[:10]
        sid=sid or "S3"
        if (race_id,sid) not in seen:
            per[sid]["races"]+=1; seen.add((race_id,sid))
        tickets=[t for t in (t_csv or "").split(",") if t]
        per[sid]["bets"]  += len(tickets)
        per[sid]["stake"] += (int(total) if str(total).isdigit() else len(tickets)*UNIT_STAKE_YEN)

        # 払戻ページ突合
        try:
            paymap=fetch_payoff_map(race_id)
        except Exception as e:
            logging.warning("[SUM] 払戻取得失敗 rid=%s: %s", race_id, e)
            continue
        winners={ _normalize_ticket_for_kind(comb, bet_kind): pay for (comb,pay) in paymap.get(bet_kind,[]) }
        for t in tickets:
            norm=_normalize_ticket_for_kind(t, bet_kind)
            if norm in winners:
                per[sid]["hits"]   += 1
                per[sid]["return"] += winners[norm]
        time.sleep(0.2)

    def pct(n,d): 
        try: return f"{(100.0*n/max(d,1)):.1f}%"
        except ZeroDivisionError: return "0.0%"

    total_stake = sum(v["stake"] for v in per.values())
    total_return= sum(v["return"] for v in per.values())

    def _fmt(sid,label):
        v=per[sid]
        return (f"{label}：該当{v['races']}R / 購入{v['bets']}点 / 的中{v['hits']}点\n"
                f"　的中率 {pct(v['hits'], v['bets'])} / 回収率 {pct(v['return'], v['stake'])} / "
                f"投資 {v['stake']:,}円 / 払戻 {v['return']:,}円")

    lines=[f"【日次サマリ】{now.strftime('%Y-%m-%d')}"]
    lines+=[_fmt("S1","①"), _fmt("S2","②"), _fmt("S3","③"), _fmt("S4","④"), ""]
    lines+=[f"合計：投資 {total_stake:,}円 / 払戻 {total_return:,}円",
            f"　的中率 {pct(sum(per[s]['hits'] for s in per), sum(per[s]['bets'] for s in per))} / 回収率 {pct(total_return, total_stake)}"]

    msg="\n".join(lines)
    uids=load_user_ids_from_simple_col()
    push_line_text(uids, msg)
    sheet_upsert_notified(key, time.time(), "daily summary")

# ========= main =========
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    p=pathlib.Path(__file__).resolve()
    sha=hashlib.sha1(p.read_bytes()).hexdigest()[:12]
    logging.info(f"[BOOT] now(JST)={now_jst().strftime('%Y-%m-%d %H:%M:%S %z')} sha={sha} CUT={CUTOFF_OFFSET_MIN}")

    hits, matches = scan_and_notify_once()
    logging.info("[INFO] HITS=%d / MATCHES=%d", hits, matches)
    summarize_today_and_notify()
    logging.info("[INFO] ジョブ終了")

if __name__ == "__main__":
    main()
