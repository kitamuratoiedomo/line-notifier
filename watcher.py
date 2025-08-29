# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視・通知バッチ（発走-オフセット固定版 v2025-08-29A）
- ターゲット = 発走時刻 - CUTOFF_OFFSET_MIN（締切は使わない）
- 窓判定 = ターゲット ± (WINDOW_BEFORE_MIN/AFTER_MIN, GRACE_SECONDS)
- 通知：窓内1回 / 429クールダウン / Google SheetでTTL永続
- 送信先：シート「1」のH列から userId を収集
- 払戻：払戻ページと買い目（馬番）を厳密突合して ROI/的中率（bets を日次サマリで集計）
- 追加：S1/S2/S4 かつ 4番人気の単勝>=15.0 のとき通知に「★★ビッグチャンスレース★★」見出しを挿入
"""

import os, re, json, time, random, logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Set

import requests
from bs4 import BeautifulSoup, Tag
from strategy_rules import eval_strategy

# ===== JST / 日付ユーティリティ =====
JST = timezone(timedelta(hours=9))
def jst_now()    -> datetime: return datetime.now(JST)
def jst_today()  -> str:      return jst_now().strftime("%Y%m%d")

# ===== ENV / 既定 =====
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0", "Accept-Language": "ja,en-US;q=0.9"})
TIMEOUT = (10, 25); RETRY = 3; SLEEP_BETWEEN = (0.6, 1.2)

LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"

START_HOUR = int(os.getenv("START_HOUR", "10"))
END_HOUR   = int(os.getenv("END_HOUR",   "22"))
DRY_RUN    = os.getenv("DRY_RUN", "False").lower() == "true"
FORCE_RUN  = os.getenv("FORCE_RUN", "0") == "1"

NOTIFY_ENABLED        = os.getenv("NOTIFY_ENABLED", "1") == "1"
NOTIFY_TTL_SEC        = int(os.getenv("NOTIFY_TTL_SEC", "3600"))
NOTIFY_COOLDOWN_SEC   = int(os.getenv("NOTIFY_COOLDOWN_SEC", "1800"))

# ※発走の“12分前”を狙うなら CUTOFF_OFFSET_MIN=12 を推奨
CUTOFF_OFFSET_MIN     = int(os.getenv("CUTOFF_OFFSET_MIN", "12"))
WINDOW_BEFORE_MIN     = int(os.getenv("WINDOW_BEFORE_MIN", "0"))   # 0でちょうど
WINDOW_AFTER_MIN      = int(os.getenv("WINDOW_AFTER_MIN",  "0"))   # 0でちょうど
GRACE_SECONDS         = int(os.getenv("GRACE_SECONDS",     "45"))  # 計測誤差/cronズレ緩衝

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "")
LINE_USER_ID      = os.getenv("LINE_USER_ID", "")
LINE_USER_IDS     = [s.strip() for s in os.getenv("LINE_USER_IDS", "").split(",") if s.strip()]

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_SHEET_ID   = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_SHEET_TAB  = os.getenv("GOOGLE_SHEET_TAB", "notified")  # TTL/フラグ保管タブ

USERS_SHEET_NAME  = os.getenv("USERS_SHEET_NAME", "1")
USERS_USERID_COL  = os.getenv("USERS_USERID_COL", "H")
BETS_SHEET_TAB    = os.getenv("BETS_SHEET_TAB", "bets")

DAILY_SUMMARY_HHMM = os.getenv("DAILY_SUMMARY_HHMM", "21:02")
ALWAYS_NOTIFY_DAILY_SUMMARY = os.getenv("ALWAYS_NOTIFY_DAILY_SUMMARY", "1") == "1"

UNIT_STAKE_YEN = int(os.getenv("UNIT_STAKE_YEN", "100"))
DEBUG_RACEIDS  = [s.strip() for s in os.getenv("DEBUG_RACEIDS","").split(",") if s.strip()]

# ===== Google Sheets 基本 =====
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

def _sheet_service():
    if not GOOGLE_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        raise RuntimeError("Google Sheets の環境変数不足")
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
        raise RuntimeError(f"指定gidのシートが見つかりません: {gid}")
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

# ===== users（送信先）読み込み =====
def load_user_ids_from_simple_col() -> List[str]:
    svc=_sheet_service(); title=USERS_SHEET_NAME; col=(USERS_USERID_COL or "H").upper()
    values=_sheet_get(svc, title, f"{col}:{col}")
    out=[]
    for i,row in enumerate(values):
        v=(row[0].strip() if row and row[0] else "")
        if not v: continue
        low=v.replace(" ","").lower()
        if i==0 and ("userid" in low or "line" in low): continue
        if v.startswith("U") and v not in out: out.append(v)
    if not out:
        fb=[s.strip() for s in os.getenv("LINE_USER_IDS","").split(",") if s.strip()]
        if not fb and LINE_USER_ID: fb=[LINE_USER_ID]
        out = fb
    logging.info("[INFO] 送信ターゲット数: %d", len(out))
    return out

# ===== HTMLユーティリティ =====
RACEID_RE   = re.compile(r"/RACEID/(\d{18})")
PLACEHOLDER = re.compile(r"\d{8}0000000000$")
TIME_PATS   = [re.compile(r"\b(\d{1,2}):(\d{2})\b"),
               re.compile(r"\b(\d{1,2})：(\d{2})\b"),
               re.compile(r"\b(\d{1,2})\s*時\s*(\d{1,2})\s*分\b")]

def within_operating_hours() -> bool:
    return True if FORCE_RUN else (START_HOUR <= jst_now().hour < END_HOUR)

def fetch(url: str) -> str:
    last=None
    for i in range(1, RETRY+1):
        try:
            r=SESSION.get(url, timeout=TIMEOUT); r.raise_for_status()
            r.encoding="utf-8"
            return r.text
        except Exception as e:
            last=e; time.sleep(random.uniform(*SLEEP_BETWEEN))
    raise last

def _rid_parts(rid:str)->Tuple[int,int,int]:
    return int(rid[:4]), int(rid[4:6]), int(rid[6:8])

def _norm_hhmm(text: str) -> Optional[Tuple[int,int,str]]:
    if not text: return None
    s=str(text)
    for pat,tag in zip(TIME_PATS,("half","full","kanji")):
        m=pat.search(s)
        if m:
            hh=int(m.group(1)); mm=int(m.group(2))
            if 0<=hh<=23 and 0<=mm<=59: return hh,mm,tag
    return None

def _mk_dt(rid:str, hh:int, mm:int)->Optional[datetime]:
    try:
        y,m,d = _rid_parts(rid)
        return datetime(y,m,d,hh,mm,tzinfo=JST)
    except: return None

# ===== 発走時刻 抽出（一覧ページ由来のみ） =====
def _find_time_nearby(el: Tag) -> Tuple[Optional[str], str]:
    t = el.find("time")
    if t:
        for attr in ("datetime","data-time","title","aria-label"):
            v=t.get(attr)
            if v:
                got=_norm_hhmm(v)
                if got: hh,mm,why=got; return f"{hh:02d}:{mm:02d}", f"time@{attr}/{why}"
        got=_norm_hhmm(t.get_text(" ", strip=True))
        if got: hh,mm,why=got; return f"{hh:02d}:{mm:02d}", f"time@text/{why}"
    for node in el.find_all(True, recursive=True):
        for attr in ("data-starttime","data-start-time","data-time","title","aria-label"):
            v=node.get(attr)
            if not v: continue
            got=_norm_hhmm(v)
            if got: hh,mm,why=got; return f"{hh:02d}:{mm:02d}", f"data:{attr}/{why}"
    for sel in [".startTime",".cellStartTime",".raceTime",".time",".start-time"]:
        node=el.select_one(sel)
        if node:
            got=_norm_hhmm(node.get_text(" ", strip=True))
            if got: hh,mm,why=got; return f"{hh:02d}:{mm:02d}", f"sel:{sel}/{why}"
    got=_norm_hhmm(el.get_text(" ", strip=True))
    if got: hh,mm,why=got; return f"{hh:02d}:{mm:02d}", f"row:text/{why}"
    return None, "-"

def _extract_rids(soup: BeautifulSoup)->List[str]:
    r=[]
    for a in soup.find_all("a", href=True):
        m=RACEID_RE.search(a["href"])
        if m:
            rid=m.group(1)
            if not PLACEHOLDER.search(rid): r.append(rid)
    return sorted(set(r))

def parse_post_times_from_list(root: Tag) -> Dict[str, datetime]:
    post_map={}
    # table
    for table in root.find_all("table"):
        thead=table.find("thead")
        if thead:
            head="".join(thead.stripped_strings)
            if not any(k in head for k in ("発走","発走時刻","レース")): continue
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
            dt=_mk_dt(rid, hh, mm)
            if dt: post_map[rid]=dt
    # card
    for a in root.find_all("a", href=True):
        m=RACEID_RE.search(a["href"])
        if not m: continue
        rid=m.group(1)
        if PLACEHOLDER.search(rid) or rid in post_map: continue
        host=None; depth=0
        for p in a.parents:
            if isinstance(p, Tag) and p.name in ("tr","li","div","section","article"):
                host=p; break
            depth+=1
            if depth>=6: break
        host=host or a
        hhmm,_=_find_time_nearby(host)
        if not hhmm:
            sib=" ".join([x.get_text(" ", strip=True) for x in a.find_all_next(limit=4) if isinstance(x,Tag)])
            got=_norm_hhmm(sib)
            if got: hh,mm,_=got; hhmm=f"{hh:02d}:{mm:02d}"
        if not hhmm: continue
        hh,mm=map(int, hhmm.split(":"))
        dt=_mk_dt(rid, hh, mm)
        if dt: post_map[rid]=dt
    return post_map

def collect_post_time_map(ymd:str, ymd_next:str)->Dict[str,datetime]:
    post_map={}
    for url in [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000",
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd_next}0000000000",
    ]:
        try:
            soup=BeautifulSoup(fetch(url),"lxml")
            post_map.update(parse_post_times_from_list(soup))
        except Exception as e:
            logging.warning(f"[WARN] 発走一覧読み込み失敗: {e} ({url})")
    logging.info(f"[INFO] 発走時刻取得: {len(post_map)}件")
    return post_map

# ==== オッズ解析（人気/馬番） ====
def _clean(s:str)->str: return re.sub(r"\s+","", s or "")
def _as_float(text:str)->Optional[float]:
    if not text: return None
    t=text.replace(",","").strip()
    if "%" in t or "-" in t or "～" in t or "~" in t: return None
    m=re.search(r"\d+(?:\.\d+)?", t); return float(m.group(0)) if m else None
def _as_int(text:str)->Optional[int]:
    if not text: return None
    m=re.search(r"\d+", text); return int(m.group(0)) if m else None

def _find_popular_odds_table(soup:BeautifulSoup)->Tuple[Optional[BeautifulSoup], Dict[str,int]]:
    for table in soup.find_all("table"):
        thead=table.find("thead"); 
        if not thead: continue
        headers=[_clean(th.get_text()) for th in thead.find_all(["th","td"])]
        if not headers: continue
        pop_idx=win_idx=num_idx=None
        for i,h in enumerate(headers):
            if h in ("人気","順位") or ("人気" in h and "順" not in h): pop_idx=i; break
        c=[]
        for i,h in enumerate(headers):
            if ("複" in h) or ("率" in h) or ("%" in h): continue
            if   h=="単勝": c.append((0,i))
            elif "単勝" in h: c.append((1,i))
            elif "オッズ" in h: c.append((2,i))
        win_idx = sorted(c, key=lambda x:x[0])[0][1] if c else None
        for i,h in enumerate(headers):
            if "馬番" in h: num_idx=i; break
        if num_idx is None:
            for i,h in enumerate(headers):
                if ("馬" in h) and ("馬名" not in h) and (i!=pop_idx):
                    num_idx=i; break
        if pop_idx is None or win_idx is None: continue
        # 妥当性チェック
        body=table.find("tbody") or table
        rows=body.find_all("tr")
        seq,last=0,0
        for tr in rows[:6]:
            tds=tr.find_all(["td","th"])
            if len(tds)<=max(pop_idx,win_idx): continue
            s=tds[pop_idx].get_text(strip=True)
            if not s.isdigit(): break
            v=int(s)
            if v<=last: break
            last=v; seq+=1
        if seq>=2:
            return table, {"pop":pop_idx,"win":win_idx,"num":num_idx if num_idx is not None else -1}
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
        pop_txt=tds[pop_idx].get_text(strip=True)
        if not pop_txt.isdigit(): continue
        pop=int(pop_txt)
        if not (1<=pop<=30): continue
        odds=_as_float(tds[win_idx].get_text(" ", strip=True))
        if odds is None: continue
        rec={"pop":pop, "odds":float(odds)}
        if 0<=num_idx<len(tds):
            num=_as_int(tds[num_idx].get_text(" ", strip=True))
            if num is not None: rec["num"]=num
        horses.append(rec)
    uniq={}
    for h in sorted(horses, key=lambda x:x["pop"]): uniq[h["pop"]]=h
    horses=[uniq[k] for k in sorted(uniq.keys())]
    return horses, venue_race, now_label

def check_tanfuku_page(race_id: str)->Optional[Dict]:
    url=f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    soup=BeautifulSoup(fetch(url),"lxml")
    horses, venue_race, now_label = parse_odds_table(soup)
    if not horses: return None
    if not venue_race: venue_race="地方競馬"
    return {"race_id":race_id,"url":url,"horses":horses,"venue_race":venue_race,"now":now_label or ""}

# ===== 通知組み立て（ビッグチャンス装飾対応） =====
def _fmt_horse(h:Dict)->str:
    num = f"{int(h['num'])}" if isinstance(h.get("num"),int) else "-"
    odds= f"{h['odds']:.1f}" if isinstance(h.get("odds"),(int,float)) else "—"
    return f"  馬番{num}  単勝{odds}倍"

def _o_map(horses: List[Dict]) -> Dict[int,float]:
    m={}
    for h in horses:
        try:
            p=int(h.get("pop")); o=float(h.get("odds"))
            if p not in m: m[p]=o
        except: pass
    return m

def _is_big_chance(strat_id: str, horses: List[Dict]) -> bool:
    if strat_id not in ("S1","S2","S4"): return False
    o=_o_map(horses); o4=o.get(4)
    try:
        return (o4 is not None) and (float(o4) >= 15.0)
    except:
        return False

def push_line_text(to_user_ids: List[str], message: str)->Tuple[int,str]:
    if DRY_RUN or not NOTIFY_ENABLED:
        logging.info("[DRY] LINE送信: %s", message.replace("\n"," / "))
        return 200,"DRY"
    if not LINE_ACCESS_TOKEN: return 0,"NO_TOKEN"
    headers={"Authorization": f"Bearer {LINE_ACCESS_TOKEN}", "Content-Type":"application/json"}
    ok=0; last=""
    for uid in to_user_ids:
        body={"to": uid, "messages":[{"type":"text","text": message[:5000]}]}
        try:
            r=SESSION.post(LINE_PUSH_URL, headers=headers, json=body, timeout=TIMEOUT)
            last=f"{r.status_code} {r.text[:160]}"
            if r.status_code==200: ok+=1
            elif r.status_code==429:
                time.sleep(NOTIFY_COOLDOWN_SEC)
            else:
                logging.warning("[WARN] LINE送信失敗 uid=%s code=%s body=%s", uid, r.status_code, r.text[:120])
        except Exception as e:
            last=str(e)
        time.sleep(0.1)
    return ok, last

def build_line_notification(result:Dict, strat:Dict, rid:str, target_dt:datetime, target_src:str, venue_race:str, now_label:str)->str:
    horses=result.get("horses", [])
    url=result.get("url","")
    strat_id=str(strat.get("id","S3"))

    # 人気→馬番
    pop2num={h["pop"]:h.get("num") for h in horses if isinstance(h.get("pop"),int)}
    def _to_num(tk:str)->str:
        try:
            a,b,c=[int(x) for x in tk.split("-")]
            return f"{pop2num.get(a,'-')}-{pop2num.get(b,'-')}-{pop2num.get(c,'-')}"
        except: return tk

    tickets=strat.get("tickets",[]) or []
    n=len(tickets)
    head_pop=" / ".join(tickets[:8])+(" …" if n>8 else "")
    head_num=" / ".join([_to_num(t) for t in tickets[:8]])+(" …" if n>8 else "")

    lines=[f"{venue_race} / RID:{rid[-6:]} / ターゲット={target_dt.strftime('%H:%M')}（{target_src}）"]

    # ★ビッグチャンスレース装飾
    if _is_big_chance(strat_id, horses):
        lines.append("★★ビッグチャンスレース★★")

    lines += [
        f"{strat.get('label','戦略')}",
        f"買い目（人気）: {head_pop}（全{n}点）",
        f"買い目（馬番）: {head_num}",
        "", "上位オッズ:"
    ]
    for h in sorted(horses, key=lambda x:x.get("pop",999))[:5]:
        lines.append(_fmt_horse(h))
    if now_label: lines.append(f"更新:{now_label}")
    if url:       lines.append(url)
    return "\n".join(lines)

# ===== 発走リスト・ターゲット決定・窓判定 =====
def list_raceids_today_and_next()->Tuple[List[str], Dict[str,datetime]]:
    today=jst_today()
    base=datetime.strptime(today,"%Y%m%d").replace(tzinfo=JST)
    ymd_next=(base+timedelta(days=1)).strftime("%Y%m%d")
    rids=set()
    for url in [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{today}0000000000",
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd_next}0000000000",
    ]:
        try:
            soup=BeautifulSoup(fetch(url),"lxml")
            rids.update(_extract_rids(soup))
        except Exception as e:
            logging.warning(f"[WARN] RID一覧取得失敗: {e} ({url})")
    post_map   = collect_post_time_map(today, ymd_next)
    return sorted(rids), post_map

def fallback_target_time(rid: str, post_map: Dict[str, datetime]) -> Tuple[Optional[datetime], str]:
    post = post_map.get(rid)
    if post:
        return post - timedelta(minutes=CUTOFF_OFFSET_MIN), "発走-オフセット固定"
    return None, "-"

def is_within_window(target_dt: datetime) -> bool:
    now = jst_now()
    start = target_dt - timedelta(minutes=WINDOW_BEFORE_MIN)
    end   = target_dt + timedelta(minutes=WINDOW_AFTER_MIN)
    return (start - timedelta(seconds=GRACE_SECONDS)) <= now <= (end + timedelta(seconds=GRACE_SECONDS))

# ===== 払戻取得 / bets 追記 / 日次サマリ =====
_PAYOUT_KIND_KEYS = ["単勝","複勝","枠連","馬連","ワイド","馬単","三連複","三連単"]

def fetch_payoff_map(race_id:str) -> Dict[str, List[Tuple[str,int]]]:
    url = f"https://keiba.rakuten.co.jp/race/payoff/RACEID/{race_id}"
    html=fetch(url); soup=BeautifulSoup(html,"lxml")
    result: Dict[str, List[Tuple[str,int]]] = {}
    for kind in _PAYOUT_KIND_KEYS:
        blocks = soup.find_all(string=re.compile(kind))
        if not blocks: continue
        items=[]
        for b in blocks:
            box=getattr(b,"parent",None) or soup
            text=" ".join((box.get_text(" ", strip=True) or "").split())
            for m in re.finditer(r"(\d+(?:-\d+){0,2})\s*([\d,]+)\s*円", text):
                comb=m.group(1); pay=int(m.group(2).replace(",",""))
                items.append((comb, pay))
        if items: result[kind] = items
    return result

def _normalize_ticket_for_kind(ticket:str, kind:str) -> str:
    parts=[int(x) for x in ticket.split("-") if x.strip().isdigit()]
    if kind in ("馬連","三連複"): parts=sorted(parts)
    return "-".join(str(x) for x in parts)

# betsシート
def _bets_header() -> List[str]:
    return ["date","race_id","venue","race_no","strategy_id","bet_kind","tickets_umaban_csv","points","unit_stake","total_stake"]

def sheet_append_bet_record(date_ymd:str, race_id:str, venue:str, race_no:str, strategy_id:str, bet_kind:str, tickets_umaban:List[str]):
    svc=_sheet_service(); title=_resolve_sheet_title(svc, BETS_SHEET_TAB)
    values=_sheet_get(svc, title, "A:J")
    if not values: values=[_bets_header()]
    points=len(tickets_umaban); unit=UNIT_STAKE_YEN; total=points*unit
    values.append([date_ymd, race_id, venue, race_no, strategy_id, bet_kind, ",".join(tickets_umaban), str(points), str(unit), str(total)])
    _sheet_put(svc, title, "A:J", values)

# ===== スキャン本体 =====
def _scan_and_notify_once()->Tuple[int,int]:
    if not within_operating_hours() and not FORCE_RUN:
        logging.info("[INFO] 運用時間外: %02d-%02d", START_HOUR, END_HOUR); return 0,0
    user_ids = load_user_ids_from_simple_col()
    notified = sheet_load_notified()
    hits=0; matches=0

    rids, post_map = list_raceids_today_and_next()
    for rid in DEBUG_RACEIDS:
        if rid and rid not in rids: rids.append(rid)

    for rid in rids:
        target_dt, src = fallback_target_time(rid, post_map)
        if not target_dt: continue
        if not is_within_window(target_dt) and not FORCE_RUN: continue

        meta = check_tanfuku_page(rid)
        if not meta: continue
        hits += 1

        # 判定
        try: strat = eval_strategy(meta["horses"], logger=logging)
        except Exception as e:
            logging.warning("[WARN] eval_strategy 例外: %s", e); continue
        if not strat or not strat.get("match"): continue

        strat_id=str(strat.get("id","S3"))
        ttl_key=f"{rid}:{target_dt.strftime('%H%M')}:{strat_id}"
        last_ts=notified.get(ttl_key, 0.0)
        if (time.time()-last_ts) < NOTIFY_TTL_SEC and not FORCE_RUN: continue

        # 通知
        msg = build_line_notification(meta, strat, rid, target_dt, src, meta.get("venue_race",""), meta.get("now",""))
        ok,last = push_line_text(user_ids, msg)
        logging.info("[INFO] LINE push ok=%s last=%s", ok, str(last)[:120])

        # TTLフラグ
        sheet_upsert_notified(ttl_key, time.time(), f"{meta.get('venue_race','')} {target_dt.strftime('%H:%M')} {src}")
        matches += 1

        # bets 追記（ROI用）— 人気→馬番変換
        try:
            pop2num={h["pop"]:h.get("num") for h in meta["horses"] if isinstance(h.get("pop"),int)}
            def _to_umaban(tk:str)->str:
                try:
                    a,b,c=[int(x) for x in tk.split("-")]
                    return f"{pop2num.get(a,'-')}-{pop2num.get(b,'-')}-{pop2num.get(c,'-')}"
                except: return tk
            raw_tickets = strat.get("tickets",[]) or []
            tickets_umaban = raw_tickets if strat_id=="S3" else [_to_umaban(t) for t in raw_tickets]
            m=re.search(r"\b(\d{1,2})R\b", meta.get("venue_race","")); race_no = (m.group(1)+"R") if m else ""
            # 券種は戦略→固定（要件に合わせて全戦略を三連単にするならここを固定）
            BET_KIND_MAP = {"S1":"馬連","S2":"馬単","S3":"三連単","S4":"三連複"}
            bet_kind = BET_KIND_MAP.get(strat_id, "三連単")
            sheet_append_bet_record(jst_today(), rid, meta.get("venue_race","").split()[0], race_no, strat_id, bet_kind, tickets_umaban)
        except Exception as e:
            logging.warning("[WARN] bets記録失敗 rid=%s: %s", rid, e)

        time.sleep(0.4)
    return hits, matches

# ===== 日次サマリ =====
def _daily_summary_due(now: datetime) -> bool:
    if not ALWAYS_NOTIFY_DAILY_SUMMARY: return False
    if not re.match(r"^\d{1,2}:\d{2}$", DAILY_SUMMARY_HHMM): return False
    h,m=map(int, DAILY_SUMMARY_HHMM.split(":"))
    due = now.replace(hour=h, minute=m, second=0, microsecond=0)
    return abs((now - due).total_seconds()) <= 300  # ±5分

def summarize_today_and_notify():
    now=jst_now()
    if not _daily_summary_due(now): return

    # 重複送信ガード
    key=f"DAILY_SUMMARY:{now.strftime('%Y%m%d')}"
    notified=sheet_load_notified()
    if key in notified:
        logging.info("[INFO] 日次サマリは既送信: %s", key); return

    # bets 読み込み
    svc=_sheet_service(); title=_resolve_sheet_title(svc, BETS_SHEET_TAB)
    rows=_sheet_get(svc, title, "A:J") or []
    body=rows[1:] if len(rows)>=2 else []
    today=now.strftime("%Y%m%d")
    recs=[r for r in body if len(r)>=10 and r[0]==today]

    per = { sid:{"races":0,"bets":0,"hits":0,"stake":0,"return":0} for sid in ("S1","S2","S3","S4") }
    seen: Set[Tuple[str,str]] = set()

    for r in recs:
        date_ymd, race_id, venue, race_no, strategy_id, bet_kind, t_csv, points, unit, total = r[:10]
        sid=strategy_id or "S3"
        if (race_id, sid) not in seen:
            per[sid]["races"] += 1
            seen.add((race_id, sid))
        tickets=[t for t in (t_csv or "").split(",") if t]
        per[sid]["bets"]  += len(tickets)
        per[sid]["stake"] += (int(total) if str(total).isdigit() else len(tickets)*UNIT_STAKE_YEN)

        # 払戻照合
        try:
            paymap=fetch_payoff_map(race_id)
        except Exception as e:
            logging.warning("[SUM] 払戻取得失敗 rid=%s: %s", race_id, e); continue
        winners={ _normalize_ticket_for_kind(comb, bet_kind): pay for (comb, pay) in paymap.get(bet_kind, []) }
        for t in tickets:
            norm=_normalize_ticket_for_kind(t, bet_kind)
            if norm in winners:
                per[sid]["hits"]   += 1
                per[sid]["return"] += winners[norm]
        time.sleep(0.2)

    def pct(n,d): 
        try: return f"{(100.0*n/max(d,1)):.1f}%"
        except ZeroDivisionError: return "0.0%"

    total_stake  = sum(v["stake"]  for v in per.values())
    total_return = sum(v["return"] for v in per.values())

    def _fmt_line(sid,label):
        v=per[sid]
        return (f"{label}：該当{v['races']}R / 購入{v['bets']}点 / 的中{v['hits']}点\n"
                f"　的中率 {pct(v['hits'], v['bets'])} / 回収率 {pct(v['return'], v['stake'])} / "
                f"投資 {v['stake']:,}円 / 払戻 {v['return']:,}円")

    # 送信
    msg="\n".join([
        f"【日次サマリ】{now.strftime('%Y-%m-%d')}",
        _fmt_line("S1","①"),
        _fmt_line("S2","②"),
        _fmt_line("S3","③"),
        _fmt_line("S4","④"),
        "",
        f"合計：投資 {total_stake:,}円 / 払戻 {total_return:,}円",
        f"　的中率 {pct(sum(per[s]['hits'] for s in per), sum(per[s]['bets'] for s in per))} / 回収率 {pct(total_return, total_stake)}",
    ])
    uids=load_user_ids_from_simple_col()
    push_line_text(uids, msg)
    sheet_upsert_notified(key, time.time(), "daily summary")

# ===== main / ループ =====
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logging.info("[BOOT] now(JST)=%s CUT=%s", jst_now().strftime("%Y-%m-%d %H:%M:%S %z"), CUTOFF_OFFSET_MIN)
    hits, matches = _scan_and_notify_once()
    logging.info("[INFO] HITS=%d / MATCHES=%d", hits, matches)
    summarize_today_and_notify()
    logging.info("[INFO] ジョブ終了")

def run_watcher_forever(sleep_sec: int = 60):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logging.info("[INFO] watcher.run_watcher_forever start (sleep=%ss)", sleep_sec)
    while True:
        try:
            _scan_and_notify_once()
            summarize_today_and_notify()
        except Exception as e:
            logging.exception("[FATAL] loop error: %s", e)
        time.sleep(max(10, sleep_sec))

if __name__ == "__main__":
    main()