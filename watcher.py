# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視・通知バッチ（完全差し替え版 v2025-08-27F1）
- 締切時刻：単複/一覧ページから“締切”を直接抽出（最優先）
- 発走時刻：一覧ページ優先＋フォールバック（発走-オフセット）
- 窓判定：ターゲット時刻（締切 or 発走-オフセット）±GRACE_SECONDS
- 通知：窓内1回 / 429時はクールダウン / Google SheetでTTL永続
- 送信先：シート「1」のH列から userId を収集
- 騎手ランク：ENVの200位表＋表記ゆれ耐性＋姓一致フォールバック
- 通知本文：戦略ラベル＋買い目（人気/馬番）＋上位オッズ
- 日次サマリ：betsシートの投資/払戻に基づく【戦略別 ROI / 的中率】厳密突合
"""

import os, re, json, time, random, logging, unicodedata
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Set

import requests
from bs4 import BeautifulSoup, Tag
from strategy_rules import eval_strategy
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# --- 通知ログ append のフォールバック ---
try:
    from utils_notify_log import append_notify_log
except ModuleNotFoundError:
    def append_notify_log(*args, **kwargs):
        logging.warning("[WARN] utils_notify_log が無いためスキップ")

# --- 日付ユーティリティ ---
JST = timezone(timedelta(hours=9))
def jst_today_str(): return datetime.now(JST).strftime("%Y%m%d")
def jst_now(): return datetime.now(JST)

# ========= 基本設定 / ENV =========
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0", "Accept-Language": "ja,en-US;q=0.9"})
TIMEOUT = (10, 25)
RETRY = 3
SLEEP_BETWEEN = (0.6, 1.2)
LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"

START_HOUR = int(os.getenv("START_HOUR", "10"))
END_HOUR   = int(os.getenv("END_HOUR",   "22"))
DRY_RUN    = os.getenv("DRY_RUN", "False").lower() == "true"
FORCE_RUN  = os.getenv("FORCE_RUN", "0") == "1"

NOTIFY_ENABLED = os.getenv("NOTIFY_ENABLED", "1") == "1"
NOTIFY_TTL_SEC = int(os.getenv("NOTIFY_TTL_SEC", "3600"))
NOTIFY_COOLDOWN_SEC = int(os.getenv("NOTIFY_COOLDOWN_SEC", "1800"))

WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "15"))
WINDOW_AFTER_MIN  = int(os.getenv("WINDOW_AFTER_MIN", "0"))
CUTOFF_OFFSET_MIN = int(os.getenv("CUTOFF_OFFSET_MIN", "5"))
GRACE_SECONDS     = int(os.getenv("GRACE_SECONDS", "60"))

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "")
LINE_USER_ID      = os.getenv("LINE_USER_ID", "")
LINE_USER_IDS     = [s.strip() for s in os.getenv("LINE_USER_IDS", "").split(",") if s.strip()]

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_SHEET_ID   = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_SHEET_TAB  = os.getenv("GOOGLE_SHEET_TAB", "notified")

USERS_SHEET_NAME  = os.getenv("USERS_SHEET_NAME", "1")
USERS_USERID_COL  = os.getenv("USERS_USERID_COL", "H")
BETS_SHEET_TAB    = os.getenv("BETS_SHEET_TAB", "bets")

DAILY_SUMMARY_HHMM = os.getenv("DAILY_SUMMARY_HHMM", "21:02")
ALWAYS_NOTIFY_DAILY_SUMMARY = os.getenv("ALWAYS_NOTIFY_DAILY_SUMMARY", "1") == "1"

DEBUG_RACEIDS = [s.strip() for s in os.getenv("DEBUG_RACEIDS", "").split(",") if s.strip()]
UNIT_STAKE_YEN = int(os.getenv("UNIT_STAKE_YEN", "100"))

# 戦略ごとの券種（ROI集計に使用）
_DEFAULT_BET_KIND = {"S1":"三連単", "S2":"三連単", "S3":"三連単", "S4":"三連単"}
try:
    STRATEGY_BET_KIND = json.loads(os.getenv("STRATEGY_BET_KIND_JSON","")) or _DEFAULT_BET_KIND
except Exception:
    STRATEGY_BET_KIND = _DEFAULT_BET_KIND

# ========= Google Sheets =========
def _sheet_service():
    if not GOOGLE_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        raise RuntimeError("Google Sheets の環境変数不足")
    info=json.loads(GOOGLE_CREDENTIALS_JSON)
    creds=Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets","v4",credentials=creds, cache_discovery=False)

def _resolve_sheet_title(svc, tab_or_gid: str) -> str:
    meta=svc.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    sheets=meta.get("sheets", [])
    if tab_or_gid.isdigit() and len(tab_or_gid)>3:
        gid=int(tab_or_gid)
        for s in sheets:
            if s["properties"]["sheetId"]==gid:
                return s["properties"]["title"]
        raise RuntimeError(f"指定gidのシートが見つかりません: {gid}")
    for s in sheets:
        if s["properties"]["title"]==tab_or_gid:
            return tab_or_gid
    body={"requests":[{"addSheet":{"properties":{"title":tab_or_gid}}}]}
    svc.spreadsheets().batchUpdate(spreadsheetId=GOOGLE_SHEET_ID, body=body).execute()
    return tab_or_gid

def _sheet_get_range_values(svc, title: str, a1: str) -> List[List[str]]:
    res=svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"'{title}'!{a1}").execute()
    return res.get("values", [])

def _sheet_update_range_values(svc, title: str, a1: str, values: List[List[str]]):
    svc.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID, range=f"'{title}'!{a1}",
        valueInputOption="RAW", body={"values": values}).execute()
        
# ========= HTMLユーティリティ / 共通パターン =========
RACEID_RE   = re.compile(r"/RACEID/(\d{18})")
PLACEHOLDER = re.compile(r"\d{8}0000000000$")
TIME_PATS = [
    re.compile(r"\b(\d{1,2}):(\d{2})\b"),
    re.compile(r"\b(\d{1,2})：(\d{2})\b"),               # 全角コロン
    re.compile(r"\b(\d{1,2})\s*時\s*(\d{1,2})\s*分\b"),  # 12時50分
]
CUTOFF_LABEL_PAT  = re.compile(r"(投票締切|発売締切|締切)")

def within_operating_hours() -> bool:
    if FORCE_RUN:
        return True
    h = jst_now().hour
    return START_HOUR <= h < END_HOUR

def fetch(url: str) -> str:
    last_err = None
    for i in range(1, RETRY+1):
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:
            last_err = e
            wait = random.uniform(*SLEEP_BETWEEN)
            logging.warning(f"[WARN] fetch失敗({i}/{RETRY}) {e} -> {wait:.1f}s待機: {url}")
            time.sleep(wait)
    raise last_err

def _rid_date_parts(rid: str) -> Tuple[int,int,int]:
    return int(rid[0:4]), int(rid[4:6]), int(rid[6:8])

def _norm_hhmm_from_text(text: str) -> Optional[Tuple[int,int,str]]:
    if not text: return None
    s = str(text)
    for pat, tag in zip(TIME_PATS, ("half","full","kanji")):
        m = pat.search(s)
        if m:
            hh, mm = int(m.group(1)), int(m.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return hh, mm, tag
    return None

def _make_dt_from_hhmm(rid: str, hh: int, mm: int) -> Optional[datetime]:
    try:
        y, mon, d = _rid_date_parts(rid)
        return datetime(y, mon, d, hh, mm, tzinfo=JST)
    except Exception:
        return None

def _find_time_nearby(el: Tag) -> Tuple[Optional[str], str]:
    # <time>要素
    t = el.find("time")
    if t:
        for attr in ("datetime","data-time","title","aria-label"):
            v = t.get(attr)
            if v:
                got = _norm_hhmm_from_text(v)
                if got:
                    hh,mm,why = got
                    return f"{hh:02d}:{mm:02d}", f"time@{attr}/{why}"
        got = _norm_hhmm_from_text(t.get_text(" ", strip=True))
        if got:
            hh,mm,why = got
            return f"{hh:02d}:{mm:02d}", f"time@text/{why}"

    # data-* / title / aria-label
    for node in el.find_all(True, recursive=True):
        for attr in ("data-starttime","data-start-time","data-time","title","aria-label"):
            v = node.get(attr)
            if not v: continue
            got = _norm_hhmm_from_text(v)
            if got:
                hh,mm,why = got
                return f"{hh:02d}:{mm:02d}", f"data:{attr}/{why}"

    # よくあるクラス名
    for sel in [".startTime",".cellStartTime",".raceTime",".time",".start-time"]:
        node = el.select_one(sel)
        if node:
            got = _norm_hhmm_from_text(node.get_text(" ", strip=True))
            if got:
                hh,mm,why = got
                return f"{hh:02d}:{mm:02d}", f"sel:{sel}/{why}"

    # テキスト
    got = _norm_hhmm_from_text(el.get_text(" ", strip=True))
    if got:
        hh,mm,why = got
        return f"{hh:02d}:{mm:02d}", f"row:text/{why}"
    return None, "-"

def _extract_raceids_from_soup(soup: BeautifulSoup) -> List[str]:
    rids: List[str] = []
    for a in soup.find_all("a", href=True):
        m = RACEID_RE.search(a["href"])
        if m:
            rid = m.group(1)
            if not PLACEHOLDER.search(rid):
                rids.append(rid)
    return sorted(set(rids))

# ========= 発走時刻（一覧ページ）解析 / 収集 =========
def parse_post_times_from_table_like(root: Tag) -> Dict[str, datetime]:
    post_map: Dict[str, datetime] = {}

    # 1) テーブル
    for table in root.find_all("table"):
        thead = table.find("thead")
        if thead:
            head_text = "".join(thead.stripped_strings)
            if not any(k in head_text for k in ("発走","発走時刻","レース")):
                continue
        body = table.find("tbody") or table
        for tr in body.find_all("tr"):
            rid = None
            link = tr.find("a", href=True)
            if link:
                m = RACEID_RE.search(link["href"])
                if m:
                    rid = m.group(1)
            if not rid or PLACEHOLDER.search(rid):
                continue

            hhmm, _ = _find_time_nearby(tr)
            if not hhmm:
                continue
            hh, mm = map(int, hhmm.split(":"))
            dt = _make_dt_from_hhmm(rid, hh, mm)
            if dt:
                post_map[rid] = dt

    # 2) カード型
    for a in root.find_all("a", href=True):
        m = RACEID_RE.search(a["href"])
        if not m: continue
        rid = m.group(1)
        if PLACEHOLDER.search(rid) or rid in post_map:
            continue
        # 近い先祖
        host = None; depth = 0
        for parent in a.parents:
            if isinstance(parent, Tag) and parent.name in ("tr","li","div","section","article"):
                host = parent; break
            depth += 1
            if depth >= 6: break
        host = host or a

        hhmm, _ = _find_time_nearby(host)
        if not hhmm:
            # 兄弟方向
            sib_text = " ".join([x.get_text(" ", strip=True) for x in a.find_all_next(limit=4) if isinstance(x, Tag)])
            got = _norm_hhmm_from_text(sib_text)
            if got:
                hh,mm,_ = got
                hhmm = f"{hh:02d}:{mm:02d}"
        if not hhmm:
            continue
        hh, mm = map(int, hhmm.split(":"))
        dt = _make_dt_from_hhmm(rid, hh, mm)
        if dt:
            post_map[rid] = dt

    return post_map

def collect_post_time_map(ymd: str, ymd_next: str) -> Dict[str, datetime]:
    post_map: Dict[str, datetime] = {}

    def _merge_from(url: str, label: str):
        try:
            soup = BeautifulSoup(fetch(url), "lxml")
            got = parse_post_times_from_table_like(soup)
            if got:
                post_map.update(got)
        except Exception as e:
            logging.warning(f"[WARN] {label} 読み込み失敗: {e} ({url})")

    _merge_from(f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000",   "list:today")
    _merge_from(f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd_next}0000000000","list:tomorrow")

    logging.info(f"[INFO] 発走時刻取得: {len(post_map)}件")
    return post_map

# ========= 締切時刻（最優先で抽出） =========
def _extract_cutoff_hhmm_from_soup(soup: BeautifulSoup) -> Optional[str]:
    # セレクタ優先
    for sel in ["time[data-type='cutoff']", ".cutoff time", ".deadline time", ".time.-deadline"]:
        t = soup.select_one(sel)
        if t:
            got = _norm_hhmm_from_text(t.get_text(" ", strip=True) or t.get("datetime",""))
            if got:
                hh,mm,_ = got
                return f"{hh:02d}:{mm:02d}"
    # 「締切」ラベル近傍
    for node in soup.find_all(string=CUTOFF_LABEL_PAT):
        container = getattr(node, "parent", None) or soup
        host = container
        for p in container.parents:
            if isinstance(p, Tag) and p.name in ("div","section","article","li"):
                host = p; break
        text = " ".join(host.get_text(" ", strip=True).split())
        got = _norm_hhmm_from_text(text)
        if got:
            hh,mm,_ = got
            return f"{hh:02d}:{mm:02d}"
    # 全文最後の手段
    txt = " ".join(soup.stripped_strings)
    if CUTOFF_LABEL_PAT.search(txt):
        got = _norm_hhmm_from_text(txt)
        if got:
            hh,mm,_ = got
            return f"{hh:02d}:{mm:02d}"
    return None

def resolve_cutoff_dt(rid: str) -> Optional[Tuple[datetime, str]]:
    # tanfuku優先 → list
    try:
        soup = BeautifulSoup(fetch(f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"), "lxml")
        hhmm = _extract_cutoff_hhmm_from_soup(soup)
        if hhmm:
            hh,mm = map(int, hhmm.split(":"))
            dt = _make_dt_from_hhmm(rid, hh, mm)
            if dt: return dt, "tanfuku"
    except Exception as e:
        logging.warning("[WARN] 締切抽出(tanfuku)失敗 rid=%s: %s", rid, e)
    try:
        soup = BeautifulSoup(fetch(f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{rid}"), "lxml")
        hhmm = _extract_cutoff_hhmm_from_soup(soup)
        if hhmm:
            hh,mm = map(int, hhmm.split(":"))
            dt = _make_dt_from_hhmm(rid, hh, mm)
            if dt: return dt, "list"
    except Exception as e:
        logging.warning("[WARN] 締切抽出(list)失敗 rid=%s: %s", rid, e)
    return None

# ========= オッズ解析（単複ページ） =========
def _clean(s: str) -> str:
    return re.sub(r"\s+","", s or "")

def _as_float(text: str) -> Optional[float]:
    if not text: return None
    t = text.replace(",","").strip()
    if "%" in t or "-" in t or "～" in t or "~" in t: return None
    m = re.search(r"\d+(?:\.\d+)?", t)
    return float(m.group(0)) if m else None

def _as_int(text: str) -> Optional[int]:
    if not text: return None
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else None

def _find_popular_odds_table(soup: BeautifulSoup) -> Tuple[Optional[BeautifulSoup], Dict[str,int]]:
    for table in soup.find_all("table"):
        thead = table.find("thead")
        if not thead: continue
        headers = [_clean(th.get_text()) for th in thead.find_all(["th","td"])]
        if not headers: continue

        pop_idx = win_idx = num_idx = None
        for i,h in enumerate(headers):
            if h in ("人気","順位") or ("人気" in h and "順" not in h):
                pop_idx = i; break

        win_candidates = []
        for i,h in enumerate(headers):
            if ("複" in h) or ("率" in h) or ("%" in h): continue
            if   h == "単勝": win_candidates.append((0,i))
            elif "単勝" in h: win_candidates.append((1,i))
            elif "オッズ" in h: win_candidates.append((2,i))
        win_idx = sorted(win_candidates, key=lambda x:x[0])[0][1] if win_candidates else None

        for i,h in enumerate(headers):
            if "馬番" in h: num_idx = i; break
        if num_idx is None:
            for i,h in enumerate(headers):
                if ("馬" in h) and ("馬名" not in h) and (i != pop_idx):
                    num_idx = i; break

        if pop_idx is None or win_idx is None:
            continue

        # 妥当性（人気が1,2と昇順）
        body = table.find("tbody") or table
        rows = body.find_all("tr")
        seq_ok, last = 0, 0
        for tr in rows[:6]:
            tds = tr.find_all(["td","th"])
            if len(tds) <= max(pop_idx, win_idx): continue
            s = tds[pop_idx].get_text(strip=True)
            if not s.isdigit(): break
            v = int(s)
            if v <= last: break
            last = v; seq_ok += 1
        if seq_ok >= 2:
            return table, {"pop":pop_idx,"win":win_idx,"num":num_idx if num_idx is not None else -1}
    return None, {}

def parse_odds_table(soup: BeautifulSoup) -> Tuple[List[Dict[str,float]], Optional[str], Optional[str]]:
    venue_race = (soup.find("h1").get_text(strip=True) if soup.find("h1") else None)
    nowtime = soup.select_one(".withUpdate .nowTime") or soup.select_one(".nowTime")
    now_label = nowtime.get_text(strip=True) if nowtime else None

    table, idx = _find_popular_odds_table(soup)
    if not table:
        return [], venue_race, now_label

    pop_idx = idx["pop"]; win_idx = idx["win"]; num_idx = idx.get("num",-1)
    horses: List[Dict[str,float]] = []
    body = table.find("tbody") or table
    for tr in body.find_all("tr"):
        tds = tr.find_all(["td","th"])
        if len(tds) <= max(pop_idx, win_idx): continue
        pop_txt = tds[pop_idx].get_text(strip=True)
        if not pop_txt.isdigit(): continue
        pop = int(pop_txt)
        if not (1 <= pop <= 30): continue
        odds = _as_float(tds[win_idx].get_text(" ", strip=True))
        if odds is None: continue
        rec = {"pop": pop, "odds": float(odds)}
        if 0 <= num_idx < len(tds):
            num = _as_int(tds[num_idx].get_text(" ", strip=True))
            if num is not None: rec["num"] = num
        horses.append(rec)

    # 人気の重複排除（最初出現を採用）
    uniq: Dict[int, Dict[str,float]] = {}
    for h in sorted(horses, key=lambda x:x["pop"]):
        uniq[h["pop"]] = h
    horses = [uniq[k] for k in sorted(uniq.keys())]
    return horses, venue_race, now_label

def check_tanfuku_page(race_id: str) -> Optional[Dict]:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")
    horses, venue_race, now_label = parse_odds_table(soup)
    if not horses:
        logging.info(f"[INFO] オッズテーブル未検出: {url}")
        return None
    if not venue_race:
        venue_race = "地方競馬"
    return {"race_id": race_id, "url": url, "horses": horses,
            "venue_race": venue_race, "now": now_label or ""}

# ========= レース列挙・時刻解決 / 窓判定 =========
def list_raceids_today_and_next() -> Tuple[List[str], Dict[str, datetime], Dict[str, Tuple[datetime,str]]]:
    today = jst_today_str()
    dt = datetime.strptime(today, "%Y%m%d").replace(tzinfo=JST)
    tomorrow = (dt + timedelta(days=1)).strftime("%Y%m%d")
    urls = [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{today}0000000000",
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{tomorrow}0000000000",
    ]
    rids: Set[str] = set()
    for url in urls:
        try:
            soup = BeautifulSoup(fetch(url), "lxml")
            rids.update(_extract_raceids_from_soup(soup))
        except Exception as e:
            logging.warning(f"[WARN] RID一覧取得失敗: {e} ({url})")

    post_map = collect_post_time_map(today, tomorrow)
    cutoff_map: Dict[str, Tuple[datetime,str]] = {}
    for rid in list(rids):
        got = resolve_cutoff_dt(rid)
        if got:
            cutoff_map[rid] = got
    return sorted(rids), post_map, cutoff_map

def fallback_target_time(rid: str, post_map: Dict[str, datetime], cutoff_map: Dict[str, Tuple[datetime,str]]) -> Tuple[Optional[datetime], str]:
    tup = cutoff_map.get(rid)
    if tup:
        dt, src = tup
        return dt, f"締切:{src}"
    post = post_map.get(rid)
    if post:
        return post - timedelta(minutes=CUTOFF_OFFSET_MIN), "発走-オフセット"
    return None, "-"

def is_within_window(target_dt: datetime) -> bool:
    now = jst_now()
    start = target_dt - timedelta(minutes=WINDOW_BEFORE_MIN)
    end   = target_dt + timedelta(minutes=WINDOW_AFTER_MIN)
    return (start - timedelta(seconds=GRACE_SECONDS)) <= now <= (end + timedelta(seconds=GRACE_SECONDS))

# ========= LINE通知 / 表示ユーティリティ =========
def _fmt_horse_line(h: Dict) -> str:
    num = f"{int(h['num'])}" if isinstance(h.get("num"), int) else "-"
    odds = f"{h['odds']:.1f}" if isinstance(h.get("odds"), (int,float)) else "—"
    return f"  馬番{num}  単勝{odds}倍"

def build_line_notification(result: Dict, strat: Dict, race_id: str,
                            target_dt: datetime, target_src: str,
                            venue_race: str, now_label: str) -> str:
    horses = result.get("horses", [])
    url    = result.get("url", "")

    # 人気→馬番変換
    pop2num = {h["pop"]:h["num"] for h in horses if isinstance(h.get("pop"),int) and isinstance(h.get("num"),int)}
    def _fmt_ticket_umaban(tk: str) -> str:
        try:
            a,b,c = [int(x) for x in tk.split("-")]
            return f"{pop2num.get(a,'-')}-{pop2num.get(b,'-')}-{pop2num.get(c,'-')}"
        except Exception:
            return tk

    header = f"{venue_race} / RID:{race_id[-6:]} / ターゲット={target_dt.strftime('%H:%M')}（{target_src}）"
    lines=[header, f""]

    tickets = strat.get("tickets", []) or []
    n = len(tickets)
    if strat.get("id") == "S3":
        head = " / ".join(tickets[:8]) + (" …" if n>8 else "")
        lines.append(f"買い目（馬番）: {head}（全{n}点）")
        axis = strat.get("axis") or {}
        if axis:
            ao = axis.get("odds")
            if isinstance(ao,(int,float)):
                lines.append(f"軸: 馬番{axis.get('umaban','-')}（単勝{ao:.1f}倍）")
            else:
                lines.append(f"軸: 馬番{axis.get('umaban','-')}")
        cands = strat.get("candidates") or []
        if cands:
            cand_s = " / ".join([
                f"{c.get('umaban','-')}({c.get('odds',0):.1f})" if isinstance(c.get('odds'),(int,float))
                else str(c.get('umaban','-')) for c in cands
            ])
            lines.append(f"相手候補: {cand_s}")
    else:
        head_pop = " / ".join(tickets[:8]) + (" …" if n>8 else "")
        head_num = " / ".join([_fmt_ticket_umaban(t) for t in tickets[:8]]) + (" …" if n>8 else "")
        lines.append(f"買い目（人気）: {head_pop}（全{n}点）")
        lines.append(f"買い目（馬番）: {head_num}")

    lines.append("")
    lines.append("上位オッズ:")
    for h in sorted(horses, key=lambda x: x.get("pop", 999))[:5]:
        lines.append(_fmt_horse_line(h))

    if now_label:
        lines.append(f"更新:{now_label}")
    if url:
        lines.append(url)
    lines += ["", "※オッズは締切直前まで変化します。", "※馬券の的中を保証するものではありません。ご注意ください。"]
    return "\n".join(lines)

# ========= LINE送信（複数宛先） =========
def push_line_text(to_user_ids: List[str], message: str) -> Tuple[int, str]:
    if DRY_RUN or not NOTIFY_ENABLED:
        logging.info("[DRY] LINE送信スキップ: %s", message.replace("\n"," / "))
        return 200, "DRY"
    if not LINE_ACCESS_TOKEN:
        logging.warning("[WARN] LINE_ACCESS_TOKEN 未設定")
        return 0, "NO_TOKEN"
    headers = {"Authorization": f"Bearer {LINE_ACCESS_TOKEN}", "Content-Type": "application/json"}
    ok = 0
    last = ""
    for uid in to_user_ids:
        body = {"to": uid, "messages": [{"type": "text", "text": message[:5000]}]}
        try:
            r = SESSION.post("https://api.line.me/v2/bot/message/push", headers=headers, json=body, timeout=TIMEOUT)
            last = f"{r.status_code} {r.text[:160]}"
            if r.status_code == 200:
                ok += 1
            elif r.status_code == 429:
                logging.warning("[WARN] LINE 429: cooldown=%ss", NOTIFY_COOLDOWN_SEC)
                time.sleep(NOTIFY_COOLDOWN_SEC)
            else:
                logging.warning("[WARN] LINE送信失敗 uid=%s code=%s body=%s", uid, r.status_code, r.text[:120])
        except Exception as e:
            last = str(e)
            logging.warning("[WARN] LINE送信例外 uid=%s e=%s", uid, e)
        time.sleep(0.1)
    return ok, last

# ========= Google Sheets: bets 追記 & 取得 =========
def _sheet_service():
    if not GOOGLE_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        raise RuntimeError("Google Sheets の環境変数不足")
    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def _resolve_sheet_title(svc, tab_or_gid: str) -> str:
    meta = svc.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    sheets = meta.get("sheets", [])
    if tab_or_gid.isdigit() and len(tab_or_gid) > 3:
        gid = int(tab_or_gid)
        for s in sheets:
            if s["properties"]["sheetId"] == gid:
                return s["properties"]["title"]
        raise RuntimeError(f"指定gidのシートが見つかりません: {gid}")
    for s in sheets:
        if s["properties"]["title"] == tab_or_gid:
            return tab_or_gid
    body = {"requests":[{"addSheet":{"properties":{"title": tab_or_gid}}}]}
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

def sheet_load_notified() -> Dict[str, float]:
    svc = _sheet_service(); title = _resolve_sheet_title(svc, GOOGLE_SHEET_TAB)
    values = _sheet_get_range_values(svc, title, "A:C")
    start = 1 if values and values[0] and str(values[0][0]).upper() in ("KEY","RACEID","RID","ID") else 0
    d: Dict[str, float] = {}
    for row in values[start:]:
        if not row or len(row) < 2: continue
        key = str(row[0]).strip()
        try: d[key] = float(row[1])
        except: pass
    return d

def sheet_upsert_notified(key: str, ts: float, note: str = "") -> None:
    svc = _sheet_service(); title = _resolve_sheet_title(svc, GOOGLE_SHEET_TAB)
    values = _sheet_get_range_values(svc, title, "A:C")
    header = ["KEY","TS_EPOCH","NOTE"]
    if not values:
        _sheet_update_range_values(svc, title, "A:C", [header, [key, ts, note]]); return
    start_row = 1 if values and values[0] and values[0][0] in header else 0
    found = None
    for i,row in enumerate(values[start_row:], start=start_row):
        if row and str(row[0]).strip() == key: found = i; break
    if found is None: values.append([key, ts, note])
    else: values[found] = [key, ts, note]
    _sheet_update_range_values(svc, title, "A:C", values)

# 送信先ユーザー（タブ「1」のH列）
def load_user_ids_from_simple_col() -> List[str]:
    svc = _sheet_service()
    title = os.getenv("USERS_SHEET_NAME","1")
    col   = (os.getenv("USERS_USERID_COL","H") or "H").upper()
    sheet = _resolve_sheet_title(svc, title)
    values = _sheet_get_range_values(svc, sheet, f"{col}:{col}")
    user_ids: List[str] = []
    for i,row in enumerate(values):
        v = (row[0].strip() if row and row[0] is not None else "")
        if not v: continue
        low = v.replace(" ","").lower()
        if i == 0 and ("userid" in low or "line" in low):  # ヘッダ回避
            continue
        if not v.startswith("U"): continue
        if v not in user_ids: user_ids.append(v)
    if not user_ids:
        fallback = [s.strip() for s in (os.getenv("LINE_USER_IDS","") or "").split(",") if s.strip()]
        if not fallback and LINE_USER_ID: fallback = [LINE_USER_ID]
        user_ids = fallback
    logging.info("[INFO] 送信ターゲット数: %d", len(user_ids))
    return user_ids

# ========= bets 追記 / 払戻取得 / ROI・的中率サマリ =========
UNIT_STAKE_YEN = int(os.getenv("UNIT_STAKE_YEN","100"))
BETS_SHEET_TAB = os.getenv("BETS_SHEET_TAB", "bets")
_DEFAULT_BET_KIND = {"S1":"馬連", "S2":"馬単", "S3":"三連単", "S4":"三連複"}
try:
    STRATEGY_BET_KIND = json.loads(os.getenv("STRATEGY_BET_KIND_JSON","")) or _DEFAULT_BET_KIND
except Exception:
    STRATEGY_BET_KIND = _DEFAULT_BET_KIND

def jst_today_str() -> str:
    return jst_now().strftime("%Y%m%d")

def _bets_header() -> List[str]:
    return ["date","race_id","venue","race_no","strategy_id","bet_kind","tickets_umaban_csv","points","unit_stake","total_stake"]

def sheet_append_bet_record(date_ymd:str, race_id:str, venue:str, race_no:str, strategy_id:str, bet_kind:str, tickets_umaban:List[str]):
    svc = _sheet_service(); title = _resolve_sheet_title(svc, BETS_SHEET_TAB)
    values = _sheet_get_range_values(svc, title, "A:J")
    if not values:
        values = [_bets_header()]
    points = len(tickets_umaban); unit = UNIT_STAKE_YEN; total = points * unit
    values.append([date_ymd, race_id, venue, race_no, strategy_id, bet_kind, ",".join(tickets_umaban), str(points), str(unit), str(total)])
    _sheet_update_range_values(svc, title, "A:J", values)

_PAYOUT_KIND_KEYS = ["単勝","複勝","枠連","馬連","ワイド","馬単","三連複","三連単"]

def fetch_payoff_map(race_id:str) -> Dict[str, List[Tuple[str,int]]]:
    url = f"https://keiba.rakuten.co.jp/race/payoff/RACEID/{race_id}"
    html = fetch(url); soup = BeautifulSoup(html,"lxml")
    result: Dict[str, List[Tuple[str,int]]] = {}
    for kind in _PAYOUT_KIND_KEYS:
        blocks = soup.find_all(string=re.compile(kind))
        if not blocks: continue
        items: List[Tuple[str,int]] = []
        for b in blocks:
            box = getattr(b,"parent",None) or soup
            text = " ".join((box.get_text(" ", strip=True) or "").split())
            for m in re.finditer(r"(\d+(?:-\d+){0,2})\s*([\d,]+)\s*円", text):
                comb = m.group(1); pay = int(m.group(2).replace(",",""))
                items.append((comb, pay))
        if items: result[kind] = items
    return result

def _normalize_ticket_for_kind(ticket:str, kind:str) -> str:
    parts = [int(x) for x in ticket.split("-") if x.strip().isdigit()]
    if kind in ("馬連","三連複"):
        parts = sorted(parts)
    return "-".join(str(x) for x in parts)

# ========= スキャン本体（TTLキー= rid:HHMM:strat_id） =========
def _scan_and_notify_once() -> Tuple[int,int]:
    if not within_operating_hours() and not FORCE_RUN:
        logging.info("[INFO] 運用時間外: %02d-%02d", START_HOUR, END_HOUR)
        return 0, 0

    user_ids = load_user_ids_from_simple_col()
    notified = sheet_load_notified()
    hits = 0; matches = 0

    rids, post_map, cutoff_map = list_raceids_today_and_next()
    # DEBUG追加入力
    extra = [s.strip() for s in os.getenv("DEBUG_RACEIDS","").split(",") if s.strip()]
    for rid in extra:
        if rid and rid not in rids: rids.append(rid)

    for rid in rids:
        target_dt, src = fallback_target_time(rid, post_map, cutoff_map)
        if not target_dt:
            continue
        in_window = is_within_window(target_dt)
        logging.info("[TRACE] time rid=%s at=%s target=%s in_window=%s",
                     rid, jst_now().strftime("%H:%M:%S"), target_dt.strftime("%H:%M"), in_window)
        if not in_window and not FORCE_RUN:
            continue

        meta = check_tanfuku_page(rid)
        if not meta:
            continue
        hits += 1

        # 観測ログ TOP5
        try:
            top = sorted(meta["horses"], key=lambda x:x.get("pop",999))[:5]
            obs = ["pop={:>2} num={:>2} odds={:>5}".format(
                h.get("pop","-"), h.get("num","-"), h.get("odds","-")
            ) for h in top]
            logging.info("[OBS] top5 %s", " | ".join(obs))
        except Exception:
            pass

        # 判定
        try:
            strat = eval_strategy(meta["horses"], logger=logging)
        except Exception as e:
            logging.warning("[WARN] eval_strategy 例外: %s", e)
            strat = {"match": False}

        if not strat or not strat.get("match"):
            logging.info("[TRACE] judge rid=%s result=FAIL reason=no_strategy_match", rid)
            continue

        strat_id = str(strat.get("id","S3"))
        ttl_key = f"{rid}:{target_dt.strftime('%H%M')}:{strat_id}"
        last_ts = notified.get(ttl_key, 0.0)
        if (time.time() - last_ts) < NOTIFY_TTL_SEC and not FORCE_RUN:
            logging.info("[TRACE] skip rid=%s reason=TTL key=%s", rid, ttl_key)
            continue

        matches += 1

        venue_race = meta.get("venue_race","")
        now_label  = meta.get("now","")
        message = build_line_notification(meta, strat, rid, target_dt, src, venue_race, now_label)
        ok_count, last = push_line_text(user_ids, message)
        logging.info("[INFO] LINE push ok=%d detail=%s", ok_count, last[:120])

        # TTLフラグ
        sheet_upsert_notified(ttl_key, time.time(), f"{venue_race} {target_dt.strftime('%H:%M')} {src}")

        # bets 追記（ROI用）
        try:
            pop2num = {h["pop"]:h["num"] for h in meta["horses"] if isinstance(h.get("pop"),int) and isinstance(h.get("num"),int)}
            def _to_umaban(tk:str)->str:
                try:
                    a,b,c = [int(x) for x in tk.split("-")]
                    return f"{pop2num.get(a,'-')}-{pop2num.get(b,'-')}-{pop2num.get(c,'-')}"
                except: return tk
            raw_tickets = strat.get("tickets",[]) or []
            tickets_umaban = raw_tickets if strat_id=="S3" else [_to_umaban(t) for t in raw_tickets]
            m = re.search(r"\b(\d{1,2})R\b", venue_race); race_no = (m.group(1)+"R") if m else ""
            bet_kind = STRATEGY_BET_KIND.get(strat_id, "三連単")
            sheet_append_bet_record(jst_today_str(), rid, venue_race.split()[0], race_no, strat_id, bet_kind, tickets_umaban)
        except Exception as e:
            logging.warning("[WARN] bets記録失敗 rid=%s: %s", rid, e)

        time.sleep(0.4)

    return hits, matches

# ========= 日次サマリ（簡略版） =========
DAILY_SUMMARY_HHMM = os.getenv("DAILY_SUMMARY_HHMM","21:02")
ALWAYS_NOTIFY_DAILY_SUMMARY = os.getenv("ALWAYS_NOTIFY_DAILY_SUMMARY","1") == "1"

def _daily_summary_due(now: datetime) -> bool:
    if not ALWAYS_NOTIFY_DAILY_SUMMARY: return False
    if not re.match(r"^\d{1,2}:\d{2}$", DAILY_SUMMARY_HHMM): return False
    h,m = map(int, DAILY_SUMMARY_HHMM.split(":"))
    due = now.replace(hour=h, minute=m, second=0, microsecond=0)
    return abs((now - due).total_seconds()) <= 300  # ±5分

def summarize_today_and_notify():
    now = jst_now()
    if not _daily_summary_due(now): return

    # 既送信チェック
    key = f"DAILY_SUMMARY:{now.strftime('%Y%m%d')}"
    try:
        notified = sheet_load_notified()
        if key in notified:
            logging.info("[INFO] 日次サマリは既送信: %s", key)
            return
    except Exception:
        notified = {}

    # bets から当日行抽出
    svc = _sheet_service(); title = _resolve_sheet_title(svc, BETS_SHEET_TAB)
    rows = _sheet_get_range_values(svc, title, "A:J") or []
    hdr  = rows[0] if rows else []
    body = rows[1:] if len(rows) >= 2 else []
    today = now.strftime("%Y%m%d")
    recs = [r for r in body if len(r)>=10 and r[0]==today]

    per = { sid:{"races":0,"bets":0,"hits":0,"stake":0,"return":0} for sid in ("S1","S2","S3","S4") }
    seen_race_sid: Set[Tuple[str,str]] = set()

    for r in recs:
        date_ymd, race_id, venue, race_no, strategy_id, bet_kind, t_csv, points, unit, total = r[:10]
        sid = strategy_id or "S3"
        if (race_id, sid) not in seen_race_sid:
            per[sid]["races"] += 1
            seen_race_sid.add((race_id, sid))
        tickets = [t for t in (t_csv or "").split(",") if t]
        per[sid]["bets"]  += len(tickets)
        per[sid]["stake"] += int(total) if str(total).isdigit() else len(tickets)*UNIT_STAKE_YEN

        # 払戻ページで照合
        try:
            paymap = fetch_payoff_map(race_id)
        except Exception as e:
            logging.warning("[SUM] 払戻取得失敗 rid=%s: %s", race_id, e)
            continue

        winners = { _normalize_ticket_for_kind(comb, bet_kind): pay for (comb, pay) in paymap.get(bet_kind, []) }
        for t in tickets:
            norm = _normalize_ticket_for_kind(t, bet_kind)
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
        v = per[sid]
        return (f"{label}：該当{v['races']}R / 購入{v['bets']}点 / 的中{v['hits']}点\n"
                f"　的中率 {pct(v['hits'], v['bets'])} / 回収率 {pct(v['return'], v['stake'])} / "
                f"投資 {v['stake']:,}円 / 払戻 {v['return']:,}円")

    lines = [f"【日次サマリ】{now.strftime('%Y-%m-%d')}"]
    lines += [
        _fmt_line("S1","①"),
        _fmt_line("S2","②"),
        _fmt_line("S3","③"),
        _fmt_line("S4","④"),
        "",
        f"合計：投資 {total_stake:,}円 / 払戻 {total_return:,}円",
        f"　的中率 {pct(sum(per[s]['hits'] for s in per), sum(per[s]['bets'] for s in per))} / 回収率 {pct(total_return, total_stake)}",
    ]
    msg = "\n".join(lines)
    uids = load_user_ids_from_simple_col()
    push_line_text(uids, msg)
    sheet_upsert_notified(key, time.time(), "daily summary")

# ========= main / ループ =========
def main():
    try:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
        logging.info("[INFO] WATCHER RUN start")
        hits, matches = _scan_and_notify_once()
        logging.info("[INFO] HITS=%d / MATCHES=%d", hits, matches)
        summarize_today_and_notify()
        logging.info("[INFO] ジョブ終了")
    except Exception as e:
        logging.exception("[FATAL] 例外で終了: %s", e)
        raise

def run_watcher_forever(sleep_sec: int = 60):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    while True:
        try:
            _scan_and_notify_once()
            summarize_today_and_notify()
        except Exception as e:
            logging.exception("[FATAL] ループ例外: %s", e)
        time.sleep(max(10, sleep_sec))

if __name__ == "__main__":
    main()
