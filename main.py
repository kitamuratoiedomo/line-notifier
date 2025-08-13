# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視・通知バッチ
- 一覧で発走時刻取得
- 詳細/オッズ フォールバック（RIDアンカー近傍 & 「発走」文脈優先、ノイズ語除外）
- 窓内1回通知 / 429クールダウン / Sheet永続TTL
- 通知先：Googleシート(タブA=名称「1」)のH列から userId を収集
- 通知の「買い目」を 人気順＋馬番 の両表示に対応
"""

import os, re, json, time, random, logging, pathlib, hashlib
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Set

import requests
from bs4 import BeautifulSoup, Tag
from strategy_rules import eval_strategy

# ===== Google Sheets =====
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ========= 基本設定 =========
JST = timezone(timedelta(hours=9))
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
})
TIMEOUT = (10, 25)
RETRY = 3
SLEEP_BETWEEN = (0.6, 1.2)

LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"

# ========= 環境変数 =========
START_HOUR          = int(os.getenv("START_HOUR", "10"))
END_HOUR            = int(os.getenv("END_HOUR",   "22"))
DRY_RUN             = os.getenv("DRY_RUN", "False").lower() == "true"
KILL_SWITCH         = os.getenv("KILL_SWITCH", "False").lower() == "true"
NOTIFY_ENABLED      = os.getenv("NOTIFY_ENABLED", "1") == "1"
DEBUG_RACEIDS       = [s.strip() for s in os.getenv("DEBUG_RACEIDS", "").split(",") if s.strip()]

NOTIFY_TTL_SEC      = int(os.getenv("NOTIFY_TTL_SEC", "3600"))
NOTIFY_COOLDOWN_SEC = int(os.getenv("NOTIFY_COOLDOWN_SEC", "1800"))

WINDOW_BEFORE_MIN   = int(os.getenv("WINDOW_BEFORE_MIN", "15"))
WINDOW_AFTER_MIN    = int(os.getenv("WINDOW_AFTER_MIN", "-10"))

CUTOFF_OFFSET_MIN   = int(os.getenv("CUTOFF_OFFSET_MIN", "0"))
FORCE_RUN           = os.getenv("FORCE_RUN", "0") == "1"

LINE_ACCESS_TOKEN   = os.getenv("LINE_ACCESS_TOKEN", "")
# 後方互換の単一宛先（フォールバック用）
LINE_USER_ID        = os.getenv("LINE_USER_ID", "")
LINE_USER_IDS       = [s.strip() for s in os.getenv("LINE_USER_IDS", "").split(",") if s.strip()]

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_SHEET_ID         = os.getenv("GOOGLE_SHEET_ID", "")
# TTL管理タブ（名前 or gid）
GOOGLE_SHEET_TAB        = os.getenv("GOOGLE_SHEET_TAB", "notified")

# 送信先ユーザーを読むタブA（=「1」）と列（=H）
USERS_SHEET_NAME        = os.getenv("USERS_SHEET_NAME", "1")
USERS_USERID_COL        = os.getenv("USERS_USERID_COL", "H")

RACEID_RE   = re.compile(r"/RACEID/(\d{18})")
# 半角コロン, 全角コロン, 「時分」表記の3系統に対応
TIME_PATS = [
    re.compile(r"\b(\d{1,2}):(\d{2})\b"),
    re.compile(r"\b(\d{1,2})：(\d{2})\b"),
    re.compile(r"\b(\d{1,2})\s*時\s*(\d{1,2})\s*分\b"),
]
PLACEHOLDER = re.compile(r"\d{8}0000000000$")

# ノイズ／優先ラベル
IGNORE_NEAR_PAT = re.compile(r"(現在|更新|発売|締切|投票|オッズ|確定|払戻|実況)")
LABEL_NEAR_PAT  = re.compile(r"(発走|発走予定|発走時刻|発送|出走)")

# ========= 共通 =========
def now_jst() -> datetime:
    return datetime.now(JST)

def within_operating_hours() -> bool:
    if FORCE_RUN:
        return True
    h = now_jst().hour
    return START_HOUR <= h < END_HOUR

def fetch(url: str) -> str:
    last_err = None
    for i in range(1, RETRY + 1):
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

# ========= Google Sheets 共通 =========
def _sheet_service():
    if not GOOGLE_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        raise RuntimeError("Google Sheets の環境変数不足")
    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def _resolve_sheet_title(svc) -> str:
    """TTL管理用タブ（GOOGLE_SHEET_TAB）をタイトルに正規化"""
    tab = GOOGLE_SHEET_TAB
    meta = svc.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    sheets = meta.get("sheets", [])
    # 数値(gid)指定のときタイトルに解決
    if tab.isdigit() and len(tab) > 3:
        gid = int(tab)
        for s in sheets:
            if s["properties"]["sheetId"] == gid:
                return s["properties"]["title"]
        raise RuntimeError(f"指定gidのシートが見つかりません: {gid}")
    # 名前指定
    for s in sheets:
        if s["properties"]["title"] == tab:
            return tab
    # なければ作成
    body = {"requests": [{"addSheet": {"properties": {"title": tab}}}]}
    svc.spreadsheets().batchUpdate(spreadsheetId=GOOGLE_SHEET_ID, body=body).execute()
    return tab

# ========= Google Sheets 永続TTL =========
def sheet_load_notified() -> Dict[str, float]:
    svc = _sheet_service()
    title = _resolve_sheet_title(svc)
    rng = f"'{title}'!A:C"
    res = svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=rng).execute()
    values = res.get("values", [])
    start = 1 if values and values[0] and str(values[0][0]).upper() in ("KEY", "RACEID", "RID", "ID") else 0
    d: Dict[str, float] = {}
    for row in values[start:]:
        if not row or len(row) < 2:
            continue
        key = str(row[0]).strip()
        try:
            ts = float(row[1])
        except Exception:
            continue
        d[key] = ts
    return d

def sheet_upsert_notified(key: str, ts: float, note: str = "") -> None:
    svc = _sheet_service()
    title = _resolve_sheet_title(svc)
    rng = f"'{title}'!A:C"
    res = svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=rng).execute()
    values = res.get("values", [])
    header = ["KEY", "TS_EPOCH", "NOTE"]
    if not values:
        body = {"values": [header, [key, ts, note]]}
        svc.spreadsheets().values().update(
            spreadsheetId=GOOGLE_SHEET_ID, range=rng, valueInputOption="RAW", body=body
        ).execute()
        return
    start_row = 1 if values and values[0] and values[0][0] in header else 0
    found_row_idx = None
    for i, row in enumerate(values[start_row:], start=start_row):
        if row and str(row[0]).strip() == key:
            found_row_idx = i
            break
    body = {"values": [[key, ts, note]]}
    if found_row_idx is not None:
        row_no = found_row_idx + 1
        rng_row = f"'{title}'!A{row_no}:C{row_no}"
        svc.spreadsheets().values().update(
            spreadsheetId=GOOGLE_SHEET_ID, range=rng_row, valueInputOption="RAW", body=body
        ).execute()
    else:
        svc.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID, range=rng, valueInputOption="RAW",
            insertDataOption="INSERT_ROWS", body=body
        ).execute()

# ========= 送信先ユーザー読み込み（タブA=名称「1」のH列） =========
def load_user_ids_from_simple_col() -> List[str]:
    """
    USERS_SHEET_NAME（既定 '1'）の USERS_USERID_COL（既定 'H'）列から userId を収集。
    - 空白/ヘッダーっぽい値は除外
    - 'U' で始まる文字列のみ採用
    - 重複排除
    """
    svc = _sheet_service()
    title = USERS_SHEET_NAME
    col = USERS_USERID_COL.upper()
    rng = f"'{title}'!{col}:{col}"
    res = svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=rng).execute()
    values = res.get("values", [])
    user_ids: List[str] = []
    for i, row in enumerate(values):
        v = (row[0].strip() if row and row[0] is not None else "")
        if not v:
            continue
        low = v.replace(" ", "").lower()
        if i == 0 and ("userid" in low or "user id" in low or "line" in low):
            # 1行目のヘッダー回避
            continue
        if not v.startswith("U"):
            continue
        user_ids.append(v)

    # 重複除去
    uniq: Dict[str, bool] = {}
    out: List[str] = []
    for uid in user_ids:
        if uid not in uniq:
            uniq[uid] = True
            out.append(uid)

    logging.info("[INFO] usersシート読込(H列固定): %d件 from tab=%s", len(out), title)
    return out

# ========= ユーティリティ =========
def _extract_raceids_from_soup(soup: BeautifulSoup) -> List[str]:
    rids: List[str] = []
    for a in soup.find_all("a", href=True):
        m = RACEID_RE.search(a["href"])
        if m:
            rid = m.group(1)
            if not PLACEHOLDER.search(rid):
                rids.append(rid)
    return sorted(set(rids))

def _row_text_snippet(el: Tag, maxlen: int = 80) -> str:
    try:
        t = " ".join(list(el.stripped_strings))
        return (t[:maxlen] + "…") if len(t) > maxlen else t
    except Exception:
        return "-"

def _rid_date_parts(rid: str) -> Tuple[int, int, int]:
    return int(rid[0:4]), int(rid[4:6]), int(rid[6:8])

def _norm_hhmm_from_text(text: str) -> Optional[Tuple[int,int,str]]:
    if not text:
        return None
    s = str(text)
    for pat, tag in zip(TIME_PATS, ("half", "full", "kanji")):
        m = pat.search(s)
        if m:
            hh = int(m.group(1)); mm = int(m.group(2))
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
        for attr in ("datetime", "data-time", "title", "aria-label"):
            v = t.get(attr)
            if v:
                got = _norm_hhmm_from_text(v)
                if got:
                    hh, mm, why = got
                    return f"{hh:02d}:{mm:02d}", f"time@{attr}/{why}"
        got = _norm_hhmm_from_text(t.get_text(" ", strip=True))
        if got:
            hh, mm, why = got
            return f"{hh:02d}:{mm:02d}", f"time@text/{why}"

    # data-*属性
    for node in el.find_all(True, recursive=True):
        for attr in ("data-starttime", "data-start-time", "data-time", "title", "aria-label"):
            v = node.get(attr)
            if not v: continue
            got = _norm_hhmm_from_text(v)
            if got:
                hh, mm, why = got
                return f"{hh:02d}:{mm:02d}", f"data:{attr}/{why}"

    # よくあるクラス名
    for sel in [".startTime", ".cellStartTime", ".raceTime", ".time", ".start-time"]:
        node = el.select_one(sel)
        if node:
            got = _norm_hhmm_from_text(node.get_text(" ", strip=True))
            if got:
                hh, mm, why = got
                return f"{hh:02d}:{mm:02d}", f"sel:{sel}/{why}"

    # テキスト本体
    got = _norm_hhmm_from_text(el.get_text(" ", strip=True))
    if got:
        hh, mm, why = got
        return f"{hh:02d}:{mm:02d}", f"row:text/{why}"
    return None, "-"

# ========= 発走時刻（一覧ページ）解析 =========
def parse_post_times_from_table_like(root: Tag) -> Dict[str, datetime]:
    post_map: Dict[str, datetime] = {}

    # 1) テーブル
    for table in root.find_all("table"):
        thead = table.find("thead")
        if thead:
            head_text = "".join(thead.stripped_strings)
            if not any(k in head_text for k in ("発走", "発走時刻", "レース")):
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
            hhmm, reason = _find_time_nearby(tr)
            if not hhmm:
                logging.debug(f"[DEBUG] 発走見つからず(table row) rid={rid} row='{_row_text_snippet(tr)}'")
                continue
            hh, mm = map(int, hhmm.split(":"))
            dt = _make_dt_from_hhmm(rid, hh, mm)
            if dt:
                post_map[rid] = dt
                logging.debug(f"[DEBUG] 発走抽出 OK rid={rid} {dt:%H:%M} via {reason}")

    # 2) カード型
    for a in root.find_all("a", href=True):
        m = RACEID_RE.search(a["href"])
        if not m: continue
        rid = m.group(1)
        if PLACEHOLDER.search(rid) or rid in post_map:
            continue

        host = None
        depth = 0
        for parent in a.parents:
            if isinstance(parent, Tag) and parent.name in ("tr", "li", "div", "section", "article"):
                host = parent; break
            depth += 1
            if depth >= 6: break
        host = host or a

        hhmm, reason = _find_time_nearby(host)
        if not hhmm:
            sib_text = " ".join([x.get_text(" ", strip=True) for x in a.find_all_next(limit=4) if isinstance(x, Tag)])
            got = _norm_hhmm_from_text(sib_text)
            if got:
                hh, mm, why = got
                hhmm, reason = f"{hh:02d}:{mm:02d}", f"next:text/{why}"
        if not hhmm:
            continue

        hh, mm = map(int, hhmm.split(":"))
        dt = _make_dt_from_hhmm(rid, hh, mm)
        if dt:
            post_map[rid] = dt
            logging.debug(f"[DEBUG] 発走抽出 OK rid={rid} {dt:%H:%M} via {reason} (card)")
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

    _merge_from(f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000", "list:today")
    _merge_from(f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd_next}0000000000", "list:tomorrow")

    logging.info(f"[INFO] 発走時刻取得: {len(post_map)}件")
    return post_map

# ========= オッズ解析（単複ページ） =========
def _clean(s: str) -> str:
    return re.sub(r"\s+", "", s or "")

def _as_float(text: str) -> Optional[float]:
    if not text:
        return None
    t = text.replace(",", "").strip()
    if "%" in t or "-" in t or "～" in t or "~" in t:
        return None
    m = re.search(r"\d+(?:\.\d+)?", t)
    return float(m.group(0)) if m else None

def _as_int(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else None

def _find_popular_odds_table(soup: BeautifulSoup) -> Tuple[Optional[BeautifulSoup], Dict[str, int]]:
    """
    人気列(pop)、単勝オッズ列(win)に加え、馬番列(num)も特定する
    """
    for table in soup.find_all("table"):
        thead = table.find("thead")
        if not thead:
            continue
        ths = thead.find_all(["th", "td"])
        headers = [_clean(th.get_text()) for th in ths]
        if not headers:
            continue

        # 人気列
        pop_idx = None
        for i, h in enumerate(headers):
            if h in ("人気", "順位") or ("人気" in h and "順" not in h):
                pop_idx = i; break

        # 単勝オッズ列（優先度: 完全一致「単勝」> 部分一致 > 「オッズ」）
        win_candidates = []
        for i, h in enumerate(headers):
            if ("複" in h) or ("率" in h) or ("%" in h): continue
            if h == "単勝": win_candidates.append((0, i))
            elif "単勝" in h: win_candidates.append((1, i))
            elif "オッズ" in h: win_candidates.append((2, i))
        win_idx = sorted(win_candidates, key=lambda x: x[0])[0][1] if win_candidates else None

        # 馬番列
        num_idx = None
        for i, h in enumerate(headers):
            if h == "馬番" or "馬番" in h:
                num_idx = i; break
        if num_idx is None:
            # 予備: 「馬」含むが「馬名」ではない列を候補に
            for i, h in enumerate(headers):
                if ("馬" in h) and ("馬名" not in h) and (i != pop_idx):
                    num_idx = i; break

        if pop_idx is None or win_idx is None:
            continue

        # テーブルっぽさの軽い検証（人気1→2の昇順が2行以上）
        body = table.find("tbody") or table
        rows = body.find_all("tr")
        seq_ok, last = 0, 0
        for tr in rows[:6]:
            tds = tr.find_all(["td", "th"])
            if len(tds) <= max(pop_idx, win_idx): continue
            s = tds[pop_idx].get_text(strip=True)
            if not s.isdigit(): break
            v = int(s)
            if v <= last: break
            last = v; seq_ok += 1
        if seq_ok >= 2:
            sample = []
            for tr in rows[:2]:
                tds = tr.find_all(["td", "th"])
                if len(tds) > win_idx:
                    sample.append(tds[win_idx].get_text(" ", strip=True))
            logging.info(f"[DEBUG] headers={headers} / pop_idx={pop_idx} / win_idx={win_idx} / num_idx={num_idx} / win_samples={sample}")
            return table, {"pop": pop_idx, "win": win_idx, "num": num_idx if num_idx is not None else -1}
    return None, {}

def parse_odds_table(soup: BeautifulSoup) -> Tuple[List[Dict[str, float]], Optional[str], Optional[str]]:
    venue_race = (soup.find("h1").get_text(strip=True) if soup.find("h1") else None)
    nowtime = soup.select_one(".withUpdate .nowTime") or soup.select_one(".nowTime")
    now_label = nowtime.get_text(strip=True) if nowtime else None

    table, idx = _find_popular_odds_table(soup)
    if not table:
        return [], venue_race, now_label

    pop_idx = idx["pop"]; win_idx = idx["win"]; num_idx = idx.get("num", -1)
    horses: List[Dict[str, float]] = []
    body = table.find("tbody") or table
    for tr in body.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if len(tds) <= max(pop_idx, win_idx): continue

        # 人気
        pop_txt = tds[pop_idx].get_text(strip=True)
        if not pop_txt.isdigit(): continue
        pop = int(pop_txt)
        if not (1 <= pop <= 30): continue

        # 単勝オッズ
        odds = _as_float(tds[win_idx].get_text(" ", strip=True))
        if odds is None: continue

        # 馬番（あれば）
        num = None
        if 0 <= num_idx < len(tds):
            num = _as_int(tds[num_idx].get_text(" ", strip=True))

        rec = {"pop": pop, "odds": float(odds)}
        if num is not None:
            rec["num"] = num
        horses.append(rec)

    # 人気でユニーク化
    uniq = {}
    for h in sorted(horses, key=lambda x: x["pop"]):
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

# ========= 発走時刻フォールバック（厳密版） =========
def fallback_post_time_for_rid(rid: str) -> Optional[Tuple[datetime, str, str]]:
    def _from_list_page() -> Optional[Tuple[datetime, str, str]]:
        url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{rid}"
        logging.info("[INFO] 詳細fallback開始 rid=%s url=%s", rid, url)
        soup = BeautifulSoup(fetch(url), "lxml")

        a = soup.find("a", href=re.compile(rf"/RACEID/{rid}"))
        if not a:
            return None

        host = None
        for parent in a.parents:
            if isinstance(parent, Tag) and parent.name in ("tr", "li", "div", "section", "article"):
                host = parent
                break
        host = host or a

        hhmm, reason = _find_time_nearby(host)
        if not hhmm:
            sibs = [n for n in host.find_all_next(limit=6) if isinstance(n, Tag)]
            text = " ".join([n.get_text(" ", strip=True) for n in sibs])
            if not IGNORE_NEAR_PAT.search(text):
                got = _norm_hhmm_from_text(text)
                if got:
                    hh, mm, why = got
                    hhmm, reason = f"{hh:02d}:{mm:02d}", f"sibling:text/{why}"
        if not hhmm:
            return None

        hh, mm = map(int, hhmm.split(":"))
        dt = _make_dt_from_hhmm(rid, hh, mm)
        if dt:
            logging.info("[INFO] 発走(詳細fallback)取得 rid=%s 発走=%s via %s (%s)",
                         rid, dt.strftime("%H:%M"), f"list-anchor/{reason}", url)
            return dt, f"list-anchor/{reason}", url
        return None

    def _from_tanfuku_page() -> Optional[Tuple[datetime, str, str]]:
        url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"
        soup = BeautifulSoup(fetch(url), "lxml")

        for key in ("発走", "発走時刻", "発走予定", "発送", "出走"):
            for node in soup.find_all(string=re.compile(key)):
                el = getattr(node, "parent", None) or soup
                container = el
                for parent in el.parents:
                    if isinstance(parent, Tag) and parent.name in ("div", "section", "article", "li"):
                        container = parent
                        break

                chunks = []
                try:
                    chunks.append(container.get_text(" ", strip=True))
                except Exception:
                    pass
                for sub in container.find_all(True, limit=6):
                    try:
                        chunks.append(sub.get_text(" ", strip=True))
                    except Exception:
                        pass
                near = " ".join(chunks)

                if IGNORE_NEAR_PAT.search(near):
                    continue
                got = _norm_hhmm_from_text(near)
                if got:
                    hh, mm, why = got
                    dt = _make_dt_from_hhmm(rid, hh, mm)
                    if dt:
                        logging.info("[INFO] 詳細fallback成功 rid=%s %s via %s (%s)",
                                     rid, dt.strftime("%H:%M"), f"tanfuku-label/{key}/{why}", url)
                        return dt, f"tanfuku-label/{key}/{why}", url

        for t in soup.find_all("time"):
            for attr in ("datetime", "data-time", "title", "aria-label"):
                v = t.get(attr)
                if not v:
                    continue
                around = f"{v} {t.get_text(' ', strip=True)}"
                if IGNORE_NEAR_PAT.search(around) and not LABEL_NEAR_PAT.search(around):
                    continue
                got = _norm_hhmm_from_text(around)
                if got:
                    hh, mm, why = got
                    dt = _make_dt_from_hhmm(rid, hh, mm)
                    if dt:
                        logging.info("[INFO] 詳細fallback成功 rid=%s %s via %s (%s)",
                                     rid, dt.strftime("%H:%M"), f"tanfuku-time@{attr}/{why}", url)
                        return dt, f"tanfuku-time@{attr}/{why}", url
            txt = t.get_text(" ", strip=True)
            if IGNORE_NEAR_PAT.search(txt) and not LABEL_NEAR_PAT.search(txt):
                continue
            got = _norm_hhmm_from_text(txt)
            if got:
                hh, mm, why = got
                dt = _make_dt_from_hhmm(rid, hh, mm)
                if dt:
                    logging.info("[INFO] 詳細fallback成功 rid=%s %s via %s (%s)",
                                 rid, dt.strftime("%H:%M"), f"tanfuku-time@text/{why}", url)
                    return dt, f"tanfuku-time@text/{why}", url

        full = soup.get_text(" ", strip=True)
        m = re.search(r"(発走|発走予定|発走時刻|発送|出走)[^0-9]{0,10}(\d{1,2})[:：](\d{2})", full)
        if m:
            hh, mm = int(m.group(2)), int(m.group(3))
            dt = _make_dt_from_hhmm(rid, hh, mm)
            if dt:
                logging.info("[INFO] 詳細fallback成功 rid=%s %s via %s (%s)",
                             rid, dt.strftime("%H:%M"), "tanfuku-fulltext/label-inline", url)
                return dt, "tanfuku-fulltext/label-inline", url

        best = None  # (hh, mm, score)
        for m in re.finditer(r"\b(\d{1,2})[:：](\d{2})\b", full):
            hh, mm = int(m.group(1)), int(m.group(2))
            if not (0 <= hh <= 23 and 0 <= mm <= 59):
                continue
            ctx = full[max(0, m.start()-120):min(len(full), m.end()+120)]
            if IGNORE_NEAR_PAT.search(ctx):
                continue
            score = 1 + (2 if LABEL_NEAR_PAT.search(ctx) else 0)
            if not best or score > best[2]:
                best = (hh, mm, score)
        if best:
            dt = _make_dt_from_hhmm(rid, best[0], best[1])
            if dt:
                logging.info("[INFO] 詳細fallback成功 rid=%s %s via %s (%s)",
                             rid, dt.strftime("%H:%M"), "tanfuku-fulltext/with-context", url)
                return dt, "tanfuku-fulltext/with-context", url

        return None

    # 実行順序：list → tanfuku
    try:
        got = _from_list_page()
        if got:
            return got
    except Exception as e:
        logging.warning("[WARN] 詳細fallback(list)失敗 rid=%s: %s", rid, e)

    try:
        got = _from_tanfuku_page()
        if got:
            return got
    except Exception as e:
        logging.warning("[WARN] 詳細fallback(tanfuku)失敗 rid=%s: %s", rid, e)

    return None

# ========= RACEID 取得 =========
def list_raceids_today_ticket(ymd: str) -> List[str]:
    url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    soup = BeautifulSoup(fetch(url), "lxml")
    ids = _extract_raceids_from_soup(soup)
    logging.info(f"[INFO] Rakuten#1 本日の発売情報: {len(ids)}件")
    return ids

def list_raceids_from_card_lists(ymd: str, ymd_next: str) -> List[str]:
    urls = [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000",
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd_next}0000000000",
    ]
    rids: List[str] = []
    for u in urls:
        try:
            soup = BeautifulSoup(fetch(u), "lxml")
            rids.extend(_extract_raceids_from_soup(soup))
        except Exception as e:
            logging.warning(f"[WARN] 出馬表一覧スキャン失敗: {e} ({u})")
    rids = sorted(set(rids))
    logging.info(f"[INFO] Rakuten#2 出馬表一覧: {len(rids)}件")
    return rids

# ========= ウィンドウ判定 =========
def is_within_window(post_time: datetime, now: datetime) -> bool:
    if CUTOFF_OFFSET_MIN > 0 and now >= (post_time - timedelta(minutes=CUTOFF_OFFSET_MIN)):
        return False
    win_start = post_time - timedelta(minutes=WINDOW_BEFORE_MIN)
    win_end   = post_time + timedelta(minutes=WINDOW_AFTER_MIN)
    return (win_start <= now <= win_end)

# ========= LINE送信 =========
def push_line_text(user_id: str, token: str, text: str, timeout=8, retries=1):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": text}]}
    for attempt in range(retries + 1):
        try:
            resp = requests.post(LINE_PUSH_URL, headers=headers, json=payload, timeout=timeout)
            req_id = resp.headers.get("X-Line-Request-Id", "-")
            body   = resp.text
            logging.info("[LINE] status=%s req_id=%s body=%s", resp.status_code, req_id, body[:200])
            if resp.status_code == 200:
                return True, 200, body
            if resp.status_code == 429 and attempt < retries:
                wait = int(resp.headers.get("Retry-After", "1"))
                logging.warning("[LINE] 429 Too Many Requests -> retry in %ss", wait)
                time.sleep(max(wait, 1)); continue
            logging.error("[ERROR] LINE push failed status=%s body=%s", resp.status_code, body[:200])
            return False, resp.status_code, body
        except requests.RequestException as e:
            logging.exception("[ERROR] LINE push exception (attempt %s): %s", attempt + 1, e)
            if attempt < retries:
                time.sleep(2); continue
            return False, None, str(e)

def notify_strategy_hit_to_many(message_text: str, targets: List[str]):
    if not NOTIFY_ENABLED:
        logging.info("[INFO] NOTIFY_ENABLED=0 のため通知スキップ"); return False, None
    if DRY_RUN:
        logging.info("[DRY_RUN] 通知メッセージ:\n%s", message_text); return False, None
    if not LINE_ACCESS_TOKEN:
        logging.error("[ERROR] LINE 環境変数不足（LINE_ACCESS_TOKEN）"); return False, None
    if not targets:
        logging.error("[ERROR] 送信先ユーザーIDが空（usersシート未設定？）"); return False, None

    all_ok = True
    last_status = None
    for uid in targets:
        ok, status, body = push_line_text(uid, LINE_ACCESS_TOKEN, message_text)
        last_status = status
        if not ok:
            all_ok = False
            logging.warning("[WARN] LINE送信失敗 user=%s status=%s body=%s", uid, status, (body or "")[:200])
        time.sleep(0.2)  # 軽い間隔（429対策）
    return all_ok, last_status

# ========= 通知メッセージ整形 =========
_CIRCLED = "①②③④⑤⑥⑦⑧⑨"
def _circled(n: int) -> str:
    return _CIRCLED[n-1] if 1 <= n <= 9 else f"{n}."

def _extract_hhmm_label(s: str) -> Optional[str]:
    got = _norm_hhmm_from_text(s)
    if not got: return None
    hh, mm, _ = got
    return f"{hh:02d}:{mm:02d}"

def _infer_pattern_no(strategy_text: str) -> int:
    if not strategy_text: return 0
    m = re.match(r"\s*([①-⑨])", strategy_text)
    if m:
        circ = m.group(1)
        return _CIRCLED.index(circ) + 1
    m = re.match(r"\s*(\d+)", strategy_text)
    if m:
        try: return int(m.group(1))
        except: return 0
    return 0

def _strip_pattern_prefix(strategy_text: str) -> str:
    if not strategy_text: return ""
    s = re.sub(r"^\s*[①-⑨]\s*", "", strategy_text)
    s = re.sub(r"^\s*\d+\s*", "", s)
    return s.strip()

def _split_venue_race(venue_race: str) -> Tuple[str, str]:
    if not venue_race:
        return "地方競馬", ""
    m = re.search(r"^\s*([^\s\d]+)\s*(\d{1,2}R)\b", venue_race)
    if m:
        venue = m.group(1)
        race = m.group(2)
        if "競馬" not in venue:
            venue_disp = f"{venue}競馬場"
        else:
            venue_disp = venue
        return venue_disp, race
    return venue_race, ""

def _parse_ticket_as_pops(ticket: str) -> List[int]:
    """'1-2-3' → [1,2,3]（人気順位として解釈）"""
    parts = [p.strip() for p in re.split(r"[-→>〜~]", str(ticket)) if p.strip()]
    pops: List[int] = []
    for p in parts:
        m = re.search(r"\d+", p)
        if not m: continue
        try:
            pops.append(int(m.group(0)))
        except:
            pass
    return pops

def _format_bets_pop_and_umanum(bets: List[str], horses: List[Dict[str, float]]) -> List[str]:
    """
    eval_strategy の買い目（人気順位ベースと想定）を、
    「X番人気（馬番 Y）」の連結表示へ変換
    """
    # 人気→馬番のマップ
    pop2num: Dict[int, Optional[int]] = {}
    for h in horses:
        pop = int(h.get("pop"))
        num = h.get("num")  # ない場合もある
        pop2num[pop] = int(num) if isinstance(num, int) else None

    out: List[str] = []
    for bet in bets:
        pops = _parse_ticket_as_pops(bet)
        if not pops:
            out.append(bet)  # 形式が違う場合はそのまま
            continue
        segs: List[str] = []
        for p in pops:
            n = pop2num.get(p)
            if n is None:
                segs.append(f"{p}番人気")
            else:
                segs.append(f"{p}番人気（馬番 {n}）")
        out.append(" - ".join(segs))
    return out

def build_line_notification(
    pattern_no: int,
    venue: str,
    race_no: str,
    time_label: str,     # "発走" or "締切"
    time_hm: str,        # "HH:MM"
    condition_text: str,
    bets: List[str],
    odds_timestamp_hm: Optional[str],
    odds_url: str,
    header_emoji: str = "🚨",
) -> str:
    lines = [
        f"{header_emoji}【戦略{pattern_no if pattern_no>0 else ''} ヒット】".replace("戦略 ヒット","戦略ヒット"),
        f"{venue} {race_no}（{time_label} {time_hm}）".strip(),
        f"条件: {condition_text}",
        "",
        "買い目:",
    ]
    for i, bet in enumerate(bets, 1):
        lines.append(f"{_circled(i)} {bet}")
    if odds_timestamp_hm:
        lines += ["", f"📅 オッズ時点: {odds_timestamp_hm}"]
    lines += ["🔗 オッズ詳細:", odds_url]
    return "\n".join(lines)

# ========= メイン =========
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # ビルド識別
    p = pathlib.Path(__file__).resolve()
    sha = hashlib.sha1(p.read_bytes()).hexdigest()[:12]
    logging.info(f"[BUILD] file={p} mtime={p.stat().st_mtime:.0f} sha1={sha} Fallback=ON v2025-08-13A")

    if KILL_SWITCH:
        logging.info("[INFO] KILL_SWITCH=True のため終了"); return
    if not within_operating_hours():
        logging.info(f"[INFO] 監視休止（JST={now_jst():%H:%M} 稼働={START_HOUR:02d}:00-{END_HOUR:02d}:00）"); return

    logging.info("[INFO] ジョブ開始")
    logging.info(f"[INFO] DRY_RUN={DRY_RUN} NOTIFY_ENABLED={'1' if NOTIFY_ENABLED else '0'} "
                 f"TTL={NOTIFY_TTL_SEC}s CD={NOTIFY_COOLDOWN_SEC}s WIN=-{WINDOW_BEFORE_MIN}m/{WINDOW_AFTER_MIN:+}m "
                 f"CUTOFF={CUTOFF_OFFSET_MIN}m")

    # 送信対象ユーザーの読み込み（タブA=「1」のH列 → フォールバック: 環境変数）
    try:
        targets = load_user_ids_from_simple_col()
        if not targets:
            fb = LINE_USER_IDS if LINE_USER_IDS else ([LINE_USER_ID] if LINE_USER_ID else [])
            targets = fb
        logging.info("[INFO] 送信ターゲット数: %d", len(targets))
    except Exception as e:
        logging.exception("[ERROR] usersシート読込失敗: %s", e)
        fb = LINE_USER_IDS if LINE_USER_IDS else ([LINE_USER_ID] if LINE_USER_ID else [])
        targets = fb
        logging.info("[INFO] フォールバック送信ターゲット数: %d", len(targets))

    # 永続TTLロード
    try:
        notified = sheet_load_notified()
    except Exception as e:
        logging.exception("[ERROR] TTLロード失敗（Google Sheets）: %s", e)
        notified = {}

    # RACEID列挙
    if DEBUG_RACEIDS:
        logging.info(f"[INFO] DEBUG_RACEIDS 指定: {len(DEBUG_RACEIDS)}件")
        target_raceids = [rid for rid in DEBUG_RACEIDS if not PLACEHOLDER.search(rid)]
        post_time_map: Dict[str, datetime] = {}
    else:
        ymd = now_jst().strftime("%Y%m%d")
        ymd_next = (now_jst() + timedelta(days=1)).strftime("%Y%m%d")
        r1 = list_raceids_today_ticket(ymd)
        r2 = list_raceids_from_card_lists(ymd, ymd_next)
        target_raceids = sorted(set(r1) | set(r2))
        post_time_map = collect_post_time_map(ymd, ymd_next)
        valid = [rid for rid in target_raceids if not PLACEHOLDER.search(rid)]
        logging.info(f"[INFO] 発見RACEID数(有効のみ): {len(valid)}")
        for rid in valid:
            logging.info(f"  - {rid} -> tanfuku")
        target_raceids = valid

    hits = 0; matches = 0
    seen_in_this_run: Set[str] = set()

    for rid in target_raceids:
        if rid in seen_in_this_run:
            logging.info(f"[SKIP] 同一ジョブ内去重: {rid}"); continue
        # TTL/CD 抑制
        now_ts = time.time()
        cd_ts = notified.get(f"{rid}:cd")
        if cd_ts and (now_ts - cd_ts) < NOTIFY_COOLDOWN_SEC:
            logging.info(f"[SKIP] クールダウン中: {rid}"); continue
        ts = notified.get(rid)
        if ts and (now_ts - ts) < NOTIFY_TTL_SEC:
            logging.info(f"[SKIP] TTL内再通知抑制: {rid}"); continue

        post_time = post_time_map.get(rid)
        via = "list"
        if not post_time:
            logging.info(f"[DEBUG] post_time not found in list, try fallback rid={rid}")
            got = fallback_post_time_for_rid(rid)
            if got:
                post_time, via, url = got
                via = f"detail:{via}"
            else:
                logging.info(f"[SKIP] 発走時刻不明のため通知保留: {rid}")
                continue

        now = now_jst()
        if not is_within_window(post_time, now):
            delta_min = int((post_time - now).total_seconds() // 60)
            logging.info(f"[SKIP] 窓外({delta_min:+}m) rid={rid} 発走={post_time:%H:%M} via={via}")
            continue

        meta = check_tanfuku_page(rid)
        if not meta:
            time.sleep(random.uniform(*SLEEP_BETWEEN)); continue

        horses = meta["horses"]
        if len(horses) < 4:
            logging.info(f"[NO MATCH] {rid} 条件詳細: horses<4 で判定不可")
            time.sleep(random.uniform(*SLEEP_BETWEEN)); continue

        try:
            odds_log = ", ".join([
                f"{h.get('pop')}番人気:単{h.get('odds')}" + (f"/馬番{h.get('num')}" if 'num' in h else "")
                for h in sorted(horses, key=lambda x: x['pop'])
            ])
        except Exception:
            odds_log = str(horses)
        logging.info(f"[DEBUG] {rid} 取得オッズ: {odds_log}")

        hits += 1
        strategy = eval_strategy(horses, logger=logging)
        if strategy:
            matches += 1

            strategy_text = strategy.get("strategy", "")
            pattern_no = _infer_pattern_no(strategy_text)
            condition_text = _strip_pattern_prefix(strategy_text) or strategy_text

            venue_disp, race_no = _split_venue_race(meta.get("venue_race", ""))

            time_label = "発走" if CUTOFF_OFFSET_MIN == 0 else "締切"
            display_dt = post_time if CUTOFF_OFFSET_MIN == 0 else (post_time - timedelta(minutes=CUTOFF_OFFSET_MIN))
            time_hm = display_dt.strftime("%H:%M")

            odds_hm = _extract_hhmm_label(meta.get("now", ""))

            raw_tickets = strategy.get("tickets", [])
            if isinstance(raw_tickets, str):
                raw_tickets = [s.strip() for s in raw_tickets.split(",") if s.strip()]

            # ここで「人気＋馬番」表記へ
            pretty_tickets = _format_bets_pop_and_umanum(raw_tickets, horses)

            message = build_line_notification(
                pattern_no=pattern_no,
                venue=venue_disp,
                race_no=race_no,
                time_label=time_label,
                time_hm=time_hm,
                condition_text=condition_text,
                bets=pretty_tickets,
                odds_timestamp_hm=odds_hm,
                odds_url=meta["url"],
            )

            # ログ詳細
            ticket_str = ", ".join(pretty_tickets)
            detail = f"{strategy_text} / 買い目: {ticket_str}"
            if "roi" in strategy or "hit" in strategy:
                detail += f" / {strategy.get('roi','-')} / {strategy.get('hit','-')}"
            logging.info(f"[MATCH] {rid} 条件詳細: {detail}")

            sent_ok, http_status = notify_strategy_hit_to_many(message, targets)

            now_epoch = time.time()
            if sent_ok:
                try:
                    sheet_upsert_notified(rid, now_epoch, note=f"{meta['venue_race']} {post_time:%H:%M}")
                    notified[rid] = now_epoch
                except Exception as e:
                    logging.exception("[ERROR] TTL更新失敗（Google Sheets）: %s", e)
                seen_in_this_run.add(rid)
            elif http_status == 429:
                try:
                    key_cd = f"{rid}:cd"
                    sheet_upsert_notified(key_cd, now_epoch, note=f"429 cooldown {meta['venue_race']} {post_time:%H:%M}")
                    notified[key_cd] = now_epoch
                except Exception as e:
                    logging.exception("[ERROR] CD更新失敗（Google Sheets）: %s", e)
                logging.warning("[WARN] 429クールダウン発動 rid=%s cool_down=%ss", rid, NOTIFY_COOLDOWN_SEC)
            else:
                logging.warning("[WARN] TTL未更新（通知未達/スキップ） rid=%s", rid)
        else:
            logging.info(f"[NO MATCH] {rid} 条件詳細: パターン①〜④に非該当")

        time.sleep(random.uniform(*SLEEP_BETWEEN))

    logging.info(f"[INFO] HITS={hits} / MATCHES={matches}")
    logging.info("[INFO] ジョブ終了")

if __name__ == "__main__":
    main()