# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視・通知バッチ（一覧のみで発走時刻取得 / 窓内1回通知 / 429クールダウン / Sheet永続TTL）
- 稼働時間: JST 10:00〜22:00（START_HOUR / END_HOUR）
- RACEID列挙: #todaysTicket + 出馬表一覧（当日/翌日）
- 発走時刻: 上記一覧から抽出（<time datetime> / data-starttime系 / 行テキストのHH:MM）
- 人気順テーブル: “人気/順位”列と“単勝/オッズ”列のみ厳密抽出（%・複勝レンジ除外）
- 通知ウィンドウ: 発走 { -WINDOW_BEFORE_MIN 分 〜 WINDOW_AFTER_MIN 分 } に入った時のみ判定・送信
- LINEは 200 OK の時だけTTL更新。429は :cd キーにクールダウンTSを記録。
"""

import os, re, json, time, random, logging
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

# 通常TTL（送達成功時のみ）
NOTIFY_TTL_SEC      = int(os.getenv("NOTIFY_TTL_SEC", "3600"))
# 429クールダウンTTL（:cd キー）
NOTIFY_COOLDOWN_SEC = int(os.getenv("NOTIFY_COOLDOWN_SEC", "1800"))

# 通知ウィンドウ（例: BEFORE=15, AFTER=-10 → 発走15分前〜10分前の間に1回）
WINDOW_BEFORE_MIN   = int(os.getenv("WINDOW_BEFORE_MIN", "15"))
WINDOW_AFTER_MIN    = int(os.getenv("WINDOW_AFTER_MIN", "-10"))

# 任意：直前の変化を考慮して、発走−N分以降は送らない
CUTOFF_OFFSET_MIN   = int(os.getenv("CUTOFF_OFFSET_MIN", "0"))
FORCE_RUN           = os.getenv("FORCE_RUN", "0") == "1"

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "")
LINE_USER_ID      = os.getenv("LINE_USER_ID", "")

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_SHEET_ID         = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_SHEET_TAB        = os.getenv("GOOGLE_SHEET_TAB", "notified")  # シート名 or gid(数値)

RACEID_RE   = re.compile(r"/RACEID/(\d{18})")
TIME_RE     = re.compile(r"\b(\d{1,2}):(\d{2})\b")
PLACEHOLDER = re.compile(r"\d{8}0000000000$")

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

# ========= Google Sheets 永続TTL =========
def _sheet_service():
    if not GOOGLE_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        raise RuntimeError("Google Sheets の環境変数不足")
    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def _resolve_sheet_title(svc) -> str:
    """環境変数 GOOGLE_SHEET_TAB がシート名 or gid(数値)のどちらでも解決"""
    tab = GOOGLE_SHEET_TAB
    meta = svc.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    sheets = meta.get("sheets", [])
    if tab.isdigit():
        gid = int(tab)
        for s in sheets:
            if s["properties"]["sheetId"] == gid:
                return s["properties"]["title"]
        raise RuntimeError(f"指定gidのシートが見つかりません: {gid}")
    else:
        for s in sheets:
            if s["properties"]["title"] == tab:
                return tab
        body = {"requests": [{"addSheet": {"properties": {"title": tab}}}]}
        svc.spreadsheets().batchUpdate(spreadsheetId=GOOGLE_SHEET_ID, body=body).execute()
        return tab

def sheet_load_notified() -> Dict[str, float]:
    """A列:キー(RACEID or RACEID:cd), B列:epoch, C列:NOTE(任意)"""
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
    """keyは RACEID または RACEID:cd（クールダウン）"""
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

def should_skip_by_ttl(notified: Dict[str, float], rid: str) -> bool:
    """成功TTL または 429クールダウンTTL が生きていればスキップ"""
    now = time.time()
    cd_ts = notified.get(f"{rid}:cd")
    if cd_ts and (now - cd_ts) < NOTIFY_COOLDOWN_SEC:
        return True
    ts = notified.get(rid)
    if ts and (now - ts) < NOTIFY_TTL_SEC:
        return True
    return False

# ========= RACEID抽出（共通） =========
def _extract_raceids_from_soup(soup: BeautifulSoup) -> List[str]:
    rids: List[str] = []
    for a in soup.find_all("a", href=True):
        m = RACEID_RE.search(a["href"])
        if m:
            rid = m.group(1)
            if not PLACEHOLDER.search(rid):  # 無効なプレースホルダ除外
                rids.append(rid)
    return sorted(set(rids))

# ========= 発走時刻（一覧ページ） =========
def _row_text_snippet(el: Tag, maxlen: int = 80) -> str:
    try:
        t = " ".join(list(el.stripped_strings))
        return (t[:maxlen] + "…") if len(t) > maxlen else t
    except Exception:
        return "-"

def _rid_date_parts(rid: str) -> Tuple[int, int, int]:
    return int(rid[0:4]), int(rid[4:6]), int(rid[6:8])

def _make_dt_from_hhmm(rid: str, hh: int, mm: int) -> Optional[datetime]:
    try:
        y, mon, d = _rid_date_parts(rid)
        return datetime(y, mon, d, hh, mm, tzinfo=JST)
    except Exception:
        return None

def _find_time_nearby(el: Tag) -> Tuple[Optional[str], str]:
    """行やカード要素から、近傍の <time> / data-* / HH:MM テキストを探す。戻り: (HH:MM, reason)"""
    # 1) <time datetime="YYYY-MM-DDTHH:MM"> / <time data-time="HH:MM">
    t = el.find("time")
    if t:
        # datetime 属性優先
        for attr in ("datetime", "data-time"):
            v = t.get(attr) or ""
            m = TIME_RE.search(v)
            if m:
                return m.group(0), f"time@{attr}"
        # 要素内テキスト
        m = TIME_RE.search(t.get_text(" ", strip=True))
        if m:
            return m.group(0), "time@text"

    # 2) data-starttime / data-start-time / data-time を持つ要素
    for node in el.find_all(True, recursive=True):
        for attr in ("data-starttime", "data-start-time", "data-time"):
            v = node.get(attr)
            if not v:
                continue
            m = TIME_RE.search(v)
            if m:
                return m.group(0), f"data:{attr}"

    # 3) よくあるクラス名
    for sel in [".startTime", ".cellStartTime", ".raceTime", ".time", ".start-time"]:
        node = el.select_one(sel)
        if node:
            m = TIME_RE.search(node.get_text(" ", strip=True))
            if m:
                return m.group(0), f"sel:{sel}"

    # 4) 最後の手段: 要素全体テキストから HH:MM
    m = TIME_RE.search(el.get_text(" ", strip=True))
    if m:
        return m.group(0), "row:text"

    return None, "-"

def parse_post_times_from_table_like(root: Tag) -> Dict[str, datetime]:
    """table行 or カード風ブロックから RACEID と 発走HH:MM を拾う万能パーサ"""
    post_map: Dict[str, datetime] = {}

    # 1) テーブル（theadに“発走/発走時刻/レース”が含まれるものを優先）
    for table in root.find_all("table"):
        thead = table.find("thead")
        if thead:
            head_text = "".join(thead.stripped_strings)
            if not any(k in head_text for k in ("発走", "発走時刻", "レース")):
                continue
        body = table.find("tbody") or table
        for tr in body.find_all("tr"):
            # RID
            rid = None
            link = tr.find("a", href=True)
            if link:
                m = RACEID_RE.search(link["href"])
                if m:
                    rid = m.group(1)
            if not rid or PLACEHOLDER.search(rid):
                continue

            # 時刻
            hhmm, reason = _find_time_nearby(tr)
            if not hhmm:
                logging.debug(f"[DEBUG] 発走見つからず(table row) rid={rid} row='{_row_text_snippet(tr)}'")
                continue
            m = TIME_RE.search(hhmm)
            if not m:
                continue
            hh, mm = int(m.group(1)), int(m.group(2))
            dt = _make_dt_from_hhmm(rid, hh, mm)
            if dt:
                post_map[rid] = dt
                logging.debug(f"[DEBUG] 発走抽出 OK rid={rid} {dt:%H:%M} via {reason}")

    # 2) テーブル以外（カード型リストなど）。/RACEID/リンクを基点に親要素から探す
    for a in root.find_all("a", href=True):
        m = RACEID_RE.search(a["href"])
        if not m:
            continue
        rid = m.group(1)
        if PLACEHOLDER.search(rid) or rid in post_map:
            continue

        # 近い先祖（tr / li / div.card 相当）を辿る
        host = None
        for parent in a.parents:
            if isinstance(parent, Tag) and parent.name in ("tr", "li", "div", "section", "article"):
                host = parent
                break
        host = host or a

        hhmm, reason = _find_time_nearby(host)
        if not hhmm:
            # a自身の兄弟方向も軽く見る
            sib_text = " ".join([x.get_text(" ", strip=True) for x in a.find_all_next(limit=3) if isinstance(x, Tag)])
            m2 = TIME_RE.search(sib_text)
            if m2:
                hhmm, reason = m2.group(0), "next:text"
        if not hhmm:
            continue

        m = TIME_RE.search(hhmm)
        if not m:
            continue
        hh, mm = int(m.group(1)), int(m.group(2))
        dt = _make_dt_from_hhmm(rid, hh, mm)
        if dt:
            post_map[rid] = dt
            logging.debug(f"[DEBUG] 発走抽出 OK rid={rid} {dt:%H:%M} via {reason} (card)")

    return post_map

def collect_post_time_map(ymd: str, ymd_next: str) -> Dict[str, datetime]:
    """#todaysTicket と 出馬表一覧（当日/翌日）の両方から発走時刻マップを構築"""
    post_map: Dict[str, datetime] = {}

    def _merge_from(url: str, label: str):
        try:
            soup = BeautifulSoup(fetch(url), "lxml")
            got = parse_post_times_from_table_like(soup)
            if got:
                post_map.update(got)
        except Exception as e:
            logging.warning(f"[WARN] {label} 読み込み失敗: {e} ({url})")

    # 本日の発売情報（同URLに #todaysTicket があることが多い）
    url_today = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    _merge_from(url_today, "#today")

    # 出馬表一覧（当日/翌日）
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

def _find_popular_odds_table(soup: BeautifulSoup) -> Tuple[Optional[BeautifulSoup], Dict[str, int]]:
    for table in soup.find_all("table"):
        thead = table.find("thead")
        if not thead:
            continue
        ths = thead.find_all(["th", "td"])
        headers = [_clean(th.get_text()) for th in ths]
        if not headers:
            continue
        pop_idx = None
        for i, h in enumerate(headers):
            if h in ("人気", "順位") or ("人気" in h and "順" not in h):
                pop_idx = i; break
        win_candidates = []
        for i, h in enumerate(headers):
            if ("複" in h) or ("率" in h) or ("%" in h): continue
            if h == "単勝": win_candidates.append((0, i))
            elif "単勝" in h: win_candidates.append((1, i))
            elif "オッズ" in h: win_candidates.append((2, i))
        win_idx = sorted(win_candidates, key=lambda x: x[0])[0][1] if win_candidates else None
        if pop_idx is None or win_idx is None:
            continue
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
            logging.info(f"[DEBUG] headers={headers} / pop_idx={pop_idx} / win_idx={win_idx} / win_samples={sample}")
            return table, {"pop": pop_idx, "win": win_idx}
    return None, {}

def parse_odds_table(soup: BeautifulSoup) -> Tuple[List[Dict[str, float]], Optional[str], Optional[str]]:
    venue_race = (soup.find("h1").get_text(strip=True) if soup.find("h1") else None)
    nowtime = soup.select_one(".withUpdate .nowTime") or soup.select_one(".nowTime")
    now_label = nowtime.get_text(strip=True) if nowtime else None

    table, idx = _find_popular_odds_table(soup)
    if not table:
        return [], venue_race, now_label

    pop_idx = idx["pop"]; win_idx = idx["win"]
    horses: List[Dict[str, float]] = []
    body = table.find("tbody") or table
    for tr in body.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if len(tds) <= max(pop_idx, win_idx): continue
        pop_txt = tds[pop_idx].get_text(strip=True)
        if not pop_txt.isdigit(): continue
        pop = int(pop_txt)
        if not (1 <= pop <= 30): continue
        odds = _as_float(tds[win_idx].get_text(" ", strip=True))
        if odds is None: continue
        horses.append({"pop": pop, "odds": float(odds)})

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

# ========= LINE送信 =========
def push_line_text(user_id: str, token: str, text: str, timeout=8, retries=1) -> Tuple[bool, Optional[int], str]:
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

def notify_strategy_hit(message_text: str) -> Tuple[bool, Optional[int]]:
    """戻り: (ok, http_status)。okは200のみTrue。429などはFalseで返る"""
    if not NOTIFY_ENABLED:
        logging.info("[INFO] NOTIFY_ENABLED=0 のため通知スキップ"); return False, None
    if DRY_RUN:
        logging.info("[DRY_RUN] 通知メッセージ:\n%s", message_text); return False, None
    if not LINE_ACCESS_TOKEN or not LINE_USER_ID:
        logging.error("[ERROR] LINE 環境変数不足（LINE_ACCESS_TOKEN/LINE_USER_ID）"); return False, None
    ok, status, body = push_line_text(LINE_USER_ID, LINE_ACCESS_TOKEN, message_text)
    if not ok:
        logging.warning("[WARN] LINE送信失敗 status=%s body=%s", status, (body or "")[:200])
    return ok, status

# ========= RACEID 取得 =========
def list_raceids_today_ticket(ymd: str) -> List[str]:
    url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    soup = BeautifulSoup(fetch(url), "lxml")
    ids = _extract_raceids_from_soup(soup)

    # #todaysTicket 限定にすると漏れやすいので、ページ全体から抽出しつつ重複排除
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
    """発走時刻を基準に、[ -WINDOW_BEFORE_MIN, WINDOW_AFTER_MIN ] に入っているか"""
    if CUTOFF_OFFSET_MIN > 0 and now >= (post_time - timedelta(minutes=CUTOFF_OFFSET_MIN)):
        return False
    win_start = post_time - timedelta(minutes=WINDOW_BEFORE_MIN)
    win_end   = post_time + timedelta(minutes=WINDOW_AFTER_MIN)
    return (win_start <= now <= win_end)

# ========= メイン =========
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if KILL_SWITCH:
        logging.info("[INFO] KILL_SWITCH=True のため終了"); return
    if not within_operating_hours():
        logging.info(f"[INFO] 監視休止（JST={now_jst():%H:%M} 稼働={START_HOUR:02d}:00-{END_HOUR:02d}:00）"); return

    logging.info("[INFO] ジョブ開始")
    logging.info(f"[INFO] DRY_RUN={DRY_RUN} NOTIFY_ENABLED={'1' if NOTIFY_ENABLED else '0'} "
                 f"TTL={NOTIFY_TTL_SEC}s CD={NOTIFY_COOLDOWN_SEC}s WIN=-{WINDOW_BEFORE_MIN}m/{WINDOW_AFTER_MIN:+}m "
                 f"CUTOFF={CUTOFF_OFFSET_MIN}m")

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
        post_time_map = {}
    else:
        ymd = now_jst().strftime("%Y%m%d")
        ymd_next = (now_jst() + timedelta(days=1)).strftime("%Y%m%d")
        r1 = list_raceids_today_ticket(ymd)
        r2 = list_raceids_from_card_lists(ymd, ymd_next)
        target_raceids = sorted(set(r1) | set(r2))
        # 発走時刻マップを構築（一覧のみ）
        post_time_map = collect_post_time_map(ymd, ymd_next)
        valid = [rid for rid in target_raceids if not PLACEHOLDER.search(rid)]
        logging.info(f"[INFO] 発見RACEID数(有効のみ): {len(valid)}")
        for rid in valid:
            logging.info(f"  - {rid} -> tanfuku")
        target_raceids = valid

    hits = 0; matches = 0
    seen_in_this_run: Set[str] = set()  # 同一ジョブ内の二重送信防止

    for rid in target_raceids:
        if rid in seen_in_this_run:
            logging.info(f"[SKIP] 同一ジョブ内去重: {rid}"); continue
        if should_skip_by_ttl(notified, rid):
            logging.info(f"[SKIP] TTL/クールダウン抑制: {rid}"); continue

        # 発走時刻の取得（一覧からのみ）
        post_time = post_time_map.get(rid)
        if not post_time:
            logging.info(f"[SKIP] 発走時刻不明のため通知保留: {rid}")
            continue

        # ウィンドウ判定
        now = now_jst()
        if not is_within_window(post_time, now):
            delta_min = int((post_time - now).total_seconds() // 60)
            logging.info(f"[SKIP] 窓外({delta_min:+}m) rid={rid} 発走={post_time:%H:%M}")
            continue

        # 窓内に入った場合のみ、オッズページを見に行く
        meta = check_tanfuku_page(rid)
        if not meta:
            time.sleep(random.uniform(*SLEEP_BETWEEN)); continue

        horses = meta["horses"]
        if len(horses) < 4:
            logging.info(f"[NO MATCH] {rid} 条件詳細: horses<4 で判定不可")
            time.sleep(random.uniform(*SLEEP_BETWEEN)); continue

        try:
            odds_log = ", ".join([f"{h['pop']}番人気:{h['odds']}" for h in sorted(horses, key=lambda x: x['pop'])])
        except Exception:
            odds_log = str(horses)
        logging.info(f"[DEBUG] {rid} 取得オッズ: {odds_log}")

        hits += 1
        strategy = eval_strategy(horses, logger=logging)
        if strategy:
            matches += 1
            ticket_str = ", ".join(strategy["tickets"])
            detail = f"{strategy['strategy']} / 買い目: {ticket_str} / {strategy['roi']} / {strategy['hit']}"
            logging.info(f"[MATCH] {rid} 条件詳細: {detail}")

            message = (
                "【戦略ヒット】\n"
                f"RACEID: {rid}\n"
                f"{meta['venue_race']} 発走{post_time:%H:%M} JST（{meta['now']}）\n"
                f"{strategy['strategy']}\n"
                f"買い目: {ticket_str}\n"
                f"{strategy['roi']} / {strategy['hit']}\n"
                f"{meta['url']}"
            )

            sent_ok, http_status = notify_strategy_hit(message)
            now_ts = time.time()

            if sent_ok:
                try:
                    sheet_upsert_notified(rid, now_ts, note=f"{meta['venue_race']} {post_time:%H:%M}")
                    notified[rid] = now_ts
                except Exception as e:
                    logging.exception("[ERROR] TTL更新失敗（Google Sheets）: %s", e)
                seen_in_this_run.add(rid)
            elif http_status == 429:
                try:
                    key_cd = f"{rid}:cd"
                    sheet_upsert_notified(key_cd, now_ts, note=f"429 cooldown {meta['venue_race']} {post_time:%H:%M}")
                    notified[key_cd] = now_ts
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