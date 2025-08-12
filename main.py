# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視・通知バッチ（発走時刻=一覧ページから取得 / 窓内1回通知 / 429クールダウン / Sheet永続TTL）
- 稼働時間: JST 10:00〜22:00（環境変数 START_HOUR / END_HOUR）
- RACEID列挙: #todaysTicket + 出馬表一覧（当日/翌日）
- 発走時刻: 上記一覧ページのテーブルから抽出（race_card/RACEID/* にはアクセスしない）
- 人気順テーブル: “人気/順位”列と“単勝/オッズ”列のみ厳密抽出（%・複勝レンジ除外）
- 通知ウィンドウ: 発走 { -WINDOW_BEFORE_MIN 分 〜 WINDOW_AFTER_MIN 分 } に入った時のみ判定・送信
- 通知は LINE 200 OK の時だけTTL更新（RACEIDキー）。429時は RACEID:cd キーにクールダウンTSを記録して抑止。
"""

import os, re, json, time, random, logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Set

import requests
from bs4 import BeautifulSoup
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

# 通常TTL（送達成功時のみ有効化）
NOTIFY_TTL_SEC      = int(os.getenv("NOTIFY_TTL_SEC", "3600"))
# 429クールダウンTTL（429時に RACEID:cd キーに記録）
NOTIFY_COOLDOWN_SEC = int(os.getenv("NOTIFY_COOLDOWN_SEC", "1800"))

# 通知ウィンドウ（例: BEFORE=15, AFTER=-10 → 発走15分前〜10分前の“間”に1回）
WINDOW_BEFORE_MIN   = int(os.getenv("WINDOW_BEFORE_MIN", "15"))
WINDOW_AFTER_MIN    = int(os.getenv("WINDOW_AFTER_MIN", "-10"))

# 任意：競走直前のデータ変化を考慮して直近数分は送らない等（0なら無効）
CUTOFF_OFFSET_MIN   = int(os.getenv("CUTOFF_OFFSET_MIN", "0"))
FORCE_RUN           = os.getenv("FORCE_RUN", "0") == "1"

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "")
LINE_USER_ID      = os.getenv("LINE_USER_ID", "")

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_SHEET_ID         = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_SHEET_TAB        = os.getenv("GOOGLE_SHEET_TAB", "notified")  # シート名 or gid(数値)

RACEID_RE = re.compile(r"/RACEID/(\d{18})")
# 全角コロンにも一応対応（保険）
TIME_RE   = re.compile(r"(\d{1,2})[:：](\d{2})")

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

# ========= RACEIDバリデーション =========
def is_valid_raceid(rid: str) -> bool:
    """個別レースIDのみTrue。一覧用プレースホルダ（…0000000000）は除外。"""
    if not rid or len(rid) != 18 or not rid.isdigit():
        return False
    if rid.endswith("0000000000"):
        return False
    return True

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

# ========= 発走時刻（一覧ページ） =========
def parse_post_times_from_table(table: BeautifulSoup) -> Dict[str, datetime]:
    """テーブルの各行から RACEID と 発走時刻(HH:MM) を拾って JST datetime を返す"""
    post_map: Dict[str, datetime] = {}
    thead = table.find("thead")
    ths = (thead.find_all(["th", "td"]) if thead else [])
    headers = ["".join(th.stripped_strings) for th in ths] if ths else []
    # 発走時刻の列インデックスを推定（ヘッダ優先、無ければパターンで抽出）
    post_col = None
    for i, h in enumerate(headers):
        if "発走時刻" in h:
            post_col = i
            break

    body = table.find("tbody") or table
    for tr in body.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        # RACEID探索（行内のリンクから）
        rid = None
        for a in tr.find_all("a", href=True):
            m = RACEID_RE.search(a["href"])
            if m:
                candidate = m.group(1)
                if is_valid_raceid(candidate):
                    rid = candidate
                    break
        if not rid:
            continue

        # 時刻テキスト抽出
        time_text = None
        if post_col is not None and len(tds) > post_col:
            time_text = "".join(tds[post_col].stripped_strings)
        if not time_text or not TIME_RE.search(time_text):
            # セルに見当たらない場合は行全体から検索
            row_text = " ".join([" ".join(td.stripped_strings) for td in tds])
            m = TIME_RE.search(row_text)
            time_text = m.group(0) if m else None
        if not time_text:
            continue

        m = TIME_RE.search(time_text)
        if not m:
            continue
        hh, mm = int(m.group(1)), int(m.group(2))
        # 日付は RACEID のYYYYMMDD先頭8桁を使う
        y = int(rid[0:4]); mon = int(rid[4:6]); d = int(rid[6:8])
        try:
            dt = datetime(y, mon, d, hh, mm, tzinfo=JST)
        except Exception:
            continue
        post_map[rid] = dt
    return post_map

def collect_post_time_map(ymd: str, ymd_next: str) -> Dict[str, datetime]:
    """#todaysTicket と 出馬表一覧（当日/翌日）の両方から発走時刻マップを構築"""
    post_map: Dict[str, datetime] = {}
    # 本日の発売情報
    try:
        url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
        soup = BeautifulSoup(fetch(url), "lxml")
        ticket = soup.find(id="todaysTicket")
        if ticket:
            post_map.update(parse_post_times_from_table(ticket))
    except Exception as e:
        logging.warning(f"[WARN] #todaysTicket 読み込み失敗: {e}")

    # 出馬表一覧（当日/翌日）
    for u in [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000",
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd_next}0000000000",
    ]:
        try:
            soup = BeautifulSoup(fetch(u), "lxml")
            for table in soup.find_all("table"):
                # ヘッダに「発走時刻」があるテーブルを対象
                thead = table.find("thead")
                if not thead:
                    continue
                head_text = "".join(thead.stripped_strings)
                if "発走" in head_text or "発走時刻" in head_text or "レース" in head_text:
                    post_map.update(parse_post_times_from_table(table))
        except Exception as e:
            logging.warning(f"[WARN] 出馬表一覧 読み込み失敗: {e} ({u})")

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
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")
    table = soup.find(id="todaysTicket")
    if not table:
        logging.info("[INFO] #todaysTicket なし"); return []
    links = table.select("td.nextRace a[href], td a[href]")
    raceids = sorted({
        m.group(1) for a in links
        for m in [RACEID_RE.search(a.get("href", ""))]
        if m and is_valid_raceid(m.group(1))
    })
    logging.info(f"[INFO] Rakuten#1 本日の発売情報: {len(raceids)}件")
    return raceids

def list_raceids_from_card_lists(ymd: str, ymd_next: str) -> List[str]:
    urls = [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000",
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd_next}0000000000",
    ]
    rids: Set[str] = set()
    for u in urls:
        try:
            html = fetch(u)
            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                m = RACEID_RE.search(a["href"])
                if m:
                    rid = m.group(1)
                    if is_valid_raceid(rid):
                        rids.add(rid)
        except Exception as e:
            logging.warning(f"[WARN] 出馬表一覧スキャン失敗: {e} ({u})")
    rids = sorted(rids)
    logging.info(f"[INFO] Rakuten#2 出馬表一覧: {len(rids)}件")
    return rids

# ========= ウィンドウ判定 =========
def is_within_window(post_time: datetime, now: datetime) -> bool:
    """発走時刻を基準に、[ -WINDOW_BEFORE_MIN, WINDOW_AFTER_MIN ] に入っているか"""
    # 直前カット（例: CUTOFF_OFFSET_MIN=3 → 発走-3分以降は送らない）
    if CUTOFF_OFFSET_MIN > 0 and now >= (post_time - timedelta(minutes=CUTOFF_OFFSET_MIN)):
        return False
    win_start = post_time - timedelta(minutes=WINDOW_BEFORE_MIN)
    win_end   = post_time + timedelta(minutes=WINDOW_AFTER_MIN)
    # AFTER_MIN が負なら “発走前のみ” の窓になる（例: -10）
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
        target_raceids = [rid for rid in DEBUG_RACEIDS if is_valid_raceid(rid)]
        post_time_map = {}
    else:
        ymd = now_jst().strftime("%Y%m%d")
        ymd_next = (now_jst() + timedelta(days=1)).strftime("%Y%m%d")
        r1 = list_raceids_today_ticket(ymd)
        r2 = list_raceids_from_card_lists(ymd, ymd_next)
        # 有効なRACEIDのみに限定
        target_raceids = [rid for rid in sorted(set(r1) | set(r2)) if is_valid_raceid(rid)]
        # 発走時刻マップを構築（詳細ページは叩かない）
        post_time_map = collect_post_time_map(ymd, ymd_next)
        logging.info(f"[INFO] 発見RACEID数(有効のみ): {len(target_raceids)}")
        for rid in target_raceids:
            logging.info(f"  - {rid} -> tanfuku")

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
            # デバッグしやすいよう距離も出す
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
                # 送達成功 → 通常TTL更新
                try:
                    sheet_upsert_notified(rid, now_ts, note=f"{meta['venue_race']} {post_time:%H:%M}")
                    notified[rid] = now_ts
                except Exception as e:
                    logging.exception("[ERROR] TTL更新失敗（Google Sheets）: %s", e)
                seen_in_this_run.add(rid)
            elif http_status == 429:
                # 429 → クールダウンTTL更新（rid:cd）
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