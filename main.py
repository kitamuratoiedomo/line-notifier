# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視・通知バッチ（人気順テーブル 厳密パース・支持率除外版）
- 門限（JST 10:00〜22:00）
- RACEID列挙：#todaysTicket / 出馬表一覧
- 人気順テーブルの見出しから “人気” 列と “単勝” 列を厳密特定（支持率/複勝は除外）
- 取得オッズをDEBUGログ出力（見出しログも追加）
- TTLで重複通知抑制
"""

import os, re, json, time, random, logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from strategy_rules import eval_strategy  # horses=[{"pop":1,"odds":1.8}, ...]

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

# ========= 環境変数 =========
NOTIFIED_PATH  = os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json")
DRY_RUN        = os.getenv("DRY_RUN", "False").lower() == "true"
KILL_SWITCH    = os.getenv("KILL_SWITCH", "False").lower() == "true"
NOTIFY_ENABLED = os.getenv("NOTIFY_ENABLED", "1") == "1"
DEBUG_RACEIDS  = [s.strip() for s in os.getenv("DEBUG_RACEIDS", "").split(",") if s.strip()]
NOTIFY_TTL_SEC = int(os.getenv("NOTIFY_TTL_SEC", "1800"))
START_HOUR     = int(os.getenv("START_HOUR", "10"))
END_HOUR       = int(os.getenv("END_HOUR",   "22"))

RACEID_RE = re.compile(r"/RACEID/(\d{18})")

# ========= ユーティリティ =========
def now_jst() -> datetime:
    return datetime.now(JST)

def within_operating_hours() -> bool:
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

def load_notified() -> Dict[str, float]:
    try:
        with open(NOTIFIED_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_notified(d: Dict[str, float]) -> None:
    os.makedirs(os.path.dirname(NOTIFIED_PATH), exist_ok=True)
    with open(NOTIFIED_PATH, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)

def should_skip_by_ttl(notified: Dict[str, float], rid: str) -> bool:
    ts = notified.get(rid)
    if not ts:
        return False
    return (time.time() - ts) < NOTIFY_TTL_SEC

# ========= RACEID 取得 =========
def list_raceids_today_ticket(ymd: str) -> List[str]:
    url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")
    table = soup.find(id="todaysTicket")
    if not table:
        logging.info("[INFO] #todaysTicket なし")
        return []
    links = table.select("td.nextRace a[href], td a[href]")
    raceids: List[str] = []
    for a in links:
        text = (a.get_text(strip=True) or "")
        m = RACEID_RE.search(a.get("href", ""))
        if not m:
            continue
        rid = m.group(1)
        if "投票受付中" in text or "発走" in text:
            raceids.append(rid)
    raceids = sorted(set(raceids))
    logging.info(f"[INFO] Rakuten#1 本日の発売情報: {len(raceids)}件")
    return raceids

def list_raceids_from_card_lists(ymd: str, ymd_next: str) -> List[str]:
    urls = [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000",
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd_next}0000000000",
    ]
    rids: List[str] = []
    for u in urls:
        try:
            html = fetch(u)
            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                m = RACEID_RE.search(a["href"])
                if m:
                    rids.append(m.group(1))
        except Exception as e:
            logging.warning(f"[WARN] 出馬表一覧スキャン失敗: {e} ({u})")
    rids = sorted(set(rids))
    logging.info(f"[INFO] Rakuten#2 出馬表一覧: {len(rids)}件")
    return rids

# ========= オッズ解析（人気順テーブルを厳密特定） =========
def _clean(s: str) -> str:
    return re.sub(r"\s+", "", s or "")

def _as_float(text: str) -> Optional[float]:
    """数値だけ抽出。% を含む文字列は None を返す（支持率除外）。"""
    if not text or "%" in text:
        return None
    m = re.search(r"\d+(?:\.\d+)?", (text or "").replace(",", ""))
    return float(m.group(0)) if m else None

def _find_popular_odds_table(soup: BeautifulSoup) -> Tuple[Optional[BeautifulSoup], Dict[str, int]]:
    """
    見出し(TH)から “人気” 列と “単勝” 列のインデックスを特定。
    戻り値: (table, {"pop": idx_pop, "win": idx_win})
    """
    for table in soup.find_all("table"):
        thead = table.find("thead")
        if not thead:
            continue
        ths = thead.find_all(["th", "td"])
        headers = [_clean(th.get_text()) for th in ths]
        if not headers:
            continue

        # DEBUG: 見出しをログ
        logging.debug(f"[DEBUG] table headers: {headers}")

        # 人気列
        pop_idx = None
        for i, h in enumerate(headers):
            if h == "人気" or ("人気" in h and "順" not in h):
                pop_idx = i
                break

        # 単勝列（厳密に優先度を付けて選ぶ）
        win_candidates = []  # (priority, index)
        for i, h in enumerate(headers):
            if "複" in h:
                continue  # 複勝/複の列は除外
            if "率" in h or "%" in h:
                continue  # 支持率/％は除外
            if h == "単勝":
                win_candidates.append((0, i))  # 最優先：完全一致
            elif "単勝" in h:
                win_candidates.append((1, i))
            elif "オッズ" in h:
                win_candidates.append((2, i))  # 代替：「オッズ」表記

        win_idx = None
        if win_candidates:
            win_idx = sorted(win_candidates, key=lambda x: x[0])[0][1]

        if pop_idx is None or win_idx is None:
            continue

        # 本文で人気が 1,2,3… と昇順になっているか軽く検証
        body = table.find("tbody") or table
        rows = body.find_all("tr")
        ok_rows = 0
        last = 0
        for tr in rows[:8]:
            tds = tr.find_all(["td", "th"])
            if len(tds) <= max(pop_idx, win_idx):
                continue
            s = tds[pop_idx].get_text(strip=True)
            if not s.isdigit():
                break
            val = int(s)
            if val <= last:
                break
            last = val
            ok_rows += 1
        if ok_rows >= 2:
            return table, {"pop": pop_idx, "win": win_idx}

    return None, {}

def parse_odds_table(soup: BeautifulSoup) -> Tuple[List[Dict[str, float]], Optional[str], Optional[str]]:
    venue_race = (soup.find("h1").get_text(strip=True) if soup.find("h1") else None)
    nowtime = soup.select_one(".withUpdate .nowTime") or soup.select_one(".nowTime")
    now_label = nowtime.get_text(strip=True) if nowtime else None

    table, idx = _find_popular_odds_table(soup)
    if not table:
        return [], venue_race, now_label

    pop_idx = idx["pop"]
    win_idx = idx["win"]

    horses: List[Dict[str, float]] = []
    body = table.find("tbody") or table
    for tr in body.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if len(tds) <= max(pop_idx, win_idx):
            continue

        pop_txt = tds[pop_idx].get_text(strip=True)
        if not pop_txt.isdigit():
            continue
        pop = int(pop_txt)
        if not (1 <= pop <= 30):
            continue

        win_txt = tds[win_idx].get_text(" ", strip=True)
        odds = _as_float(win_txt)
        if odds is None:
            continue  # 支持率や空欄などは除外

        horses.append({"pop": pop, "odds": float(odds)})

    # 人気でユニーク化＆昇順
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
        logging.warning(f"[WARN] オッズテーブル未検出: {url}")
        return None
    if not venue_race:
        venue_race = "地方競馬"
    return {"race_id": race_id, "url": url, "horses": horses,
            "venue_race": venue_race, "now": now_label or ""}

# ========= 通知 =========
def send_notification(msg: str) -> None:
    logging.info(f"[NOTIFY] {msg}")

# ========= メイン =========
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if KILL_SWITCH:
        logging.info("[INFO] KILL_SWITCH=True のため終了"); return
    if not within_operating_hours():
        logging.info(f"[INFO] 監視休止（JST={now_jst():%H:%M} 稼働={START_HOUR:02d}:00-{END_HOUR:02d}:00）"); return

    logging.info("[INFO] ジョブ開始")
    logging.info(f"[INFO] NOTIFIED_PATH={NOTIFIED_PATH} KILL_SWITCH={KILL_SWITCH} DRY_RUN={DRY_RUN}")
    logging.info(f"[INFO] NOTIFY_ENABLED={'1' if NOTIFY_ENABLED else '0'}")

    notified = load_notified()
    hits = 0; matches = 0

    if DEBUG_RACEIDS:
        logging.info(f"[INFO] DEBUG_RACEIDS 指定: {len(DEBUG_RACEIDS)}件")
        target_raceids = DEBUG_RACEIDS
    else:
        ymd = now_jst().strftime("%Y%m%d")
        ymd_next = (now_jst() + timedelta(days=1)).strftime("%Y%m%d")
        r1 = list_raceids_today_ticket(ymd)
        r2 = list_raceids_from_card_lists(ymd, ymd_next)
        target_raceids = sorted(set(r1) | set(r2))
        logging.info(f"[INFO] 発見RACEID数: {len(target_raceids)}")
        for rid in target_raceids:
            logging.info(f"  - {rid} -> tanfuku")

    for rid in target_raceids:
        if should_skip_by_ttl(notified, rid):
            logging.info(f"[SKIP] TTL抑制: {rid}"); continue

        meta = check_tanfuku_page(rid)
        if not meta: continue

        horses = meta["horses"]
        if len(horses) < 4:
            logging.info(f"[NO MATCH] {rid} 条件詳細: horses<4 で判定不可"); continue

        # 取得オッズの可視化
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

            if NOTIFY_ENABLED and not DRY_RUN:
                msg = (f"【戦略ヒット】\n"
                       f"RACEID: {rid}\n"
                       f"{meta['venue_race']} {meta['now']}\n"
                       f"{strategy['strategy']}\n"
                       f"買い目: {ticket_str}\n"
                       f"{strategy['roi']} / {strategy['hit']}\n"
                       f"{meta['url']}")
                send_notification(msg)
            else:
                logging.info("[DRY_RUN] 通知はスキップ")
            notified[rid] = time.time()
        else:
            logging.info(f"[NO MATCH] {rid} 条件詳細: パターン①〜④に非該当")

        time.sleep(random.uniform(*SLEEP_BETWEEN))

    logging.info(f"[INFO] HITS={hits} / MATCHES={matches}")
    save_notified(notified)
    logging.info(f"[INFO] notified saved: {NOTIFIED_PATH} (bytes={len(json.dumps(notified, ensure_ascii=False))})")
    logging.info("[INFO] ジョブ終了")

if __name__ == "__main__":
    main()