# -*- coding: utf-8 -*-
"""
Rakuten競馬：本日の出馬表(一覧)から「投票受付中」のレース RACEID を抽出して
単勝/複勝オッズページへ到達する、強化・多段フォールバック版（詳細ログ付き）

特徴
- 公式 #todaysTicket > 「投票受付中」リンク優先
- 失敗時： (1) #todaysTicket 内の全 <a> 走査 → (2) ページ全体を正規表現でRACEID抽出
- さらに、拾えた RACEID は odds/tanfuku へ疎通チェック
- 取得HTMLを /tmp/rakuten_list.html に保存（事故調査用）
- 主要カウントと経路を INFO ログに明示
"""

import os
import re
import json
import time
import random
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

# -------------------------
# 設定
# -------------------------
JST = timezone(timedelta(hours=9))

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
})

TIMEOUT = (10, 20)  # (connect, read)
RETRY = 3
SLEEP_BETWEEN = (0.8, 1.4)

NOTIFIED_PATH = os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json")
DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"
KILL_SWITCH = os.getenv("KILL_SWITCH", "False").lower() == "true"

DEBUG_SAVE = True  # 取得HTMLを保存しておく

# -------------------------
# ユーティリティ
# -------------------------
def today_ymd() -> str:
    return datetime.now(JST).strftime("%Y%m%d")

def fetch(url: str) -> str:
    last = None
    for i in range(1, RETRY + 1):
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

# -------------------------
# 抽出ロジック
# -------------------------
RACEID_RE = re.compile(r"/RACEID/(\d{18})")
PURE_ID_RE = re.compile(r"\b(20\d{16})\b")  # ページ全体の保険用

def list_today_raceids() -> List[str]:
    ymd = today_ymd()
    url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    html = fetch(url)

    if DEBUG_SAVE:
        try:
            with open("/tmp/rakuten_list.html", "w", encoding="utf-8") as f:
                f.write(html)
            logging.info("[INFO] 取得HTMLを /tmp/rakuten_list.html に保存しました")
        except Exception as e:
            logging.warning(f"[WARN] HTML保存に失敗: {e}")

    soup = BeautifulSoup(html, "lxml")

    # 1) #todaysTicket 内の「投票受付中」リンクから
    base = soup.find(id="todaysTicket")
    ids1: List[str] = []
    if base:
        links = base.select("a[href]")
        for a in links:
            text = (a.get_text(strip=True) or "")
            href = a.get("href", "")
            m = RACEID_RE.search(href)
            if not m:
                continue
            if ("投票受付中" in text) or ("発走" in text):
                ids1.append(m.group(1))
        logging.info(f"[INFO] 経路#1: todaysTicketから抽出 {len(ids1)} 件")
    else:
        logging.info("[INFO] 経路#1: #todaysTicket が見つかりませんでした")

    # 2) #todaysTicket 内の全リンクから（文言に頼らない保険）
    ids2: List[str] = []
    if base:
        for a in base.select("a[href]"):
            href = a.get("href", "")
            m = RACEID_RE.search(href)
            if m:
                ids2.append(m.group(1))
        logging.info(f"[INFO] 経路#2: todaysTicket内の全リンクから抽出 {len(ids2)} 件")

    # 3) ページ全体の <a> を総なめ
    ids3: List[str] = []
    for a in soup.find_all("a", href=True):
        m = RACEID_RE.search(a["href"])
        if m:
            ids3.append(m.group(1))
    logging.info(f"[INFO] 経路#3: 全リンク総なめ抽出 {len(ids3)} 件")

    # 4) ページ全体テキストから正規表現で生ID抽出（最後の砦）
    ids4 = PURE_ID_RE.findall(html)
    logging.info(f"[INFO] 経路#4: 正規表現スキャン抽出 {len(ids4)} 件")

    # マージ（重複排除）
    merged = []
    seen = set()
    for bucket in [ids1, ids2, ids3, ids4]:
        for rid in bucket:
            if rid not in seen and len(rid) == 18 and rid.startswith("20"):
                seen.add(rid)
                merged.append(rid)

    logging.info(f"[INFO] 本日ページからのRACEIDユニーク合計: {len(merged)} 件")
    return sorted(merged)

def check_tanfuku_page(race_id: str) -> Optional[Dict]:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    try:
        html = fetch(url)
    except Exception as e:
        logging.warning(f"[WARN] tanfuku取得失敗 {race_id}: {e}")
        return None

    soup = BeautifulSoup(html, "lxml")
    h1 = soup.find("h1")
    nowtime = soup.select_one(".withUpdate .nowTime")
    odds_table = soup.find("table", {"summary": re.compile("オッズ")})
    if not odds_table:
        logging.warning(f"[WARN] オッズ表が見つからず: {url}")
        return None

    return {
        "race_id": race_id,
        "title": h1.get_text(strip=True) if h1 else "",
        "now": nowtime.get_text(strip=True) if nowtime else "",
        "url": url,
    }

# -------------------------
# メイン
# -------------------------
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    if KILL_SWITCH:
        logging.info("KILL_SWITCH=True のため、処理を停止します")
        return

    logging.info("ジョブ開始 JST=%s", datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"))
    logging.info("NOTIFIED_PATH=%s KILL_SWITCH=%s DRY_RUN=%s", NOTIFIED_PATH, KILL_SWITCH, DRY_RUN)

    raceids = list_today_raceids()
    if not raceids:
        logging.info("Rakutenスクレイピングで本日検出: 0 件")
        logging.info("対象レースIDがありません（開催なし or 取得失敗）")
        logging.info("HITS=0")
        save_notified({})
        logging.info("ジョブ終了")
        return

    logging.info("発見RACEID数: %d", len(raceids))
    for rid in raceids:
        logging.info("  - %s -> tanfuku", rid)

    notified = load_notified()
    hits = 0
    for rid in raceids:
        meta = check_tanfuku_page(rid)
        if not meta:
            continue
        hits += 1
        logging.info("[OK] %s %s %s %s", meta["race_id"], meta["title"], meta["now"], meta["url"])
        notified[rid] = time.time()
        if not DRY_RUN:
            time.sleep(random.uniform(*SLEEP_BETWEEN))

    logging.info("HITS=%d", hits)
    save_notified(notified if hits else {})
    logging.info("notified saved: %s (bytes=%d)", NOTIFIED_PATH, len(json.dumps(notified if hits else {}, ensure_ascii=False)))
    logging.info("ジョブ終了")

if __name__ == "__main__":
    main()