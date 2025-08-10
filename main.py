# -*- coding: utf-8 -*-
"""
Rakuten競馬：本日の出馬表(一覧)から RACEID を抽出し、
単勝/複勝オッズページへ疎通確認する“堅牢版オールインワン”。

変更点（重要）
- 「投票受付中/発走」の文言依存をやめ、#todaysTicket 配下の a[href]から
  RACEID を無条件回収（=取りこぼし防止）。
- #todaysTicket が無い/崩れている場合は全リンク走査にフォールバックし、RACEID を抽出。
- 何件拾えたか・どの段に失敗したかを段階ログで可視化。
- 既存の NOTIFIED_PATH / DRY_RUN / KILL_SWITCH を継承。MAX_CHECK でチェック数を制限可能。
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
})

TIMEOUT = (10, 20)  # (connect, read)
RETRY = 3
SLEEP_BETWEEN = (0.8, 1.6)

# 既存の環境変数(あれば使う)
NOTIFIED_PATH = os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json")
DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"
KILL_SWITCH = os.getenv("KILL_SWITCH", "False").lower() == "true"
MAX_CHECK = int(os.getenv("MAX_CHECK", "50"))  # 念のためチェック上限

# -------------------------
# ユーティリティ
# -------------------------
def get_today_str() -> str:
    """JSTで今日の YYYYMMDD を返す"""
    return datetime.now(JST).strftime("%Y%m%d")

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

# -------------------------
# 1) 今日の出馬表(一覧)から RACEID を集める
# -------------------------
RACEID_RE = re.compile(r"/RACEID/(\d{18})")

def list_today_raceids() -> List[str]:
    logging.info("### LIST v3 start ###")
    ymd = get_today_str()
    list_url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    logging.info(f"[INFO] fetch(list) => {list_url}")

    html = fetch(list_url)
    logging.info(f"[INFO] fetched html length={len(html)}")
    soup = BeautifulSoup(html, "lxml")

    raceids: List[str] = []

    # まず #todaysTicket を優先（ここに“今日の発売情報”が集約される）
    table = soup.find(id="todaysTicket")
    if table:
        logging.info("[INFO] #todaysTicket found")
        links = table.select("a[href]")
        logging.info(f"[INFO] links_in_todaysTicket={len(links)}")
        for a in links:
            href = a.get("href", "")
            m = RACEID_RE.search(href)
            if m:
                raceids.append(m.group(1))
    else:
        # 全リンク走査フォールバック
        logging.info("[INFO] #todaysTicket missing -> 全リンク走査へ")
        links = soup.find_all("a", href=True)
        logging.info(f"[INFO] links_all={len(links)}")
        for a in links:
            href = a["href"]
            m = RACEID_RE.search(href)
            if m:
                raceids.append(m.group(1))

    # 重複排除＆安定ソート
    raceids = sorted(set(raceids))
    logging.info(f"[INFO] RACEID collected={len(raceids)}")
    if raceids[:5]:
        logging.info(f"[INFO] sample: {raceids[:5]} ...")

    return raceids

# -------------------------
# 2) 各レースの単勝/複勝オッズページへアクセス（疎通確認）
# -------------------------
def check_tanfuku_page(race_id: str) -> Optional[Dict]:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    try:
        html = fetch(url)
    except Exception as e:
        logging.warning(f"[WARN] odds fetch失敗: {race_id} {e}")
        return None

    soup = BeautifulSoup(html, "lxml")

    # 見出しや時刻など、最低限のメタ
    h1 = soup.find("h1")
    nowtime = soup.select_one(".withUpdate .nowTime")
    race_title = h1.get_text(strip=True) if h1 else "N/A"
    now_label = nowtime.get_text(strip=True) if nowtime else ""

    # 単/複のテーブルがあるか（壊れてないか）
    odds_table = soup.find("table", {"summary": re.compile("オッズ")})
    if not odds_table:
        logging.warning(f"[WARN] 単勝/複勝テーブル見つからず: {url}")
        return None

    return {
        "race_id": race_id,
        "title": race_title,
        "now": now_label,
        "url": url,
    }

# -------------------------
# メイン
# -------------------------
def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if KILL_SWITCH:
        logging.info("[INFO] KILL_SWITCH=True のため、処理をスキップします。")
        return

    logging.info(f"[INFO] ジョブ開始 JST={datetime.now(JST):%Y-%m-%d %H:%M:%S}")
    logging.info(f"[INFO] NOTIFIED_PATH={NOTIFIED_PATH} KILL_SWITCH={KILL_SWITCH} DRY_RUN={DRY_RUN} MAX_CHECK={MAX_CHECK}")

    raceids = list_today_raceids()
    if not raceids:
        logging.info("[INFO] RACEID=0。ページ構造変更/メンテ/アクセス拒否の可能性。")
        logging.info("[INFO] HITS=0")
        save_notified({})
        logging.info("[INFO] ジョブ終了")
        return

    # ログ：一覧
    logging.info("[INFO] 発見RACEID一覧(最大10): " + ", ".join(raceids[:10]) + (" ..." if len(raceids) > 10 else ""))

    notified = load_notified()
    hits = 0

    # 上限チェック（念のため）
    target_ids = raceids[:MAX_CHECK]

    for rid in target_ids:
        meta = check_tanfuku_page(rid)
        if not meta:
            continue

        hits += 1
        logging.info(f"[INFO] OK: {meta['race_id']} {meta['title']} {meta['now']} {meta['url']}")

        # 記録（通知は各自の処理に接続）
        notified[rid] = time.time()

        # 実通知例（必要なら既存の送信関数へ）
        # if not DRY_RUN:
        #     send_line(meta)

        time.sleep(random.uniform(*SLEEP_BETWEEN))

    logging.info(f"[INFO] HITS={hits}")
    save_notified(notified)
    logging.info(f"[INFO] notified saved: {NOTIFIED_PATH} (bytes={len(json.dumps(notified, ensure_ascii=False))})")
    logging.info("[INFO] ジョブ終了")

if __name__ == "__main__":
    main()