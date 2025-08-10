# -*- coding: utf-8 -*-
"""
Rakuten競馬：本日の「投票受付中」から RACEID を抽出 → 単勝/複勝オッズに到達する最小・堅牢版
- まずトップページ(PC/スマホ)の『本日の発売情報』を解析
- 取れなければ 出馬表一覧ページにフォールバック
- 取得した RACEID ごとに /odds/tanfuku/ へ疎通確認
- 既存の環境変数(KILL_SWITCH, DRY_RUN, NOTIFIED_PATH ほか)を尊重

ログ目安:
  [INFO] Rakuten#1 本日の発売情報: X件
  [INFO] Rakuten#2 出馬表一覧: X件
  [INFO] 発見RACEID数: N
  [INFO] OK: <race_id> <title> <now> <url>
"""

import os
import re
import json
import time
import random
import logging
from typing import List, Dict, Optional, Iterable
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

# ===================== 基本設定 =====================
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

TIMEOUT = (10, 20)     # (connect, read)
RETRY = 3
SLEEP_BETWEEN = (0.6, 1.2)

NOTIFIED_PATH = os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json")
DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"
KILL_SWITCH = os.getenv("KILL_SWITCH", "False").lower() == "true"

# 必要なら環境変数 RACEIDS="2025...,2025..." で上書きも可
ENV_RACEIDS = [x.strip() for x in os.getenv("RACEIDS", "").split(",") if x.strip()]

RACEID_RE = re.compile(r"/RACEID/(\d{18})")

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
            wt = random.uniform(*SLEEP_BETWEEN)
            logging.warning(f"[WARN] fetch失敗({i}/{RETRY}) {e} -> {wt:.1f}s待機: {url}")
            time.sleep(wt)
    raise last

def uniq_sorted(xs: Iterable[str]) -> List[str]:
    return sorted(set(xs))

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

# ===================== 1) 本日の発売情報（トップ） =====================
def scrape_from_top() -> List[str]:
    """
    keiba.rakuten.co.jp（PC/スマホ同一HTML）トップの『本日の発売情報』から
    「投票受付中」を含む a[href] の RACEID を抽出
    """
    html = fetch("https://keiba.rakuten.co.jp/")
    soup = BeautifulSoup(html, "lxml")

    # 『本日の発売情報』周辺を広めに探す（PC/スマホどちらでも拾えるように）
    candidates: List[str] = []
    for a in soup.find_all("a", href=True):
        text = (a.get_text(strip=True) or "")
        if not text:
            continue
        if ("投票受付中" in text) or ("発走" in text):
            m = RACEID_RE.search(a["href"])
            if m:
                candidates.append(m.group(1))
    logging.info(f"[INFO] Rakuten#1 本日の発売情報: {len(candidates)}件")
    return uniq_sorted(candidates)

# ===================== 2) 出馬表一覧フォールバック =====================
def scrape_from_list() -> List[str]:
    """
    出馬表一覧（本日）から保険抽出。
    """
    ymd = today_ymd()
    url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    # #todaysTicket があれば最優先でそこから
    raceids: List[str] = []
    box = soup.find(id="todaysTicket")
    links = box.select("a[href]") if box else soup.find_all("a", href=True)
    for a in links:
        text = (a.get_text(strip=True) or "")
        m = RACEID_RE.search(a["href"])
        if not m:
            continue
        if ("投票受付中" in text) or ("発走" in text) or ("レース一覧" in text):
            raceids.append(m.group(1))

    logging.info(f"[INFO] Rakuten#2 出馬表一覧: {len(raceids)}件")
    return uniq_sorted(raceids)

# ===================== 3) tanfuku ページ疎通 =====================
def check_tanfuku(race_id: str) -> Optional[Dict]:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    h1 = soup.find("h1")
    race_title = h1.get_text(strip=True) if h1 else "N/A"
    nowtime = soup.select_one(".withUpdate .nowTime")
    now_label = nowtime.get_text(strip=True) if nowtime else ""

    # 単勝/複勝テーブル有無を緩く判定
    odds_table = soup.find("table")
    if not odds_table:
        logging.warning(f"[WARN] オッズ表が見つからない可能性: {url}")
        # 表示が重い/遅延のケースもあるので、None は返さずメタだけ返す運用でもよい
        # ここでは疎通OKとして扱う
    return {
        "race_id": race_id,
        "title": race_title,
        "now": now_label,
        "url": url,
    }

# ===================== メイン =====================
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    if KILL_SWITCH:
        logging.info("[INFO] KILL_SWITCH=True のため終了")
        return

    logging.info(f"[INFO] ジョブ開始 host={os.uname().nodename} pid={os.getpid()}")
    logging.info(f"[INFO] NOTIFIED_PATH={NOTIFIED_PATH} KILL_SWITCH={KILL_SWITCH} DRY_RUN={DRY_RUN}")

    # 1) env 指定があればそれを最優先
    raceids: List[str] = []
    if ENV_RACEIDS:
        raceids = uniq_sorted(ENV_RACEIDS)
        logging.info(f"[INFO] ENV RACEIDS 使用: {len(raceids)}件")
    else:
        # 2) トップ → 3) フォールバック
        r1 = scrape_from_top()
        r2 = scrape_from_list() if not r1 else []
        raceids = uniq_sorted([*r1, *r2])

    if not raceids:
        logging.info("[INFO] 本日分の対象RACEIDなし（開催なし or 取得失敗）")
        logging.info("[INFO] HITS=0")
        save_notified({})
        logging.info("[INFO] ジョブ終了")
        return

    logging.info(f"[INFO] 発見RACEID数: {len(raceids)}")
    for rid in raceids:
        logging.info(f"  - {rid} -> tanfuku")

    notified = load_notified()
    hits = 0

    for rid in raceids:
        meta = check_tanfuku(rid)
        if not meta:
            continue
        hits += 1
        logging.info(f"[INFO] OK: {meta['race_id']} {meta['title']} {meta['now']} {meta['url']}")
        notified[rid] = time.time()
        time.sleep(random.uniform(*SLEEP_BETWEEN))

    logging.info(f"[INFO] HITS={hits}")
    save_notified(notified)
    logging.info(f"[INFO] notified saved: {NOTIFIED_PATH} (bytes={len(json.dumps(notified, ensure_ascii=False))})")
    logging.info("[INFO] ジョブ終了")

if __name__ == "__main__":
    main()