# -*- coding: utf-8 -*-
"""
Rakuten競馬：本日の出馬表(一覧)から「投票受付中」のレースを抽出し、
RACEID を拾って単勝/複勝オッズページにアクセスする最小・堅牢版 v3.

・#todaysTicket（本日の発売情報）を最優先で解析
・"投票受付中" を含む <a> の href から /RACEID/18桁 を正規表現で抽出
・見つからなくても複数段フォールバック（全リンク走査・レース一覧テーブルなど）
・各 RACEID に対しオッズ（単勝/複勝）ページへ疎通確認
・ログに明確な目印（### LIST v3 start ### / RACEID collected=... / OK: ...）
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

# =========================
# 設定
# =========================
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

TIMEOUT = (10, 20)   # (connect, read)
RETRY = 3
SLEEP_BETWEEN = (0.8, 1.6)

NOTIFIED_PATH = os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json")
DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"
KILL_SWITCH = os.getenv("KILL_SWITCH", "False").lower() == "true"

# =========================
# ユーティリティ
# =========================
RACEID_RE = re.compile(r"/RACEID/(\d{18})")

def jst_now_str(fmt="%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now(JST).strftime(fmt)

def today_id_str() -> str:
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

def uniq_sorted(xs: List[str]) -> List[str]:
    return sorted(set(xs))

# =========================
# RACEID 抽出 v3
# =========================
def list_today_raceids_v3() -> List[str]:
    """
    入口: その日の「出馬表（一覧）」ページ
    例: https://keiba.rakuten.co.jp/race_card/list/RACEID/{YYYYMMDD}0000000000
    """
    logging.info("### LIST v3 start ###")
    ymd = today_id_str()
    list_url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    html = fetch(list_url)
    soup = BeautifulSoup(html, "lxml")

    raceids: List[str] = []

    # 1) #todaysTicket（本日の発売情報）を最優先
    todays = soup.find(id="todaysTicket")
    if todays:
        logging.info("#todaysTicket found")
        for a in todays.select("a[href]"):
            text = (a.get_text(strip=True) or "")
            href = a["href"]
            m = RACEID_RE.search(href)
            if not m:
                continue
            if ("投票受付中" in text) or ("発走" in text):
                raceids.append(m.group(1))
    else:
        logging.info("#todaysTicket missing -> fallback")

    # 2) フォールバック：ページ内の全リンクを走査
    if not raceids:
        links = soup.find_all("a", href=True)
        for a in links:
            text = (a.get_text(strip=True) or "")
            href = a["href"]
            m = RACEID_RE.search(href)
            if not m:
                continue
            if ("投票受付中" in text) or ("発走" in text):
                raceids.append(m.group(1))

    # 3) さらに保険：レース一覧（RACEID/xxxxxxxxxxxxxxxxxx）系リンクからも拾う
    if not raceids:
        for a in soup.find_all("a", href=True):
            m = RACEID_RE.search(a["href"])
            if m:
                raceids.append(m.group(1))

    raceids = uniq_sorted(raceids)
    logging.info(f"RACEID collected={len(raceids)}")
    for rid in raceids:
        logging.info(f"  RACEID: {rid}")

    return raceids

# =========================
# オッズページ疎通
# =========================
def check_tanfuku_page(race_id: str) -> Optional[Dict]:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    # タイトルや時刻表示など最低限チェック
    h1 = soup.find("h1")
    nowtime = soup.select_one(".withUpdate .nowTime")
    race_title = h1.get_text(strip=True) if h1 else "N/A"
    now_label = nowtime.get_text(strip=True) if nowtime else ""

    odds_table = soup.find("table", {"summary": re.compile("オッズ")})
    if not odds_table:
        logging.warning(f"[WARN] 単勝/複勝テーブルが見つかりません: {url}")
        return None

    return {
        "race_id": race_id,
        "title": race_title,
        "now": now_label,
        "url": url,
    }

# =========================
# main
# =========================
def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if KILL_SWITCH:
        logging.info("[INFO] KILL_SWITCH=True のため、処理をスキップします。")
        return

    logging.info(f"[INFO] ジョブ開始 JST={jst_now_str()}")
    logging.info(f"[INFO] NOTIFIED_PATH={NOTIFIED_PATH} KILL_SWITCH={KILL_SWITCH} DRY_RUN={DRY_RUN}")

    raceids = list_today_raceids_v3()

    if not raceids:
        logging.info("[INFO] list_today_raceids_v3() は空でした。フォールバック終了。")
        logging.info("[INFO] HITS=0")
        save_notified({})
        logging.info("[INFO] ジョブ終了")
        return

    notified = load_notified()
    hits = 0

    for rid in raceids:
        meta = check_tanfuku_page(rid)
        if not meta:
            continue

        hits += 1
        logging.info(f"[INFO] OK: {meta['race_id']} {meta['title']} {meta['now']} {meta['url']}")
        notified[rid] = time.time()

        # 実際の通知処理はここへ（LINEなど）
        # if not DRY_RUN:
        #     send_line(meta)

        time.sleep(random.uniform(*SLEEP_BETWEEN))

    logging.info(f"[INFO] HITS={hits}")
    save_notified(notified)
    logging.info(f"[INFO] notified saved: {NOTIFIED_PATH} (bytes={len(json.dumps(notified, ensure_ascii=False))})")
    logging.info("[INFO] ジョブ終了")


if __name__ == "__main__":
    main()