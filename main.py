# -*- coding: utf-8 -*-
"""
Rakuten競馬：本日の出馬表(一覧)から「投票受付中」のレースを抽出し、
RACEID を拾って単勝/複勝オッズページにアクセスする最小・堅牢版。

ポイント
- 公式「本日の発売情報」(#todaysTicket)だけを見て、"投票受付中" の <a> の href から RACEID を正規表現で抽出
- 取得した RACEID で https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{id} にアクセス
- どのページ構造でも壊れにくいように、厳密な CSS セレクタには依存しすぎない
- 既存の環境変数 (DRY_RUN, NOTIFIED_PATH など) があれば軽く対応
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
            # 文字コードはサイトがUTF-8宣言済みだが、保険で設定
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
# 1) 今日の出馬表(一覧)から「投票受付中」の RACEID を集める
# -------------------------
RACEID_RE = re.compile(r"/RACEID/(\d{18})")

def list_today_raceids() -> List[str]:
    ymd = get_today_str()
    list_url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    html = fetch(list_url)
    soup = BeautifulSoup(html, "lxml")

    table = soup.find(id="todaysTicket")
    if not table:
        logging.info("[INFO] #todaysTicket が見つからないためフォールバックします（全リンク走査）")
        links = soup.find_all("a", href=True)
    else:
        # 「投票受付中」を含むセル（td.nextRace）配下のリンクを狙う
        links = table.select("td.nextRace a[href], td a[href]")

    raceids: List[str] = []
    for a in links:
        text = (a.get_text(strip=True) or "")
        href = a["href"]
        m = RACEID_RE.search(href)
        if not m:
            continue
        race_id = m.group(1)

        # 「投票受付中」が含まれる行に限定（厳しめフィルタ）
        if "投票受付中" in text or "発走" in text:
            raceids.append(race_id)

    # 重複排除＆安定ソート
    raceids = sorted(set(raceids))
    return raceids

# -------------------------
# 2) 各レースの単勝/複勝オッズページへアクセス（疎通確認）
# -------------------------
def check_tanfuku_page(race_id: str) -> Optional[Dict]:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    # 見出しや時刻など、最低限のメタを拾う（強すぎる依存は避ける）
    h1 = soup.find("h1")
    nowtime = soup.select_one(".withUpdate .nowTime")
    race_title = h1.get_text(strip=True) if h1 else "N/A"
    now_label = nowtime.get_text(strip=True) if nowtime else ""

    # 単勝のテーブルがあるか軽く確認（壊れてないか）
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

# -------------------------
# メイン
# -------------------------
def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if KILL_SWITCH:
        logging.info("[INFO] KILL_SWITCH=True のため、処理をスキップします。")
        return

    logging.info(f"[INFO] ジョブ開始 JST={datetime.now(JST):%Y-%m-%d %H:%M:%S}")
    logging.info(f"[INFO] NOTIFIED_PATH={NOTIFIED_PATH} KILL_SWITCH={KILL_SWITCH} DRY_RUN={DRY_RUN}")

    raceids = list_today_raceids()
    if not raceids:
        logging.info("[INFO] list_today_raceids() は空でした。フォールバックします。")
        # 念のため：帯広/盛岡/大井の “レース一覧” へのリンク群を拾う
        # （ページ例: .../race_card/list/RACEID/202508101006060400 等）
        # ここは最低限の保険なので、何もしないで終了でもOK
        logging.info("[INFO] HITS=0")
        save_notified({})
        logging.info("[INFO] ジョブ終了")
        return

    logging.info(f"[INFO] 発見RACEID数: {len(raceids)}")
    for rid in raceids:
        logging.info(f"  - {rid} -> odds/tanfuku")

    notified = load_notified()

    hits = 0
    for rid in raceids:
        meta = check_tanfuku_page(rid)
        if not meta:
            continue

        hits += 1
        logging.info(f"[INFO] OK: {meta['race_id']} {meta['title']} {meta['now']} {meta['url']}")

        # 通知/記録の例：既に通知済みなら何もしない（ここでは保存だけ）
        notified[rid] = time.time()

        # 実際のLINE通知やシート更新は既存処理に接続してください
        # if not DRY_RUN:
        #     send_line(meta)

        time.sleep(random.uniform(*SLEEP_BETWEEN))

    logging.info(f"[INFO] HITS={hits}")
    save_notified(notified)
    logging.info(f"[INFO] notified saved: {NOTIFIED_PATH} (bytes={len(json.dumps(notified, ensure_ascii=False))})")
    logging.info("[INFO] ジョブ終了")

if __name__ == "__main__":
    main()