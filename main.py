# -*- coding: utf-8 -*-
"""
Rakuten競馬：本日の出馬表(一覧)から「投票受付中」を抽出し、
RACEID を拾って単勝/複勝オッズページへ疎通確認する堅牢版（全面修正版）。

改修ポイント
- 「RACEIDS が未設定のため…」の分岐を廃止（envが未設定でも必ず列挙する）
- 本日の出馬表HTMLを /tmp/rakuten_list.html に毎回保存
- 4経路で RACEID を抽出（冗長化）
  経路#1: #todaysTicket 内の a[href]（推奨）
  経路#2: ページ内の全ての a[href] から /RACEID/\d{18} を網羅抽出
  経路#3: 「レース一覧」「投票受付中」などのテキスト行を優先抽出
  経路#4: env RACEIDS（カンマ/空白区切り）で明示追加（“上書き”ではなく“追加”）
- 単勝/複勝テーブル存在チェックを強化
- ログを簡潔だが判断可能なものに統一
"""

import os
import re
import json
import time
import random
import logging
from typing import List, Dict, Optional, Set
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

# -------------------------
# 基本設定
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
SLEEP_BETWEEN = (0.7, 1.4)

NOTIFIED_PATH = os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json")
DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"
KILL_SWITCH = os.getenv("KILL_SWITCH", "False").lower() == "true"

LIST_SAVE_PATH = "/tmp/rakuten_list.html"

RACEID_RE = re.compile(r"/RACEID/(\d{18})")

# -------------------------
# ユーティリティ
# -------------------------
def now_jst_str(fmt="%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now(JST).strftime(fmt)

def get_today_str() -> str:
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
            logging.warning(f"[WARN] fetch失敗({i}/{RETRY}): {e} -> {wait:.1f}s待機 url={url}")
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

def unique_sorted(seq) -> List[str]:
    return sorted(set(seq))

# -------------------------
# 抽出ロジック
# -------------------------
def enumerate_today_raceids() -> List[str]:
    """本日の出馬表(一覧)からRACEIDを多経路抽出"""
    ymd = get_today_str()
    list_url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    html = fetch(list_url)

    # 保存（デバッグ用）
    try:
        with open(LIST_SAVE_PATH, "w", encoding="utf-8") as f:
            f.write(html)
        logging.info(f"[INFO] 取得HTMLを保存: {LIST_SAVE_PATH}")
    except Exception as e:
        logging.warning(f"[WARN] HTML保存に失敗: {e}")

    soup = BeautifulSoup(html, "lxml")

    raceids: Set[str] = set()

    # 経路#1: #todaysTicket の中だけを優先で抽出（最も信頼できる）
    table = soup.find(id="todaysTicket")
    if table:
        for a in table.select("a[href]"):
            href = a.get("href", "")
            m = RACEID_RE.search(href)
            if m:
                txt = (a.get_text(strip=True) or "")
                # 「投票受付中」か「発走」を強めに優先
                if "投票受付中" in txt or "発走" in txt or "レース一覧" in txt:
                    raceids.add(m.group(1))

    # 経路#2: ページ全体の a[href] を走査（保険）
    for a in soup.find_all("a", href=True):
        m = RACEID_RE.search(a["href"])
        if m:
            raceids.add(m.group(1))

    # 経路#3: 行テキストのヒントベース（強めフィルタ）
    for td in soup.find_all(["td", "a"]):
        text = (td.get_text(strip=True) or "")
        if ("投票受付中" in text) or ("レース一覧" in text) or ("発走" in text):
            # 付近のリンクから RACEID を拾う
            near = td if hasattr(td, "find_all") else None
            if near:
                for a in near.find_all("a", href=True):
                    m = RACEID_RE.search(a["href"])
                    if m:
                        raceids.add(m.group(1))

    # 経路#4: 環境変数での明示追加（任意）
    env_ids = []
    raw_env = os.getenv("RACEIDS", "")
    if raw_env:
        for token in re.split(r"[,\s]+", raw_env.strip()):
            if re.fullmatch(r"\d{18}", token):
                env_ids.append(token)
    if env_ids:
        logging.info(f"[INFO] 環境変数RACEIDSでの明示追加: {len(env_ids)}件")
        raceids.update(env_ids)

    out = unique_sorted(raceids)
    logging.info(f"[INFO] 本日ページからのRACEIDユニーク合計: {len(out)} 件")
    return out

# -------------------------
# オッズページ疎通
# -------------------------
def probe_tanfuku(race_id: str) -> Optional[Dict]:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""
    nowtime = soup.select_one(".withUpdate .nowTime")
    now_label = nowtime.get_text(strip=True) if nowtime else ""

    # 単複テーブル検出（summaryに“オッズ”を含むtable）
    odds_table = soup.find("table", {"summary": re.compile("オッズ")})
    if not odds_table:
        # 代替: 人気順 or 高配当順テーブルの存在
        alt = soup.select_one("table.dataTable thead.singleOdds")
        if not alt:
            logging.warning(f"[WARN] 単複テーブル未検出: {url}")
            return None

    return {
        "race_id": race_id,
        "title": title or "N/A",
        "now": now_label,
        "url": url,
    }

# -------------------------
# メイン
# -------------------------
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    if KILL_SWITCH:
        logging.info("[INFO] KILL_SWITCH=True のためスキップ")
        return

    logging.info(f"[INFO] ジョブ開始 JST={now_jst_str()}")
    logging.info(f"[INFO] NOTIFIED_PATH={NOTIFIED_PATH} KILL_SWITCH={KILL_SWITCH} DRY_RUN={DRY_RUN}")

    raceids = enumerate_today_raceids()
    if not raceids:
        logging.info("[INFO] 本日RACEIDが見つかりません（開催なし/構造変化/取得失敗）。")
        save_notified({})
        logging.info("[INFO] ジョブ終了")
        return

    for rid in raceids:
        logging.info(f"[INFO] チェック: {rid} -> tanfuku")

    notified = load_notified()
    hits = 0

    for rid in raceids:
        try:
            meta = probe_tanfuku(rid)
        except Exception as e:
            logging.warning(f"[WARN] tanfuku取得失敗: {rid} err={e}")
            continue

        if not meta:
            continue

        hits += 1
        logging.info(f"[OK] {meta['race_id']} {meta['title']} {meta['now']} {meta['url']}")
        notified[rid] = time.time()

        if not DRY_RUN:
            # ここで通知やシート更新などに接続
            pass

        time.sleep(random.uniform(*SLEEP_BETWEEN))

    logging.info(f"[INFO] HITS={hits}")
    save_notified(notified if hits else {})
    logging.info(f"[INFO] notified saved: {NOTIFIED_PATH} (bytes={len(json.dumps(notified if hits else {}, ensure_ascii=False))})")
    logging.info("[INFO] ジョブ終了")

if __name__ == "__main__":
    main()