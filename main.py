# -*- coding: utf-8 -*-
"""
Rakuten競馬 全面差し替え版
- 今日の「出馬表一覧」から開催ごとのページを拾う
- 開催ページ内の各レース（個別RACEID）を列挙
- 各レースの単勝/人気テーブルから 1〜4番人気のオッズを抽出
- いただいた戦略①〜④で判定し、一致のみ通知（またはDRY_RUNでログ）
"""

import os
import re
import time
import json
import random
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

from strategy_rules import eval_strategy  # ← いただいた判定そのまま利用

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
TIMEOUT = (10, 20)
RETRY = 3
SLEEP_BETWEEN = (0.5, 1.2)

NOTIFIED_PATH = os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json")
DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"
KILL_SWITCH = os.getenv("KILL_SWITCH", "False").lower() == "true"
NOTIFY_ENABLED = os.getenv("NOTIFY_ENABLED", "0") == "1"
LINE_NOTIFY_TOKEN = os.getenv("LINE_NOTIFY_TOKEN", "").strip()

# テスト用: カンマ区切り（空欄なら無効）
# 例: DEBUG_RACEIDS="202508101006060411,202508111006060501"
DEBUG_RACEIDS = [x.strip() for x in os.getenv("DEBUG_RACEIDS", "").split(",") if x.strip()]

RACEID_RE = re.compile(r"/RACEID/(\d{18})")

# -------------------------
# ユーティリティ
# -------------------------
def today_str() -> str:
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

def line_notify(message: str) -> None:
    """LINE Notify が設定されている場合のみ送る。無ければ何もしない。"""
    if not (NOTIFY_ENABLED and LINE_NOTIFY_TOKEN and not DRY_RUN):
        logging.info("[DRY_RUN]" if DRY_RUN else "[INFO] 通知はスキップ（トークン未設定または無効）")
        return
    try:
        resp = SESSION.post(
            "https://notify-api.line.me/api/notify",
            headers={"Authorization": f"Bearer {LINE_NOTIFY_TOKEN}"},
            data={"message": message},
            timeout=TIMEOUT,
        )
        if resp.status_code != 200:
            logging.warning(f"[WARN] LINE通知失敗 status={resp.status_code} body={resp.text[:200]}")
        else:
            logging.info("[INFO] LINE通知OK")
    except Exception as e:
        logging.warning(f"[WARN] LINE通知中に例外: {e}")

# -------------------------
# RACEID 正規化と列挙
# -------------------------
def list_meeting_pages_for_today() -> List[str]:
    """本日の出馬表トップから、開催別「出馬表一覧」ページのURLを抜く"""
    ymd = today_str()
    url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    # 1) 「出馬表」メニューに並ぶ各開催の一覧リンク
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # 例: https://keiba.rakuten.co.jp/race_card/list/RACEID/202508111006060500
        if "/race_card/list/RACEID/" in href:
            m = RACEID_RE.search(href)
            if m:
                rid = m.group(1)
                # 開催一覧（末尾00）だけ採用（個別は後段で拾う）
                if rid.endswith("00"):
                    links.append(href)

    # 重複排除
    links = sorted(set(links))
    return links

def expand_meeting_to_raceids(meeting_url: str) -> List[str]:
    """開催別の『出馬表一覧』ページから、個別レースのRACEIDを全部抜く"""
    html = fetch(meeting_url)
    soup = BeautifulSoup(html, "lxml")

    raceids = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # tanfukuへの直リンクがあれば最優先で採用
        if "/odds/tanfuku/RACEID/" in href:
            m = RACEID_RE.search(href)
            if m:
                raceids.append(m.group(1))
                continue
        # それ以外に /race_card/list/RACEID/xxxxxx（個別レース）のリンクがあれば拾う
        if "/race_card/list/RACEID/" in href:
            m = RACEID_RE.search(href)
            if m:
                rid = m.group(1)
                # 個別レースは末尾が "01"〜"12" のように "00" 以外
                if not rid.endswith("00"):
                    raceids.append(rid)

    # 重複排除
    raceids = sorted(set(raceids))
    return raceids

def gather_today_raceids() -> List[str]:
    """今日の全開催から個別レースIDを集める。"""
    meeting_pages = list_meeting_pages_for_today()
    logging.info(f"[INFO] Rakuten#2 出馬表一覧: {len(meeting_pages)}件")

    all_ids: List[str] = []
    for url in meeting_pages:
        ids = expand_meeting_to_raceids(url)
        all_ids.extend(ids)

    all_ids = sorted(set(all_ids))
    return all_ids

# -------------------------
# tanfukuページ → 人気別オッズ抽出
# -------------------------
def parse_horses_from_tanfuku(race_id: str) -> Tuple[str, str, List[Dict]]:
    """
    returns: (title, nowLabel, horses)
    horses: [{pop:int, odds:float}, ...]
    """
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    h1 = soup.find("h1")
    race_title = h1.get_text(strip=True) if h1 else "地方競馬"
    nowtime = soup.select_one(".withUpdate .nowTime")
    now_label = nowtime.get_text(strip=True) if nowtime else ""

    # 単勝テーブルを緩く特定（ヘッダ/summaryに「単勝」or「オッズ」）
    odds_table = None
    for tbl in soup.find_all("table"):
        summary = (tbl.get("summary") or "") + " " + (tbl.get("aria-label") or "")
        head_txt = " ".join(th.get_text(strip=True) for th in tbl.find_all("th"))
        joined = (summary + " " + head_txt)
        if ("単勝" in joined) or ("オッズ" in joined):
            odds_table = tbl
            break
    if odds_table is None:
        logging.warning(f"[WARN] オッズテーブル未検出: {url}")
        return (race_title, now_label, [])

    # 列名っぽいものから「人気」「単勝」に近い列のindexを推定
    headers = [th.get_text(strip=True) for th in odds_table.find_all("th")]
    def find_col(candidates: List[str]) -> int:
        for i, txt in enumerate(headers):
            for key in candidates:
                if key in txt:
                    return i
        return -1

    idx_pop = find_col(["人気", "人", "人気順"])
    idx_odds = find_col(["単勝", "オッズ", "単"])

    horses: List[Dict] = []
    for tr in odds_table.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if not tds or len(tds) < 2:
            continue

        # 人気
        pop_val = None
        if 0 <= idx_pop < len(tds):
            txt = tds[idx_pop].get_text(" ", strip=True)
            # 例: "1" / "1人気" / "1 位"
            m = re.search(r"\d+", txt)
            if m:
                pop_val = int(m.group())

        # オッズ
        odds_val = None
        if 0 <= idx_odds < len(tds):
            txt = tds[idx_odds].get_text(" ", strip=True)
            # "1.7" / "1.7-2.3" / "1.7～2.3"
            txt = txt.replace("～", "-").replace("−", "-").replace("―", "-")
            m = re.search(r"\d+(?:\.\d+)?(?:-\d+(?:\.\d+)?)?", txt)
            if m:
                num = m.group()
                if "-" in num:
                    num = num.split("-")[0]
                odds_val = float(num)

        if pop_val is not None and odds_val is not None:
            horses.append({"pop": pop_val, "odds": odds_val})

    # 人気順でソート＆重複除去（pop重複があれば先に見つかった方を残す）
    uniq = {}
    for h in horses:
        uniq.setdefault(h["pop"], h)
    horses = [uniq[k] for k in sorted(uniq.keys())]

    return (race_title, now_label, horses)

# -------------------------
# メイン
# -------------------------
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    if KILL_SWITCH:
        logging.info("[INFO] KILL_SWITCH=True のため終了")
        return

    logging.info(f"[INFO] ジョブ開始 host={os.uname().nodename} pid={os.getpid()}")
    logging.info(f"[INFO] NOTIFIED_PATH={NOTIFIED_PATH} KILL_SWITCH={KILL_SWITCH} DRY_RUN={DRY_RUN}")
    if NOTIFY_ENABLED:
        logging.info("[INFO] NOTIFY_ENABLED=1")

    notified = load_notified()
    hits = 0
    matches = 0

    # --- 対象RACEIDの決定 ---
    target_ids: List[str]
    if DEBUG_RACEIDS:
        logging.info(f"[INFO] DEBUG_RACEIDS 指定: {len(DEBUG_RACEIDS)}件")
        target_ids = DEBUG_RACEIDS
    else:
        # 本日の開催一覧 → 各開催の個別レースIDへ展開
        meeting_pages = list_meeting_pages_for_today()
        logging.info(f"[INFO] Rakuten#1 本日の開催一覧: {len(meeting_pages)}件")
        target_ids = []
        for murl in meeting_pages:
            target_ids.extend(expand_meeting_to_raceids(murl))
        # 重複排除
        target_ids = sorted(set(target_ids))
        logging.info(f"[INFO] 個別レースID抽出: {len(target_ids)}件")

    # --- 各レースで判定 ---
    for rid in target_ids:
        title, now_label, horses = parse_horses_from_tanfuku(rid)
        if not horses:
            # 未発売/未表示の時間帯や一覧ID直叩きなどはここで弾かれる
            continue

        hits += 1
        url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"
        logging.info(f"[OK] tanfuku疎通: {rid} {title} {now_label} {url}")

        # 1〜4番人気が揃わなければ判定不能
        if len([h for h in horses if 1 <= h["pop"] <= 4]) < 4:
            logging.info(f"[NO MATCH] {rid} 条件詳細: horses<4 で判定不可")
            continue

        res = eval_strategy(horses)
        if not res:
            continue

        matches += 1
        tickets = ", ".join(res["tickets"])
        msg = (
            f"【一致】{res['strategy']}\n"
            f"RACEID: {rid}\n"
            f"{title} {now_label}\n"
            f"買い目: {tickets}\n"
            f"{res['roi']} / {res['hit']}\n"
            f"{url}"
        )
        logging.info(f"[MATCH] {rid} 条件詳細: {res['strategy']} / 買い目: {tickets} / {res['roi']} / {res['hit']}")

        # 通知
        if NOTIFY_ENABLED and not DRY_RUN:
            line_notify(msg)
            notified[rid] = time.time()
        else:
            logging.info("[DRY_RUN] 通知はスキップ")

        time.sleep(random.uniform(*SLEEP_BETWEEN))

    logging.info(f"[INFO] HITS={hits} / MATCHES={matches}")
    # 一致時のみ保存（DRY_RUN時は空のままでもOK）
    if not DRY_RUN:
        save_notified(notified)
        logging.info(f"[INFO] notified saved: {NOTIFIED_PATH} (bytes={len(json.dumps(notified, ensure_ascii=False))})")
    else:
        # DRY_RUNでも書式合わせのため空dictを保存する場合は下行を有効化
        save_notified({})
        logging.info(f"[INFO] notified saved: {NOTIFIED_PATH} (bytes=2)")

    logging.info("[INFO] ジョブ終了")

if __name__ == "__main__":
    main()