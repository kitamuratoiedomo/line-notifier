# scripts/backtest_strategies_debug.py
import os
import re
import sys
import json
import time
import math
import random
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple

import requests

# === ログ設定 ================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("backtest_debug")

# Render の本体コードと一緒に動かす想定
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from odds_client import fetch_tanfuku_odds  # 単勝オッズ取得（本番のやつを使う）

JST = timezone(timedelta(hours=9))
HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# === ユーティリティ ==========================================================
def get_html(url: str, timeout: int = 12) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code != 200:
            return None
        r.encoding = r.apparent_encoding or "utf-8"
        return r.text
    except Exception:
        return None

def extract_raceids_from_html(html: str) -> List[str]:
    # どのページにも "RACEID/数字" が埋まっていることが多いので一網打尽に拾う
    return list(dict.fromkeys(re.findall(r"RACEID/(\d{15,})", html or "")))

def list_raceids_for_date(d: datetime) -> List[str]:
    """
    楽天の “日別ページ” のパスは頻繁に変わるので、
    それっぽい候補URLを複数たたいて RACEID を総当り抽出する。
    1つでも当たりがあればOKという割り切り。
    """
    ymd = d.strftime("%Y%m%d")
    candidates = [
        f"https://keiba.rakuten.co.jp/race_card/list/DATE/{ymd}",
        f"https://keiba.rakuten.co.jp/race_card/list?date={ymd}",
        f"https://keiba.rakuten.co.jp/schedule/list/days/{ymd}",
        f"https://keiba.rakuten.co.jp/odds/tanfuku?date={ymd}",
        f"https://keiba.rakuten.co.jp/",  # フォールバック（当日なら何かしら載っている）
    ]
    found: List[str] = []
    for url in candidates:
        html = get_html(url, timeout=8)
        if not html:
            continue
        ids = extract_raceids_from_html(html)
        if ids:
            found.extend(ids)
    # uniq の保持
    found = list(dict.fromkeys(found))
    return found

# === 戦略ロジック（“一致件数”だけ数える）====================================
def sort_by_odds(horses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    hs = [h for h in horses if isinstance(h.get("odds"), (int, float))]
    return sorted(hs, key=lambda x: x["odds"])

def strategy_match_ids(horses: List[Dict[str, Any]]) -> List[str]:
    """
    どの戦略に“条件一致”したかを返す（S1/S2/S3/S4）。
    ※的一致件数のみ。的中・回収は扱わない。
    """
    res: List[str] = []
    ranks = sort_by_odds(horses)
    if len(ranks) < 4:
        return res

    o1, o2, o3, o4 = ranks[0]["odds"], ranks[1]["odds"], ranks[2]["odds"], ranks[3]["odds"]

    # ① 1〜3番人気 三連単BOX
    # 条件: 1人気 2.0〜10.0 / 2〜3人気 <10.0 / 4人気 >=15.0
    if (2.0 <= o1 <= 10.0) and (o2 < 10.0) and (o3 < 10.0) and (o4 >= 15.0):
        res.append("S1")

    # ② 1番人気1着固定 × 2・3番人気（三連単2点）
    # 条件: 1人気 <2.0 / 2〜3人気 <10.0
    if (o1 < 2.0) and (o2 < 10.0) and (o3 < 10.0):
        res.append("S2")

    # ③ 1→相手4頭→相手4頭（1頭マルチ）
    # 条件: 1人気 <=1.5 かつ “相手” が 2番人気以下の中で 10〜20倍の馬を最大4頭
    # 追加条件: 2番人気のオッズが 10倍以上（ユーザー要望）
    if (o1 <= 1.5) and (o2 >= 10.0):
        # 2番人気以下から 10〜20 倍の馬を抽出
        pool = [h for h in ranks[1:] if 10.0 <= h["odds"] <= 20.0]
        if len(pool) >= 1:  # 本来は最大4頭。ここは“出現件数”を見るため >=1 で一致にする
            res.append("S3")

    # ④ 3着固定（3番人気固定）三連単 2点
    # 条件: 1・2人気 <= 3.0 / 3人気 6.0〜10.0 / 4人気 >= 15.0
    if (o1 <= 3.0) and (o2 <= 3.0) and (6.0 <= o3 <= 10.0) and (o4 >= 15.0):
        res.append("S4")

    return res

# === メイン =================================================================
def main():
    days_back = int(os.getenv("BT_DAYS", "365"))  # 既定は過去1年
    sample_cap = int(os.getenv("BT_SAMPLE_CAP", "0"))  # 0なら全件、>0 なら日単位でサンプリング
    per_day_cap = int(os.getenv("BT_PER_DAY_CAP", "0"))  # 1日あたりのレース上限（デバッグ向け）

    start = datetime.now(JST) - timedelta(days=days_back)
    today = datetime.now(JST)

    log.info("=== BACKTEST DEBUG START === days_back=%s sample_cap=%s per_day_cap=%s",
             days_back, sample_cap, per_day_cap)

    totals = {
        "days_scanned": 0,
        "race_ids_found": 0,
        "odds_ok": 0,
        "odds_fail": 0,
        "too_few_horses": 0,
        "strategy_hits": {"S1": 0, "S2": 0, "S3": 0, "S4": 0},
    }

    # 失敗例の一部を溜める（多すぎないように）
    samples = {
        "raceids_empty_days": [],
        "odds_fail_ids": [],
        "too_few_ids": [],
        "matched_ids": {"S1": [], "S2": [], "S3": [], "S4": []},
    }

    d = start
    day_idx = 0
    while d <= today:
        day_idx += 1
        if sample_cap and (day_idx % sample_cap != 0):
            d += timedelta(days=1)
            continue

        ids = list_raceids_for_date(d)
        totals["days_scanned"] += 1
        totals["race_ids_found"] += len(ids)
        if not ids:
            if len(samples["raceids_empty_days"]) < 10:
                samples["raceids_empty_days"].append(d.strftime("%Y-%m-%d"))
            log.info("[DAY %s] %s: race ids=0", totals["days_scanned"], d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
            continue

        if per_day_cap and len(ids) > per_day_cap:
            ids = ids[:per_day_cap]

        log.info("[DAY %s] %s: race ids=%s", totals["days_scanned"], d.strftime("%Y-%m-%d"), len(ids))

        for rid in ids:
            info = fetch_tanfuku_odds(rid)
            if not info or not info.get("horses"):
                totals["odds_fail"] += 1
                if len(samples["odds_fail_ids"]) < 20:
                    samples["odds_fail_ids"].append(rid)
                continue

            horses = info["horses"]
            if len(horses) < 4:
                totals["too_few_horses"] += 1
                if len(samples["too_few_ids"]) < 10:
                    samples["too_few_ids"].append(rid)
                continue

            totals["odds_ok"] += 1
            matched = strategy_match_ids(horses)
            for mid in matched:
                totals["strategy_hits"][mid] += 1
                if len(samples["matched_ids"][mid]) < 20:
                    samples["matched_ids"][mid].append(rid)

        # 進捗ログ（多すぎないように）
        if totals["odds_ok"] % 100 == 0 and totals["odds_ok"] > 0:
            log.info("PROGRESS: odds_ok=%s, hits=%s",
                     totals["odds_ok"], json.dumps(totals["strategy_hits"], ensure_ascii=False))

        d += timedelta(days=1)

    # 結果出力
    log.info("=== SUMMARY ===")
    log.info("days_scanned=%s  race_ids_found=%s", totals["days_scanned"], totals["race_ids_found"])
    log.info("odds_ok=%s  odds_fail=%s  too_few_horses=%s",
             totals["odds_ok"], totals["odds_fail"], totals["too_few_horses"])
    log.info("strategy_hits=%s", json.dumps(totals["strategy_hits"], ensure_ascii=False))

    # サンプル（原因の当たりを付けるため）
    log.info("--- samples: raceids_empty_days (up to 10) --- %s", samples["raceids_empty_days"])
    log.info("--- samples: odds_fail_ids (up to 20) --- %s", samples["odds_fail_ids"])
    log.info("--- samples: too_few_ids (up to 10) --- %s", samples["too_few_ids"])
    for k, v in samples["matched_ids"].items():
        log.info("--- samples: matched %s (up to 20) --- %s", k, v)

    print(json.dumps({"totals": totals, "samples": samples}, ensure_ascii=False))


if __name__ == "__main__":
    main()