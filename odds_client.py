# 既存の import 群のままでOK
import os
import re
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))

HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def _get(url: str, timeout: int = 12) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

# ---------- ここから：当日レースIDの自動取得 ----------
_RACEID_PAT = re.compile(r"/RACEID/(\d{16,20})")

def _extract_raceids_from_html(html: str) -> List[str]:
    ids = set(m.group(1) for m in _RACEID_PAT.finditer(html))
    return sorted(ids)

def _try_fetch(url: str) -> List[str]:
    try:
        html = _get(url, timeout=10)
        ids = _extract_raceids_from_html(html)
        if ids:
            logging.info("race ids fetched from %s: %d件", url, len(ids))
        return ids
    except Exception as e:
        logging.debug("fetch skip %s: %s", url, e)
        return []

def list_today_raceids() -> List[str]:
    """
    当日の全レースIDを楽天競馬サイトから“広めに”拾う。
    - まず RACEIDS 環境変数（手動指定）があればそれを優先
    - なければ複数の候補ページをクロールして /RACEID/<id> を抽出
    - 取得0件なら最後に FALLBACK_RACEIDS があれば使う
    """
    # 1) 手動指定があれば最優先
    env = os.getenv("RACEIDS", "").strip()
    if env:
        ids = [x.strip() for x in env.split(",") if x.strip()]
        logging.info("RACEIDS（env）から %d 件", len(ids))
        return ids

    # 2) 自動取得
    today = datetime.now(JST).strftime("%Y%m%d")
    # なるべく取りこぼさないよう複数URLを試す（存在しないURLは無視）
    candidate_urls = [
        # トップ/スマホ版
        "https://keiba.rakuten.co.jp/",
        "https://keiba.rakuten.co.jp/smp/",
        # 当日カード・開催一覧っぽいページ（将来のUI変更に備えて多めに）
        f"https://keiba.rakuten.co.jp/race_card/list?raceDate={today}",
        f"https://keiba.rakuten.co.jp/race/list?raceDate={today}",
        f"https://keiba.rakuten.co.jp/?raceDate={today}",
        # 直リンクで回ってくることが多いパターン
        f"https://keiba.rakuten.co.jp/race_card/list/date/{today}",
        f"https://keiba.rakuten.co.jp/odds/tanfuku/date/{today}",
    ]

    found: List[str] = []
    seen = set()
    for url in candidate_urls:
        ids = _try_fetch(url)
        for rid in ids:
            if rid not in seen:
                seen.add(rid)
                found.append(rid)

    # 3) 0件ならフォールバック（任意）
    if not found:
        fb = os.getenv("FALLBACK_RACEIDS", "").strip()
        if fb:
            ids = [x.strip() for x in fb.split(",") if x.strip()]
            logging.warning("自動取得0件。FALLBACK_RACEIDS から %d 件", len(ids))
            return ids
        logging.info("当日のレースID自動取得は0件でした。")
        return []

    logging.info("当日のレースID 自動取得: %d件", len(found))
    return sorted(found)
# ---------- ここまで：当日レースIDの自動取得 ----------


# 以降（単勝オッズ取得など）は今のままでOK
# fetch_tanfuku_odds(...) 等、これまで通り使えます