# coding: utf-8
"""
楽天競馬（地方）過去1年バックテスト：戦略①〜④の的中率/回収率
- RACEID を日付ごとに自動クロール
- 単勝オッズを robust にスクレイピング
- 確定結果ページから三連単払戻を取得
- 条件に一致するレースのみ購入したと仮定し、100円/点で試算

注意：これはバックテスト専用です。運用コードには影響しません。
"""

import re
import sys
import json
import time
import math
import random
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))

# ------------------------ 設定 ------------------------
DAYS_BACK = 365          # 直近何日分を見るか
SLEEP_SEC_BETWEEN_REQ = 0.4  # 叩きすぎ防止
MAX_RACES_PER_DAY = 400      # 安全弁：1日あたりの最大レース数
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) backtest/1.0"
HEADERS = {
    "user-agent": USER_AGENT,
    "accept-language": "ja,en;q=0.9",
    "referer": "https://keiba.rakuten.co.jp/"
}
# ------------------------------------------------------


# ============== 汎用HTTP ===============
def http_get(url: str, timeout: int = 12) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code != 200:
            return None
        r.encoding = r.apparent_encoding or "utf-8"
        return r.text
    except Exception:
        return None


# ============== RACEID 収集 ===============
RACEID_RE = re.compile(r"/RACEID/(\d{17,18})")

def list_raceids_for_date(yyyymmdd: str) -> List[str]:
    """
    例：20250601 の race card / result / odds などのページから RACEID を収集
    """
    urls = [
        f"https://keiba.rakuten.co.jp/?l-id=top",  # おとり（稼働確認用）
        f"https://keiba.rakuten.co.jp/race_calendar/list?date={yyyymmdd}",
        f"https://keiba.rakuten.co.jp/race/list?date={yyyymmdd}",
        f"https://keiba.rakuten.co.jp/result/list?date={yyyymmdd}",
        f"https://keiba.rakuten.co.jp/odds/list?date={yyyymmdd}",
    ]
    ids = set()
    for u in urls:
        html = http_get(u)
        if not html:
            continue
        for m in RACEID_RE.finditer(html):
            ids.add(m.group(1))
        time.sleep(SLEEP_SEC_BETWEEN_REQ)

    out = sorted(ids)
    if len(out) > MAX: