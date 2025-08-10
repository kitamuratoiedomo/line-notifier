# utils/raceids.py — 本日の「地方競馬・全レース」RACEIDを安全取得（厳密検証）
from __future__ import annotations

import re
import time
import datetime as dt
from typing import List, Set, Iterable

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

# ===== 時刻・HTTP =====
JST = dt.timezone(dt.timedelta(hours=9))
USER_AGENT = "Mozilla/5.0 (compatible; LocalKeibaNotifier/1.3)"
HEADERS = {"User-Agent": USER_AGENT}

def _session(timeout: int = 10) -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    orig_request = s.request
    def _req(method, url, **kw):
        kw.setdefault("timeout", timeout)
        return orig_request(method, url, **kw)
    s.request = _req  # type: ignore
    return s

# ===== ID 抽出用パターン =====
RACE_LINK_PATTERNS = [
    re.compile(r"/race_card/list/RACEID/(\d{18,})"),
    re.compile(r"/race/detail/(\d{18,})"),
    re.compile(r"/odds/(?:tanfuku/)?RACEID/(\d{18,})"),
    re.compile(r"/odds/(\d{18,})"),
]
MEETING_SUFFIX = re.compile(r"\d{8}0{10}$")  # 20250810 + 0000000000

def _is_meeting_id(rid: str) -> bool:
    return bool(MEETING_SUFFIX.fullmatch(rid))

# ===== 抽出ユーティリティ =====
def _extract_ids_from_html(html: str) -> Set[str]:
    ids: Set[str] = set()
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        for pat in RACE_LINK_PATTERNS:
            m = pat.search(href)
            if m:
                ids.add(m.group(1))
    for pat in RACE_LINK_PATTERNS:  # 念のため本文走査
        ids |= set(pat.findall(html))
    return {i for i in ids if re.fullmatch(r"\d{18,}", i)}

def _extract_ids_from_url(sess: requests.Session, url: str) -> Set[str]:
    try:
        r = sess.get(url)
        if not r.ok or not r.text:
            return set()
        return _extract_ids_from_html(r.text)
    except Exception:
        return set()

def _maybe_filter_today(ids: Iterable[str], today: str) -> Set[str]:
    today_ids = {i for i in ids if i.startswith(today)}
    return today_ids if today_ids else set(ids)

# ===== 単勝オッズページの「準備完了」判定 =====
# 数字検出（少数1〜2桁対応）
_ODDS_NUM = re.compile(r"\b\d{1,3}\.\d{1,2}\b")

_BLOCK_WORDS = (
    "発売前", "発売は締め切りました", "オッズ情報はありません",
    "ただいま集計中", "投票は締め切りました"
)

def _is_tanfuku_ready(sess: requests.Session, rid: str) -> bool:
    """
    単勝オッズページが実体を持ち、表が埋まっているかを判定。
    - ブロック語（発売前/締切/未提供/集計中）が含まれていたら不可
    - '単勝' か '単勝オッズ' を含み、本文にオッズらしき数値が複数（>=3）ある
    - <table> が1つ以上
    """
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"
    try:
        r = sess.get(url)
        if not r.ok or not r.text:
            return False
        text = r.text
        for w in _BLOCK_WORDS:
            if w in text:
                return False
        if ("単勝" not in text) and ("単勝オッズ" not in text):
            return False
        nums = _ODDS_NUM.findall(text)
        if len(nums) < 3:
            return False
        soup = BeautifulSoup(text, "html.parser")
        if not soup.find_all("table"):
            return False
        return True
    except Exception:
        return False

# ===== メイン関数 =====
def get_all_local_race_ids_today() -> List[str]:
    """
    トップ/一覧 → 開催日配下 → detail/odds をたどって候補を収集。
    最後に “単勝オッズページが **準備完了** のIDのみ” に絞り込んで返す。
    """
    today = dt.datetime.now(JST).strftime("%Y%m%d")
    entry_urls = [
        "https://keiba.rakuten.co.jp/",
        "https://keiba.rakuten.co.jp/schedule/list",
        "https://keiba.rakuten.co.jp/racecard",
    ]

    sess = _session()
    coarse: Set[str] = set()

    # 1) トップ/一覧から当日候補
    for url in entry_urls:
        coarse |= _maybe_filter_today(_extract_ids_from_url(sess, url), today)

    # 2) 開催日IDとレースIDを仕分け
    meeting_ids = {rid for rid in coarse if _is_meeting_id(rid)}
    race_level: Set[str] = {rid for rid in coarse if not _is_meeting_id(rid)}

    # 3) 開催日IDの配下から「各レースID」を取得
    for mid in list(meeting_ids)[:12]:  # 最大12会場
        list_url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{mid}"
        race_level |= _extract_ids_from_url(sess, list_url)
        time.sleep(0.12)

    # 4) 取りこぼし対策：一部 detail/odds を覗く
    peek = list(race_level)[:40]
    for rid in peek:
        for path in (
            f"https://keiba.rakuten.co.jp/race/detail/{rid}",
            f"https://keiba.rakuten.co.jp/odds/{rid}",
        ):
            race_level |= _extract_ids_from_url(sess, path)
            time.sleep(0.1)

    # 5) 形式面でクリーニング（開催日ID除外）
    cleaned = sorted({
        i for i in race_level
        if re.fullmatch(r"\d{18,}", i) and not _is_meeting_id(i)
    })

    # 6) **準備完了チェック**で最終フィルタ
    validated: List[str] = []
    for rid in cleaned:
        if _is_tanfuku_ready(sess, rid):
            validated.append(rid)
        time.sleep(0.08)  # サイト負荷配慮

    return validated