# utils/raceids.py  — 本日の「地方競馬・全レース」の RACEID を安全に列挙
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
USER_AGENT = "Mozilla/5.0 (compatible; LocalKeibaNotifier/1.1)"
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

    # 既定タイムアウトを強制
    orig_request = s.request
    def _req(method, url, **kw):
        kw.setdefault("timeout", timeout)
        return orig_request(method, url, **kw)
    s.request = _req  # type: ignore
    return s

# ===== ID 抽出用パターン =====
# レース個別ページ/オッズページ/レース一覧のリンクから RACEID を拾う
RACE_LINK_PATTERNS = [
    re.compile(r"/race_card/list/RACEID/(\d{18,})"),
    re.compile(r"/race/detail/(\d{18,})"),
    re.compile(r"/odds/(?:tanfuku/)?RACEID/(\d{18,})"),
    re.compile(r"/odds/(\d{18,})"),
]

# 開催日ID（末尾10桁が全部0）を識別：例 20250810 + 0000000000
MEETING_SUFFIX = re.compile(r"\d{8}0{10}$")

def _is_meeting_id(rid: str) -> bool:
    return bool(MEETING_SUFFIX.fullmatch(rid))

# ===== 抽出ユーティリティ =====
def _extract_ids_from_html(html: str) -> Set[str]:
    """HTMLから RACEID 候補を収集（href優先＋保険でテキスト全体も走査）"""
    ids: Set[str] = set()

    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        for pat in RACE_LINK_PATTERNS:
            m = pat.search(href)
            if m:
                ids.add(m.group(1))

    # 念のため本文全体からも拾う（取りこぼし対策）
    for pat in RACE_LINK_PATTERNS:
        ids |= set(pat.findall(html))

    # 数字18桁以上に限定
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
    """多くのIDは先頭にYYYYMMDDを含む→当日優先。無ければそのまま返す。"""
    today_ids = {i for i in ids if i.startswith(today)}
    return today_ids if today_ids else set(ids)

# ===== メイン関数 =====
def get_all_local_race_ids_today() -> List[str]:
    """
    Rakuten競馬のトップ/一覧 → （必要に応じて）開催日ID配下を深掘りして、
    “本日の地方競馬・各レースIDのみ” を返す。
    - 開催日ID（末尾10桁が0）は除外
    - 取りこぼしを減らすため detail/odds を薄くプレビュー
    - 失敗時は空リスト
    """
    today = dt.datetime.now(JST).strftime("%Y%m%d")

    entry_urls = [
        "https://keiba.rakuten.co.jp/",
        "https://keiba.rakuten.co.jp/schedule/list",
        "https://keiba.rakuten.co.jp/racecard",
    ]

    sess = _session()
    coarse: Set[str] = set()

    # 1) まずトップ/一覧から “当日らしきID” を拾う
    for url in entry_urls:
        coarse |= _maybe_filter_today(_extract_ids_from_url(sess, url), today)

    # 2) 開催日IDとレースIDを仕分け
    meeting_ids = {rid for rid in coarse if _is_meeting_id(rid)}
    race_level: Set[str] = {rid for rid in coarse if not _is_meeting_id(rid)}

    # 3) 開催日IDの配下一覧を開き、そこから“各レースID”を抽出
    for mid in list(meeting_ids)[:12]:  # 安全のため最大12会場まで
        list_url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{mid}"
        race_level |= _extract_ids_from_url(sess, list_url)
        time.sleep(0.15)

    # 4) 取りこぼし削減：一部の detail/odds を覗く
    peek = list(race_level)[:40]
    for rid in peek:
        for path in (
            f"https://keiba.rakuten.co.jp/race/detail/{rid}",
            f"https://keiba.rakuten.co.jp/odds/{rid}",
        ):
            race_level |= _extract_ids_from_url(sess, path)
            time.sleep(0.12)

    # 5) ルール最終適用：18桁以上 & 開催日ID除外 → 昇順
    cleaned = sorted({
        i for i in race_level
        if re.fullmatch(r"\d{18,}", i) and not _is_meeting_id(i)
    })

    return cleaned