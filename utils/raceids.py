# utils/raceids.py
from __future__ import annotations
import re
import time
import datetime as dt
from typing import List, Set, Iterable
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

JST = dt.timezone(dt.timedelta(hours=9))
USER_AGENT = "Mozilla/5.0 (compatible; LocalKeibaNotifier/1.0)"
HEADERS = {"User-Agent": USER_AGENT}

# RakutenのレースIDが現れるURLパターン
RACE_LINK_PATTERNS = [
    re.compile(r"/race_card/list/RACEID/(\d{18,})"),
    re.compile(r"/race/detail/(\d{18,})"),
    re.compile(r"/odds/(\d{18,})"),
]

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
    s.request = lambda method, url, **kw: requests.Session.request(  # type: ignore
        s, method, url, timeout=kw.pop("timeout", timeout), **kw
    )
    return s

def _extract_ids_from_html(html: str) -> Set[str]:
    ids: Set[str] = set()
    # まずはaタグのhrefを総当り（安全・高速）
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        for pat in RACE_LINK_PATTERNS:
            m = pat.search(href)
            if m:
                ids.add(m.group(1))
    # 万一取りこぼしがある場合に備え、テキスト全体からも拾う
    for pat in RACE_LINK_PATTERNS:
        ids |= set(pat.findall(html))
    # 18桁以上のみ
    return {i for i in ids if re.fullmatch(r"\d{18,}", i)}

def _maybe_filter_today(ids: Iterable[str], today: str) -> Set[str]:
    # 多くのIDは先頭にYYYYMMDDを含むため、まずは当日IDを優先フィルタ
    today_ids = {i for i in ids if i.startswith(today)}
    return today_ids if today_ids else set(ids)

def get_all_local_race_ids_today() -> List[str]:
    """
    Rakuten競馬のトップ/一覧ページから“本日の地方競馬”に紐づくRACEIDを抽出。
    - ページ単位で軽量にクロール
    - 取りこぼし防止に一部詳細を薄くプレビュー
    - 取得できなければ空配列
    """
    today = dt.datetime.now(JST).strftime("%Y%m%d")
    entry_urls = [
        "https://keiba.rakuten.co.jp/",
        "https://keiba.rakuten.co.jp/schedule/list",
        "https://keiba.rakuten.co.jp/racecard",
    ]

    sess = _session()
    found: Set[str] = set()

    for url in entry_urls:
        try:
            r = sess.get(url)
            if not r.ok or not r.text:
                continue
            ids = _maybe_filter_today(_extract_ids_from_html(r.text), today)
            found |= ids
        except Exception:
            continue

    # 取りこぼし減らすため、最大20件だけ詳細を覗いて再抽出
    for rid in list(found)[:20]:
        for path in (
            f"https://keiba.rakuten.co.jp/race/detail/{rid}",
            f"https://keiba.rakuten.co.jp/odds/{rid}",
        ):
            try:
                r = sess.get(path)
                if r.ok and r.text:
                    found |= _extract_ids_from_html(r.text)
                    time.sleep(0.2)
            except Exception:
                pass

    # 重複排除して昇順
    cleaned = sorted({i for i in found if re.fullmatch(r"\d{18,}", i)})
    return cleaned