# utils/raceids.py — 本日の「地方競馬・全レース」RACEIDを安全取得
# v1.6: 厳密検証で0件のときは緩和検証で取り直す（盛岡などの相性対策）
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
USER_AGENT = "Mozilla/5.0 (compatible; LocalKeibaNotifier/1.6)"
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
MEETING_SUFFIX = re.compile(r"\d{8}0{10}$")  # 例: 20250810 + 0000000000
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
    for pat in RACE_LINK_PATTERNS:  # 念のため本文も走査
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

# ===== 単勝オッズページの検証 =====
_ODDS_NUM = re.compile(r"\b\d{1,3}\.\d{1,2}\b")
_BLOCK_WORDS_STRICT = (
    "発売前", "発売開始前", "ただいま集計中", "集計中",
    "発売は締め切りました", "投票は締め切りました",
    "オッズ情報はありません", "発売中止",
)
_PLACEHOLDER = {"--", "—", "-", "0.0", "0", ""}

def _table_ready_check(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return False

    target = None
    for tb in tables:
        if "単勝" in tb.get_text() or "単勝オッズ" in tb.get_text():
            target = tb
            break
    if target is None:
        target = tables[0]

    rows = target.find_all("tr")
    data_rows = [tr for tr in rows if len(tr.find_all("td")) >= 2]
    if len(data_rows) < 6:
        return False

    numeric_cells = 0
    placeholder_cells = 0
    for tr in data_rows:
        for td in tr.find_all("td"):
            txt = td.get_text(strip=True)
            if _ODDS_NUM.fullmatch(txt):
                numeric_cells += 1
            elif txt in _PLACEHOLDER:
                placeholder_cells += 1

    if numeric_cells < 3:
        return False

    total = numeric_cells + placeholder_cells
    if total > 0 and (placeholder_cells / total) >= 0.5:
        return False

    return True

def _is_tanfuku_ready_strict(sess: requests.Session, rid: str) -> bool:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"
    try:
        r = sess.get(url)
        if not r.ok or not r.text:
            return False
        text = r.text

        for w in _BLOCK_WORDS_STRICT:
            if w in text:
                return False
        if ("単勝" not in text) and ("単勝オッズ" not in text):
            return False

        if len(_ODDS_NUM.findall(text)) < 3:
            return False
        if not _table_ready_check(text):
            return False
        return True
    except Exception:
        return False

# —— フォールバック用：やや緩い判定（発売中だが数値が少ない・表構造が異なる会場向け）
_BLOCK_WORDS_RELAXED = ("発売中止", "オッズ情報はありません")
def _is_tanfuku_ready_relaxed(sess: requests.Session, rid: str) -> bool:
    """
    NG: 中止/未提供。OK: '単勝' があり、テーブル行数>=4 か 1件以上の小数が見える。
    盛岡などで発売中でも厳密条件を満たさないケースを救済。
    """
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"
    try:
        r = sess.get(url)
        if not r.ok or not r.text:
            return False
        text = r.text
        for w in _BLOCK_WORDS_RELAXED:
            if w in text:
                return False
        if ("単勝" not in text) and ("単勝オッズ" not in text):
            return False

        soup = BeautifulSoup(text, "html.parser")
        tables = soup.find_all("table")
        if tables:
            # 緩め：実データ行が4行以上あればOK
            for tb in tables:
                rows = tb.find_all("tr")
                data_rows = [tr for tr in rows if len(tr.find_all("td")) >= 2]
                if len(data_rows) >= 4:
                    return True

        # あるいは小数が1つでも見えればOK
        if _ODDS_NUM.search(text):
            return True

        return False
    except Exception:
        return False

# ===== メイン関数 =====
def get_all_local_race_ids_today() -> List[str]:
    """
    トップ/一覧 → 開催日配下 → detail/odds をたどって候補を収集。
    まず厳密検証でフィルタし、結果が0件なら緩和検証で取り直す。
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
    for mid in list(meeting_ids)[:16]:  # 深度↑：最大16会場
        list_url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{mid}"
        race_level |= _extract_ids_from_url(sess, list_url)
        time.sleep(0.1)

    # 4) 取りこぼし対策：detail/odds を広めに覗く
    peek = list(race_level)[:80]  # 深度↑
    for rid in peek:
        for path in (
            f"https://keiba.rakuten.co.jp/race/detail/{rid}",
            f"https://keiba.rakuten.co.jp/odds/{rid}",
        ):
            race_level |= _extract_ids_from_url(sess, path)
            time.sleep(0.08)

    # 5) 形式面でクリーニング（開催日ID除外）
    cleaned = sorted({
        i for i in race_level
        if re.fullmatch(r"\d{18,}", i) and not _is_meeting_id(i)
    })

    # 6) 厳密検証
    strict_validated: List[str] = []
    for rid in cleaned:
        if _is_tanfuku_ready_strict(sess, rid):
            strict_validated.append(rid)
        time.sleep(0.06)

    if strict_validated:
        return strict_validated

    # 7) フォールバック：緩和検証で再フィルタ（盛岡などを救済）
    relaxed_validated: List[str] = []
    for rid in cleaned:
        if _is_tanfuku_ready_relaxed(sess, rid):
            relaxed_validated.append(rid)
        time.sleep(0.05)

    return relaxed_validated