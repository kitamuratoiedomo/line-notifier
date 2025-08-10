# utils/raceids.py — 本日の「地方競馬・全レース」RACEIDを安全取得
# v1.8: tanfuku(単勝)の表だけでなく、race/detail の「発売中/投票/締切」でも発売中判定する
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
USER_AGENT = "Mozilla/5.0 (compatible; LocalKeibaNotifier/1.8)"
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
    base = s.request
    def _req(method, url, **kw):
        kw.setdefault("timeout", timeout)
        return base(method, url, **kw)
    s.request = _req  # type: ignore
    return s

# ===== ID抽出 =====
RACE_LINK_PATTERNS = [
    re.compile(r"/race_card/list/RACEID/(\d{18,})"),
    re.compile(r"/race/detail/(\d{18,})"),
    re.compile(r"/odds/(?:tanfuku/)?RACEID/(\d{18,})"),
    re.compile(r"/odds/(\d{18,})"),
]
MEETING_SUFFIX = re.compile(r"\d{8}0{10}$")  # 例: 20250810 + 0000000000
def _is_meeting_id(rid: str) -> bool: return bool(MEETING_SUFFIX.fullmatch(rid))

def _extract_ids_from_html(html: str) -> Set[str]:
    ids: Set[str] = set()
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        for pat in RACE_LINK_PATTERNS:
            m = pat.search(href)
            if m: ids.add(m.group(1))
    for pat in RACE_LINK_PATTERNS:  # 保険で本文も走査
        ids |= set(pat.findall(html))
    return {i for i in ids if re.fullmatch(r"\d{18,}", i)}

def _extract_ids_from_url(sess: requests.Session, url: str) -> Set[str]:
    try:
        r = sess.get(url)
        if not r.ok or not r.text: return set()
        return _extract_ids_from_html(r.text)
    except Exception:
        return set()

def _maybe_filter_today(ids: Iterable[str], today: str) -> Set[str]:
    today_ids = {i for i in ids if i.startswith(today)}
    return today_ids if today_ids else set(ids)

# ===== 発売中判定（tanfuku HTML / detail HTML の二段構え） =====
_ODDS_NUM = re.compile(r"\b\d{1,3}\.\d{1,2}\b")
_PLACEHOLDER = {"--", "—", "-", "0.0", "0", ""}

BLOCK_WORDS_COMMON = (
    "発売開始前", "発売前", "ただいま集計中", "集計中",
    "発売は締め切りました", "投票は締め切りました",
    "オッズ情報はありません", "発売中止",
)

def _table_ready_check(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables: return False
    # '単勝' 含むテーブルを優先
    target = None
    for tb in tables:
        if "単勝" in tb.get_text() or "単勝オッズ" in tb.get_text():
            target = tb; break
    if target is None: target = tables[0]

    rows = target.find_all("tr")
    data_rows = [tr for tr in rows if len(tr.find_all("td")) >= 2]
    if len(data_rows) < 4:  # 会場により頭数少のことがあるため 6→4 に緩和
        return False

    numeric_cells, placeholder_cells = 0, 0
    for tr in data_rows:
        for td in tr.find_all("td"):
            txt = td.get_text(strip=True)
            if _ODDS_NUM.fullmatch(txt): numeric_cells += 1
            elif txt in _PLACEHOLDER: placeholder_cells += 1

    if numeric_cells < 1:  # 完全ゼロはNG
        return False
    total = numeric_cells + placeholder_cells
    if total > 0 and (placeholder_cells / total) >= 0.7:  # プレースホルダだらけはNG
        return False
    return True

def _tanfuku_looks_open(sess: requests.Session, rid: str) -> bool:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"
    try:
        r = sess.get(url)
        if not r.ok or not r.text: return False
        text = r.text
        if any(w in text for w in BLOCK_WORDS_COMMON): return False
        if ("単勝" not in text) and ("単勝オッズ" not in text): return False
        # 小数が1つでも、かつテーブル検査も通ればOK（JS後描画でもHTMLに残る場合あり）
        if _ODDS_NUM.search(text) and _table_ready_check(text):
            return True
        # ここまででダメなら detail 側へ回す
        return False
    except Exception:
        return False

def _detail_says_open(sess: requests.Session, rid: str) -> bool:
    """
    race/detail ページに「投票ボタン」「締切」「オッズ更新」などが出ていれば発売中とみなす。
    一方、BLOCK_WORDS_COMMON があれば除外。
    """
    url = f"https://keiba.rakuten.co.jp/race/detail/{rid}"
    try:
        r = sess.get(url)
        if not r.ok or not r.text: return False
        text = r.text
        if any(w in text for w in BLOCK_WORDS_COMMON): return False

        # 代表的な発売中の目印
        OPEN_HINTS = ("投票", "締切", "オッズ", "オッズ更新", "投票する")
        if any(h in text for h in OPEN_HINTS):
            return True
        return False
    except Exception:
        return False

def _is_open(sess: requests.Session, rid: str) -> bool:
    # まず tanfuku で判定、ダメなら detail で補完
    return _tanfuku_looks_open(sess, rid) or _detail_says_open(sess, rid)

# ===== メイン =====
def get_all_local_race_ids_today() -> List[str]:
    """
    トップ/一覧 → 開催日配下 → detail/odds をたどって候補を収集。
    最後に “発売中（単勝ページ or detailページで確認）” のIDのみ返す。
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

    # 3) 開催日ID配下（会場ごとの一覧）から各レースID
    for mid in list(meeting_ids)[:16]:
        list_url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{mid}"
        race_level |= _extract_ids_from_url(sess, list_url)
        time.sleep(0.1)

    # 4) 取りこぼし削減：detail/odds を軽くクロール
    peek = list(race_level)[:80]
    for rid in peek:
        for path in (f"https://keiba.rakuten.co.jp/race/detail/{rid}",
                     f"https://keiba.rakuten.co.jp/odds/{rid}"):
            race_level |= _extract_ids_from_url(sess, path)
            time.sleep(0.08)

    # 5) フォーマット整形（開催日ID除外）
    cleaned = sorted({
        i for i in race_level
        if re.fullmatch(r"\d{18,}", i) and not _is_meeting_id(i)
    })

    # 6) 発売中チェック（tanfuku or detail）
    validated: List[str] = []
    for rid in cleaned:
        if _is_open(sess, rid):
            validated.append(rid)
        time.sleep(0.05)

    return validated