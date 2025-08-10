# utils/raceids.py — 本日の「地方競馬・全レース」RACEIDを安全取得
# v1.9: 会場一覧(list)で「投票」リンクを一次判定 + detailの締切/発走/投票ヒントで発売中補完
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
USER_AGENT = "Mozilla/5.0 (compatible; LocalKeibaNotifier/1.9)"
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
            if m:
                ids.add(m.group(1))
    # 本文保険
    for pat in RACE_LINK_PATTERNS:
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

# ===== 発売中判定（list/detail/tanfuku の三段構え） =====
_ODDS_NUM = re.compile(r"\b\d{1,3}\.\d{1,2}\b")
_TIME_PAT = re.compile(r"\b(?:[01]?\d|2[0-3]):[0-5]\d\b")  # 14:53 のような時刻
_PLACEHOLDER = {"--", "—", "-", "0.0", "0", ""}

BLOCK_WORDS_COMMON = (
    "発売開始前", "発売前", "ただいま集計中", "集計中",
    "発売は締め切りました", "投票は締め切りました",
    "オッズ情報はありません", "発売中止",
)

def _list_page_open_ids(sess: requests.Session, meeting_id: str) -> Set[str]:
    """
    会場一覧 (race_card/list/RACEID/{meeting_id}) から
    “投票”リンク/ボタンに紐づく RACEID を優先抽出。
    """
    url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{meeting_id}"
    ids: Set[str] = set()
    try:
        r = sess.get(url)
        if not r.ok or not r.text: return ids
        soup = BeautifulSoup(r.text, "html.parser")

        # aタグのテキストやボタン周辺に「投票」「オッズ」があるリンクを抽出
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True)
            href = a["href"]
            m = None
            for pat in RACE_LINK_PATTERNS:
                m = pat.search(href)
                if m: break
            if not m: continue
            rid = m.group(1)
            if not re.fullmatch(r"\d{18,}", rid): continue

            # “投票”“オッズ”という語、もしくはボタン風クラス名で判断
            cls = " ".join(a.get("class", []))
            neighbor = (a.find_next(string=True) or "") if not txt else ""
            if ("投票" in txt or "オッズ" in txt or
                "btn" in cls or "vote" in cls or "odds" in cls or
                ("投票" in neighbor) or ("オッズ" in neighbor)):
                ids.add(rid)

        # もし何も拾えなければ従来の抽出にフォールバック
        if not ids:
            ids = _extract_ids_from_html(r.text)
        return ids
    except Exception:
        return ids

def _tanfuku_looks_open(sess: requests.Session, rid: str) -> bool:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"
    try:
        r = sess.get(url)
        if not r.ok or not r.text: return False
        text = r.text
        if any(w in text for w in BLOCK_WORDS_COMMON): return False
        if ("単勝" not in text) and ("単勝オッズ" not in text): return False

        # 小数が1つ以上 & プレースホルダだらけではない
        if not _ODDS_NUM.search(text): return False
        soup = BeautifulSoup(text, "html.parser")
        nums = 0; ph = 0
        for td in soup.find_all("td"):
            t = td.get_text(strip=True)
            if _ODDS_NUM.fullmatch(t): nums += 1
            elif t in _PLACEHOLDER: ph += 1
        if nums == 0: return False
        total = nums + ph
        if total > 0 and ph/total >= 0.7: return False
        return True
    except Exception:
        return False

def _detail_says_open(sess: requests.Session, rid: str) -> bool:
    """
    race/detail の非JS要素で発売中を推定。
    - ブロック語が無い
    - 「投票」「締切」「オッズ更新」「R番」「発走」「時点」など＋時刻パターンが存在
    """
    url = f"https://keiba.rakuten.co.jp/race/detail/{rid}"
    try:
        r = sess.get(url)
        if not r.ok or not r.text: return False
        text = r.text
        if any(w in text for w in BLOCK_WORDS_COMMON): return False

        # キーワード＋時刻の組み合わせ
        HINTS = ("投票", "締切", "オッズ更新", "発走", "R", "時点")
        has_hint = any(h in text for h in HINTS)
        has_time = bool(_TIME_PAT.search(text))
        return has_hint and has_time
    except Exception:
        return False

def _is_open(sess: requests.Session, rid: str) -> bool:
    # tanfuku で開いている or detail で開いている → 発売中とみなす
    return _tanfuku_looks_open(sess, rid) or _detail_says_open(sess, rid)

# ===== メイン =====
def get_all_local_race_ids_today() -> List[str]:
    """
    トップ/一覧 → 開催日配下（list）→ detail/odds をたどって候補を収集。
    - 会場一覧で“投票リンクがある RACEID”を優先抽出
    - その後、tanfuku/detail で発売中チェック
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

    # 3) 会場一覧から “投票リンク有り” の RACEID を優先で収集
    open_from_list: Set[str] = set()
    for mid in list(meeting_ids)[:20]:  # 会場多い日も拾えるよう上限↑
        open_from_list |= _list_page_open_ids(sess, mid)
        time.sleep(0.08)

    race_level |= open_from_list

    # 4) 取りこぼし削減：detail/odds を軽くクロール
    peek = list(race_level)[:120]
    for rid in peek:
        for path in (f"https://keiba.rakuten.co.jp/race/detail/{rid}",
                     f"https://keiba.rakuten.co.jp/odds/{rid}"):
            race_level |= _extract_ids_from_url(sess, path)
            time.sleep(0.06)

    # 5) フォーマット整形（開催日ID除外）
    cleaned = sorted({
        i for i in race_level
        if re.fullmatch(r"\d{18,}", i) and not _is_meeting_id(i)
    })

    # 6) 発売中チェック
    validated: List[str] = []
    for rid in cleaned:
        if _is_open(sess, rid):
            validated.append(rid)
        time.sleep(0.05)

    return validated