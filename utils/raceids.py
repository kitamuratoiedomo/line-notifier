# utils/raceids.py — 本日の「地方競馬・全レース」RACEIDを安全取得
# v2.0: JS後描画でも拾えるよう、単勝オッズページ内の「埋め込みJSON」を直接解析して発売中判定
from __future__ import annotations

import re
import json
import time
import datetime as dt
from typing import List, Set, Iterable, Any, Dict

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

# ===== 時刻・HTTP =====
JST = dt.timezone(dt.timedelta(hours=9))
USER_AGENT = "Mozilla/5.0 (compatible; LocalKeibaNotifier/2.0)"
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

# ===== 発売中判定（JSON抽出 → detail補完 → テーブル保険） =====
_ODDS_NUM = re.compile(r"\b\d{1,3}\.\d{1,2}\b")
_TIME_PAT = re.compile(r"\b(?:[01]?\d|2[0-3]):[0-5]\d\b")
_PLACEHOLDER = {"--", "—", "-", "0.0", "0", ""}

BLOCK_WORDS_COMMON = (
    "発売開始前", "発売前", "ただいま集計中", "集計中",
    "発売は締め切りました", "投票は締め切りました",
    "オッズ情報はありません", "発売中止",
)

def _pick_script_json_blobs(html: str) -> List[str]:
    """
    ページに埋め込まれたJSONらしき文字列を抽出。
    Nuxt/Next/初期データなど広めに拾う。
    """
    blobs: List[str] = []
    soup = BeautifulSoup(html, "html.parser")
    for sc in soup.find_all("script"):
        txt = sc.string or sc.get_text() or ""
        if not txt:
            continue
        # 典型的なキー語
        if any(k in txt for k in ("__NUXT__", "__NEXT_DATA__", "initialData", "odds", "tanfuku")):
            # JSONっぽい先頭/末尾をざっくり切り出す
            # ブラウザ向けに window.__NUXT__= {...}; のような形式を想定
            m = re.search(r"(\{.*\})", txt, re.DOTALL)
            if m:
                blobs.append(m.group(1))
    return blobs

def _json_find_odds_arrays(obj: Any) -> int:
    """
    ネストした辞書/配列をたどり、単勝オッズらしき小数の配列件数をカウント
    """
    count = 0
    def walk(x: Any):
        nonlocal count
        if isinstance(x, dict):
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            # 小数文字列が複数ある配列
            odds_like = 0
            for v in x:
                s = str(v)
                if _ODDS_NUM.fullmatch(s):
                    odds_like += 1
            if odds_like >= 3:
                count += 1
            for v in x:
                walk(v)
    walk(obj)
    return count

def _tanfuku_json_says_open(sess: requests.Session, rid: str) -> bool:
    """
    単勝オッズページの埋め込みJSONから発売中を判断。
    - ブロック語がHTMLに無い
    - JSONが1つ以上パースでき、その中で “オッズらしき配列” が見つかる
    """
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"
    try:
        r = sess.get(url)
        if not r.ok or not r.text: return False
        html = r.text
        if any(w in html for w in BLOCK_WORDS_COMMON): return False
        blobs = _pick_script_json_blobs(html)
        for b in blobs:
            try:
                obj = json.loads(b)
            except Exception:
                # JSON5風/末尾カンマ等で失敗したら軽く整形して再挑戦
                b2 = re.sub(r",\s*}", "}", re.sub(r",\s*]", "]", b))
                try:
                    obj = json.loads(b2)
                except Exception:
                    continue
            if _json_find_odds_arrays(obj) > 0:
                return True
        return False
    except Exception:
        return False

def _detail_says_open(sess: requests.Session, rid: str) -> bool:
    url = f"https://keiba.rakuten.co.jp/race/detail/{rid}"
    try:
        r = sess.get(url)
        if not r.ok or not r.text: return False
        text = r.text
        if any(w in text for w in BLOCK_WORDS_COMMON): return False
        HINTS = ("投票", "締切", "オッズ更新", "発走", "R", "時点")
        return any(h in text for h in HINTS) and bool(_TIME_PAT.search(text))
    except Exception:
        return False

def _table_looks_open(sess: requests.Session, rid: str) -> bool:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"
    try:
        r = sess.get(url)
        if not r.ok or not r.text: return False
        text = r.text
        if any(w in text for w in BLOCK_WORDS_COMMON): return False
        soup = BeautifulSoup(text, "html.parser")
        tds = [td.get_text(strip=True) for td in soup.find_all("td")]
        if not tds: return False
        nums = sum(1 for t in tds if _ODDS_NUM.fullmatch(t))
        ph   = sum(1 for t in tds if t in _PLACEHOLDER)
        if nums == 0: return False
        total = nums + ph
        return not (total > 0 and ph/total >= 0.7)
    except Exception:
        return False

def _is_open(sess: requests.Session, rid: str) -> bool:
    # 1) 埋め込みJSON → 2) detail → 3) テーブル保険
    return _tanfuku_json_says_open(sess, rid) or _detail_says_open(sess, rid) or _table_looks_open(sess, rid)

# ===== メイン =====
def get_all_local_race_ids_today() -> List[str]:
    """
    トップ/一覧 → 開催日配下（list）→ detail/odds をたどって候補を収集。
    最後に “発売中（JSON/ detail / table で判定）” のIDのみ返す。
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
    for mid in list(meeting_ids)[:20]:
        list_url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{mid}"
        race_level |= _extract_ids_from_url(sess, list_url)
        time.sleep(0.08)

    # 4) 取りこぼし削減：detail/odds を軽くクロール
    peek = list(race_level)[:120]
    for rid in peek:
        for path in (f"https://keiba.rakuten.co.jp/race/detail/{rid}",
                     f"https://keiba.rakuten.co.jp/odds/{rid}"):
            race_level |= _extract_ids_from_url(sess, path)
            time.sleep(0.06)

    # 5) 形式面でクリーニング（開催日ID除外）
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