# utils/raceids.py — 本日の「地方競馬・全レース」RACEIDを安全取得（厳密検証 v1.5）
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
USER_AGENT = "Mozilla/5.0 (compatible; LocalKeibaNotifier/1.5)"
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
# 開催日ID（末尾10桁が0）例: 20250810 + 0000000000
MEETING_SUFFIX = re.compile(r"\d{8}0{10}$")
def _is_meeting_id(rid: str) -> bool:
    return bool(MEETING_SUFFIX.fullmatch(rid))

# ===== 抽出ユーティリティ =====
def _extract_ids_from_html(html: str) -> Set[str]:
    """HTMLから RACEID 候補を収集（href優先＋本文保険）"""
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
    """多くのIDは先頭にYYYYMMDDを含む→当日優先。無ければそのまま返す。"""
    today_ids = {i for i in ids if i.startswith(today)}
    return today_ids if today_ids else set(ids)

# ===== 単勝オッズページの「準備完了」判定 =====
# 小数（1〜3桁.1〜2桁）を複数検出する
_ODDS_NUM = re.compile(r"\b\d{1,3}\.\d{1,2}\b")
# 発売前・集計中・締切・中止など、除外したい語
_BLOCK_WORDS = (
    "発売前", "発売開始前", "ただいま集計中", "集計中",
    "発売は締め切りました", "投票は締め切りました",
    "オッズ情報はありません", "発売中止"
)
_PLACEHOLDER = {"--", "—", "-", "0.0", "0", ""}

def _table_ready_check(html: str) -> bool:
    """
    テーブル行を確認して“発売前の骨格だけ”を排除。
    - 馬行(tr)の数が十分（>=6）
    - オッズ数値セルの数が >=3
    - プレースホルダ（--, 0.0 等）ばかりではない（比率 < 0.5）
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return False

    # '単勝' を含むテーブルを優先。無ければ最初のテーブル。
    target = None
    for tb in tables:
        if "単勝" in tb.get_text() or "単勝オッズ" in tb.get_text():
            target = tb
            break
    if target is None:
        target = tables[0]

    rows = target.find_all("tr")
    # 見出しを除いた実データ行の概算（tdが2つ以上あるもの）
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

def _is_tanfuku_ready(sess: requests.Session, rid: str) -> bool:
    """
    単勝オッズページが実体を持ち、表が埋まっているかを判定。
    - ブロック語（発売前/締切/未提供/集計中/中止）を含んでいたら不可
    - '単勝' または '単勝オッズ' を含む
    - オッズらしき数値が複数（>=3）かつ、全て同一値の羅列ではない
    - <table> 検査（行数・プレースホルダ比率）に合格
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
        if len(set(nums)) == 1:  # ダミーの同一値羅列はNG
            return False

        if not _table_ready_check(text):
            return False

        return True
    except Exception:
        return False

# ===== メイン関数 =====
def get_all_local_race_ids_today() -> List[str]:
    """
    トップ/一覧 → 開催日配下 → detail/odds をたどって候補を収集。
    最後に “単勝オッズページが準備完了” のIDのみ返す。
    失敗時は空リスト。
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