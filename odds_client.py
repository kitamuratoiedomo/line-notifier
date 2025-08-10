# odds_client.py  v2.3  — 今日のRACEIDを堅牢に収集する版
import re
import time
from typing import List, Set, Tuple
import requests
from urllib.parse import urljoin

BASE = "https://keiba.rakuten.co.jp/"
UA_PC = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT = 15
RETRY = 3
SLEEP_BETWEEN = 0.8

# 正規表現（頑強にマッチさせる）
RE_RACEID_IN_URL = re.compile(r"/RACEID/(\d{18})")
RE_ODDS_TANFUKU = re.compile(r"/odds/tanfuku/RACEID/(\d{18})")
RE_RACECARD_LIST = re.compile(r"/race_card/list/RACEID/(\d{18})")


def _fetch(url: str) -> Tuple[int, str]:
    """GETして(status_code, text)を返す。軽いリトライ付き"""
    last_err = None
    for i in range(RETRY):
        try:
            r = requests.get(
                url,
                headers={"User-Agent": UA_PC, "Accept-Language": "ja,en;q=0.8"},
                timeout=DEFAULT_TIMEOUT,
            )
            if r.status_code == 200 and r.text:
                return r.status_code, r.text
            last_err = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last_err = str(e)
        time.sleep(0.6 + 0.2 * i)
    raise RuntimeError(f"fetch failed: {url} ({last_err})")


def _extract_racecard_base_ids_from_top(html: str) -> Set[str]:
    """
    トップページ #todaysTicket テーブルから 'race_card/list/RACEID/xxxxxxxxxxxxxx00'
    を根こそぎ拾う。念のためページ全体からも拾う。
    """
    base_ids: Set[str] = set()

    # セクション優先抽出（無ければ全体走査）
    section_match = re.search(r'id="todaysTicket".*?</table>', html, re.S | re.I)
    target_html = section_match.group(0) if section_match else html

    for m in RE_RACECARD_LIST.finditer(target_html):
        rid = m.group(1)
        # 「当日レース一覧」は末尾が00（例：...060400）
        base_ids.add(rid[:-2] + "00")

    # 念のため重複拾い
    if not base_ids:
        for m in RE_RACECARD_LIST.finditer(html):
            rid = m.group(1)
            base_ids.add(rid[:-2] + "00")

    return base_ids


def _extract_today_odds_ids_from_racecard(html: str) -> Set[str]:
    """
    出馬表（レース一覧）ページから、その日の単勝/複勝オッズへのRACEIDを全部拾う。
    """
    ids: Set[str] = set()
    for m in RE_ODDS_TANFUKU.finditer(html):
        ids.add(m.group(1))
    # もしオッズURLが見つからない場合は、レースカード内のRACEIDから補完（末尾**を01..12に試行）
    if not ids:
        # race_card/list内の任意RACEIDを拾う
        any_ids = {m.group(1) for m in RE_RACEID_IN_URL.finditer(html)}
        for rid in any_ids:
            # 末尾2桁がレース番号。01〜12を試し、存在確認は呼び出し側で行う
            prefix = rid[:-2]
            for n in range(1, 13):
                ids.add(f"{prefix}{n:02d}")
    return ids


def list_today_raceids() -> List[str]:
    """
    今日（JST）の『発売中』場をトップから検出し、
    各場のレース一覧ページ→単勝/複勝オッズのRACEIDを列挙して返す。
    """
    # 1) トップページ
    _, top_html = _fetch(BASE)

    base_ids = _extract_racecard_base_ids_from_top(top_html)

    # フォールバック：何も拾えない時は全ページからrace_card/listのRACEIDを総なめ
    if not base_ids:
        base_ids = _extract_racecard_base_ids_from_top(top_html)

    raceids: Set[str] = set()
    for base_id in sorted(base_ids):
        # レース一覧（出馬表）ページ
        list_url = urljoin(BASE, f"race_card/list/RACEID/{base_id}")
        try:
            _, list_html = _fetch(list_url)
        except Exception:
            time.sleep(SLEEP_BETWEEN)
            continue

        # 2) レース一覧ページから当日のオッズRACEIDを収集
        cand_ids = _extract_today_odds_ids_from_racecard(list_html)

        # 3) 収集したオッズRACEIDの存在を軽くヘルスチェック（HEAD代わりにGET 1回だけ）
        for rid in sorted(cand_ids):
            odds_url = urljoin(BASE, f"odds/tanfuku/RACEID/{rid}")
            try:
                status, _ = _fetch(odds_url)
                if status == 200:
                    raceids.add(rid)
                    # サーバーへの負荷を下げる
                    time.sleep(SLEEP_BETWEEN)
            except Exception:
                # 存在しない組はスキップ
                time.sleep(0.15)

        time.sleep(SLEEP_BETWEEN)

    # 末尾重複もあり得るので数字順に
    return sorted(raceids)


# 既存コードとの互換：PC版HTMLを強制するためのヘルパ（必要なら呼び出し側で使用）
def force_pc_headers(session: requests.Session):
    session.headers.update({"User-Agent": UA_PC, "Accept-Language": "ja,en;q=0.8"})