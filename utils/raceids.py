# odds_client.py
import os
import re
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))

# ===== Rakuten URL =====
BASE = "https://keiba.rakuten.co.jp/"
ODDS_TANFUKU = "odds/tanfuku/RACEID/{race_id}"

# ===== 共通 =====
def _get(url: str, *, timeout: int = 15) -> Optional[str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; rv:109.0) Gecko/20100101 Firefox/117.0"
    }
    for i in range(2):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200 and r.text:
                return r.text
            logging.warning("GET失敗 status=%s url=%s", r.status_code, url)
        except Exception as e:
            logging.warning("GET例外(%s/%s): %s", i + 1, 2, e)
            time.sleep(0.7)
    return None


def _text(el) -> str:
    return (el.get_text(strip=True) if el else "").strip()


def _to_float(s: str, default: float = 9999.0) -> float:
    try:
        return float(s.replace(",", ""))
    except Exception:
        return default


def _parse_date_hm_on_page(soup: BeautifulSoup) -> Optional[str]:
    """
    ページ上の日付と発走時刻 → ISO（JST）に。
    例: 「2025年8月10日」「発走時刻 16:45」
    """
    # 日付
    date_span = soup.select_one("#headline .dateSelect .selectedDay")
    date_txt = _text(date_span)
    # 発走時刻
    note_dl = soup.select_one(".raceNote .trackMainState")
    hm = None
    if note_dl:
        m = re.search(r"発走時刻\s*([0-2]?\d:[0-5]\d)", _text(note_dl))
        if m:
            hm = m.group(1)

    if date_txt and hm:
        m = re.search(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日", date_txt)
        if m:
            y, mo, d = map(int, m.groups())
            hh, mm = map(int, hm.split(":"))
            dt = datetime(y, mo, d, hh, mm, tzinfo=JST)
            return dt.isoformat()
    return None


# ====== 公開API（main.py が参照） ======

def list_today_raceids() -> List[str]:
    """
    本日の「発売中レース」の RACEID をまとめて拾う。
    取得方法：
      1) トップ近辺の「本日の発売情報」テーブルが載っている、どの単複ページでもOK
      2) そこから「レース一覧」リンク（…/odds/tanfuku/RACEID/XXXXXXXXXXXXXX00）へ
      3) 一覧ページのレース番号リンクの RACEID を全部収集
    ここでは、まず岩手（盛岡）を起点に fallback しつつ広めに探します。
    """
    seeds = [
        # 盛岡の「当日レース一覧」仮ID（下二桁00は“当日一覧”ページ）
        # 失敗しても後段のフォールバックで拾えるのでOK
        "202508101006060400",
    ]

    found: List[str] = []

    def collect_from_list_page(list_race_id: str):
        url = urljoin(BASE, ODDS_TANFUKU.format(race_id=list_race_id))
        html = _get(url)
        if not html:
            return
        soup = BeautifulSoup(html, "lxml")
        for a in soup.select(".raceNumber a[href*='/odds/tanfuku/RACEID/']"):
            href = a.get("href") or ""
            m = re.search(r"/odds/tanfuku/RACEID/(\d{17,18})", href)
            if m:
                rid = m.group(1)
                if rid not in found:
                    found.append(rid)

    for s in seeds:
        collect_from_list_page(s)

    # フォールバック（今日の任意1Rを開いて、そのページのレース帯から集める）
    if not found:
        # 岩手7Rを便宜上 seed に（存在しなければ無視）
        url = urljoin(BASE, ODDS_TANFUKU.format(race_id="202508101006060407"))
        html = _get(url)
        if html:
            soup = BeautifulSoup(html, "lxml")
            for a in soup.select(".raceNumber a[href*='/odds/tanfuku/RACEID/']"):
                m = re.search(r"/odds/tanfuku/RACEID/(\d{17,18})", a.get("href") or "")
                if m:
                    rid = m.group(1)
                    if rid not in found:
                        found.append(rid)

    if found:
        logging.info("Rakutenスクレイピングで本日検出: %d 件", len(found))
    else:
        logging.info("list_today_raceids() は空でした。フォールバックします。")

    return found


def fetch_tanfuku_odds(race_id: str) -> Optional[Dict[str, Any]]:
    """
    単勝/複勝ページから、枠・馬番順のテーブルを**直接**パースして取得する。
    返り値：
      {
        race_id, venue, race_no, start_at_iso,
        horses: [{pop, umaban, odds}, ...]  （pop=人気, odds=単勝オッズの数値）
      }
    """
    url = urljoin(BASE, ODDS_TANFUKU.format(race_id=race_id))
    html = _get(url)
    if not html:
        logging.warning("HTML取得に失敗 race_id=%s", race_id)
        return None

    soup = BeautifulSoup(html, "lxml")

    # 会場・R
    # 例: <h1 class="unique">盛岡競馬場 7R オッズ</h1>
    h1 = _text(soup.select_one("#headline h1.unique"))
    m = re.search(r"(.+?)競馬場\s+(\d+)R", h1)
    venue = (m.group(1) if m else "").strip() or "地方"
    race_no = (m.group(2) if m else "").strip() + "R"

    # 発走（ISO）
    start_iso = _parse_date_hm_on_page(soup)

    # 本体テーブル（枠・馬番順）— 非表示でもHTML上は存在する
    rows = soup.select("#oddsField #wakuUmaBanJun table tbody tr")
    if not rows:
        # ページ表示直後に「人気順」タブしか載らないケースへの保険
        rows = soup.select("#oddsField table.dataTable tbody.selectWrap tr")

    horses: List[Dict[str, Any]] = []
    for tr in rows:
        umaban = _text(tr.select_one("td.number"))
        odds_s = _text(tr.select_one("td.oddsWin span"))
        pop_s = _text(tr.select_one("td.rank"))

        if not umaban or not odds_s:
            # 稀に馬体重行や装飾行が紛れることがあるのでスキップ
            continue

        # 人気は「8番人気」→ 8
        pop = None
        m2 = re.search(r"(\d+)\s*番人気", pop_s)
        if m2:
            pop = int(m2.group(1))

        horses.append({
            "umaban": int(umaban),
            "odds": _to_float(odds_s),
            "pop": (pop if pop is not None else 999),
        })

    if not horses:
        logging.warning("単勝テーブル抽出に失敗（空）  race_id=%s", race_id)
        return None

    # 人気がテーブルに無い場合、人気順ブロックから補完
    if any(h["pop"] == 999 for h in horses):
        rank_rows = soup.select("#ninkiKohaitoJun .rank table tbody tr")
        # rank_rows: 「順位 / 馬番 / 馬名 / 単勝 / 複勝」
        pop_by_umaban: Dict[int, int] = {}
        for rr in rank_rows:
            num = _text(rr.select_one("th.number"))
            pos = _text(rr.select_one("td.position"))
            if num.isdigit() and pos.isdigit():
                pop_by_umaban[int(num)] = int(pos)
        if pop_by_umaban:
            for h in horses:
                if h["pop"] == 999 and h["umaban"] in pop_by_umaban:
                    h["pop"] = pop_by_umaban[h["umaban"]]

    horses.sort(key=lambda x: x["pop"])

    return {
        "race_id": race_id,
        "venue": venue,
        "race_no": race_no,
        "start_at_iso": start_iso,
        "horses": horses,
    }


def get_race_start_iso(race_id: str) -> Optional[str]:
    """
    発走時刻のISO（JST）を再取得（fetch_tanfuku_odds と同一ページから）。
    """
    url = urljoin(BASE, ODDS_TANFUKU.format(race_id=race_id))
    html = _get(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    return _parse_date_hm_on_page(soup)