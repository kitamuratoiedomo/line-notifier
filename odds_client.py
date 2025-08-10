# odds_client.py v2.1
# - 本日の発売情報テーブルから全開催のレース一覧を総当たり
# - 各レースの単勝/複勝オッズページから RACEID を抽出
# - 単勝オッズ・人気・馬番を抽出（人気順の表が無くても枠順表から拾える）
# - 発走日時を ISO(+09:00) で返却

from __future__ import annotations
import re
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; line-notifier/2.1; +https://example.invalid)",
    "Accept-Language": "ja,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "close",
    "Cache-Control": "no-cache",
})

BASE = "https://keiba.rakuten.co.jp"
JST = timezone(timedelta(hours=9))

_RACEID_RE = re.compile(r"/RACEID/(\d{18,20})")
_ODDS_TANFUKU_RE = re.compile(r"/odds/tanfuku/RACEID/(\d{18,20})")

def _get(url: str, timeout: int = 12) -> Optional[str]:
    try:
        r = SESSION.get(url, timeout=timeout)
        if r.status_code != 200:
            logging.warning("GET失敗 status=%s url=%s", r.status_code, url)
            return None
        r.encoding = r.apparent_encoding or "utf-8"
        return r.text
    except Exception as e:
        logging.warning("GET例外 url=%s err=%s", url, e)
        return None

def _uniq(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

# ---------- 1) 当日の全RACEID列挙 ----------
def list_today_raceids() -> List[str]:
    """
    楽天競馬トップの『本日の発売情報』から全開催のレース一覧を辿り、
    各レースの単勝/複勝オッズページに現れる RACEID を総当たりで収集する。
    """
    top_html = _get(f"{BASE}/")
    if not top_html:
        logging.info("トップページ取得失敗（フォールバック空）")
        return []

    # まず /race_card/list/RACEID/xxxxx へのリンクを全部拾う
    list_urls = []
    for m in re.finditer(r'href="(/race_card/list/RACEID/\d{18,20})"', top_html):
        list_urls.append(BASE + m.group(1))
    list_urls = _uniq(list_urls)

    if not list_urls:
        logging.info("本日の発売情報リンクが見つからず（フォールバック空）")
        return []

    race_ids: List[str] = []

    # 各「レース一覧」ページから /odds/tanfuku/RACEID/ を直接拾う
    for url in list_urls:
        html = _get(url)
        if not html:
            continue
        # そのページ内にすでにオッズページのリンクが埋まっていることがある
        for m in _ODDS_TANFUKU_RE.finditer(html):
            race_ids.append(m.group(1))

        # 念のため、ページ内に別の RACEID の一覧がある場合は、それらのページも辿って拾う
        # 例：レース番号の各リンク → その先のオッズページに RACEID がある
        sub_urls = []
        for sub in re.finditer(r'href="(/odds/tanfuku/RACEID/\d{18,20})"', html):
            sub_urls.append(BASE + sub.group(1))
        sub_urls = _uniq(sub_urls)

        # サブに飛んで RaceID を確実に収集
        for su in sub_urls:
            sm = _RACEID_RE.search(su)
            if sm:
                race_ids.append(sm.group(1))

        # まだ無い場合は、同ページ内の「RACEID/......00(レース一覧)」から
        # 末尾 01..12 を機械生成して GET→存在するものを確定収集（軽めに）
        if not any(url.find("/odds/tanfuku/") >= 0 for url in sub_urls):
            base_id_m = _RACEID_RE.search(url)
            if base_id_m:
                base_id = base_id_m.group(1)  # 例: ......00 (レース一覧)
                head = base_id[:-2]           # 末尾2桁がレース番号
                for rn in range(1, 13):
                    rid = f"{head}{rn:02d}"
                    odds_url = f"{BASE}/odds/tanfuku/RACEID/{rid}"
                    page = _get(odds_url)
                    if page and "オッズ" in page:
                        race_ids.append(rid)

    race_ids = _uniq(race_ids)
    logging.info("Rakutenスクレイピングで本日検出: %d 件", len(race_ids))
    return race_ids

# ---------- 2) レース基本情報 ----------
def _text(el) -> str:
    if not el:
        return ""
    return re.sub(r"\s+", "", el.get_text(strip=True))

def get_race_start_iso(race_id: str) -> Optional[str]:
    """
    単複オッズページから 日付 と 発走時刻 を拾って ISO(+09:00) にして返す
    """
    url = f"{BASE}/odds/tanfuku/RACEID/{race_id}"
    html = _get(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")

    # 日付（見出しやレース情報のところ）
    date_text = ""
    h1 = soup.select_one("#headline .dateSelect .selectedDay")
    if h1:
        date_text = _text(h1)  # 例: 2025年8月10日
    if not date_text:
        # 予備: メタタグ
        mt = soup.find("meta", attrs={"name": "title"})
        if mt and mt.get("content"):
            m2 = re.search(r"(\d{4})/(\d{2})/(\d{2})", mt["content"])
            if m2:
                date_text = f"{m2.group(1)}年{int(m2.group(2))}月{int(m2.group(3))}日"

    # 発走時刻
    time_text = ""
    for dl in soup.select(".raceNote .trackMainState dl"):
        label = _text(dl)
        m = re.search(r"発走時刻(\d{1,2}:\d{2})", label)
        if m:
            time_text = m.group(1)
            break

    if not (date_text and time_text):
        return None

    # 和暦表記をパース
    dm = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", date_text)
    if not dm:
        return None
    yyyy, mm, dd = int(dm.group(1)), int(dm.group(2)), int(dm.group(3))
    hh, mi = map(int, time_text.split(":"))
    dt = datetime(yyyy, mm, dd, hh, mi, tzinfo=JST)
    return dt.isoformat()

# ---------- 3) 単勝オッズの抽出 ----------
def fetch_tanfuku_odds(race_id: str) -> Optional[Dict[str, Any]]:
    """
    返却: {
      race_id, venue, race_no, start_at_iso?,
      horses: [{pop, umaban, odds}]
    }
    """
    url = f"{BASE}/odds/tanfuku/RACEID/{race_id}"
    html = _get(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")

    # 開催・レース番号
    venue = "地方"
    race_no = ""
    rt = soup.select_one(".raceTitle .placeNumber")
    if rt:
        v = rt.select_one(".racePlace")
        n = rt.select_one(".raceNumber .num")
        venue = _text(v) or venue
        race_no = (_text(n) + "R") if n else ""

    # 馬ごとの 単勝・人気・馬番
    horses: List[Dict[str, Any]] = []

    # 1) 人気順表（ある方が取りやすい）
    rows = soup.select("#ninkiKohaitoJun .rank table tbody tr")
    if rows:
        for tr in rows:
            num = _text(tr.select_one("th.number"))
            odds = _text(tr.select_one("td.win span"))
            pop = _text(tr.select_one("td.position"))
            try:
                if num and odds:
                    horses.append({
                        "umaban": int(num),
                        "odds": float(odds),
                        "pop": int(pop) if pop else 999,
                    })
            except Exception:
                pass

    # 2) 人気順がなくても、枠番順の表から拾う（人気は「td.rank」に “X番人気”）
    if not horses:
        for tr in soup.select("#wakuUmaBanJun table tbody tr"):
            num = _text(tr.select_one("td.number"))
            odds = _text(tr.select_one("td.oddsWin span"))
            rank_txt = _text(tr.select_one("td.rank"))
            pop_val = None
            m = re.search(r"(\d+)番人気", rank_txt or "")
            if m:
                pop_val = int(m.group(1))
            try:
                if num and odds:
                    horses.append({
                        "umaban": int(num),
                        "odds": float(odds),
                        "pop": pop_val if pop_val is not None else 999,
                    })
            except Exception:
                pass

    if not horses:
        logging.warning("単勝テーブル抽出に失敗（空）  race_id=%s", race_id)
        return None

    # レース開始の ISO（無くても上位層が再取得するので任意）
    start_iso = get_race_start_iso(race_id)

    return {
        "race_id": race_id,
        "venue": venue,
        "race_no": race_no,
        "start_at_iso": start_iso,
        "horses": horses,
    }