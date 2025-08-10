# odds_client.py  v2.2  (PC版HTML強制 + フォールバック強化)
import os
import re
import time
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))

BASE = "https://keiba.rakuten.co.jp"
HEADERS = {
    # PC版を必ず返させる
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
}

REQ_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "12"))
RETRY = 2


# ------------------ HTTP / Soup ------------------
def _get(url: str) -> Optional[str]:
    last_err = None
    for i in range(RETRY + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
            if r.status_code == 200 and r.text:
                return r.text
            last_err = f"status={r.status_code}"
        except Exception as e:
            last_err = str(e)
        time.sleep(0.6)
    logging.warning("GET失敗 url=%s err=%s", url, last_err)
    return None


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def _text(el) -> str:
    if not el:
        return ""
    return re.sub(r"\s+", " ", el.get_text(strip=True))


def _uniq_keep_order(xs: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in xs:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


# ------------------ RACEID 抽出系 ------------------
_re_raceid = re.compile(r"RACEID/(\d{17,20})")


def _extract_raceids_any(html: str) -> List[str]:
    """ページ内に現れる全ての RACEID を正規表現で吸い上げる"""
    return _uniq_keep_order(_re_raceid.findall(html or ""))


def _today_prefix() -> str:
    return datetime.now(JST).strftime("%Y%m%d")


def list_today_raceids() -> List[str]:
    """
    楽天競馬トップの「本日の発売情報」→ 各開催のレース一覧ページへ
    → そのページ内に出てくる全 RACEID を正規表現で抽出。
    取得できない場合にも出来るだけフォールバックして返す。
    """
    url_top = f"{BASE}/"
    html = _get(url_top)
    if not html:
        logging.info("楽天トップ取得失敗")
        return []

    soup = _soup(html)

    # 1) 「本日の発売情報」(#todaysTicket) から各開催のレース一覧リンクを拾う（PC版前提）
    venue_links: List[str] = []
    todays = soup.select("#todaysTicket a[href*='/race_card/list/']")
    for a in todays:
        href = a.get("href") or ""
        if "/race_card/list/RACEID/" in href:
            if href.startswith("/"):
                href = BASE + href
            venue_links.append(href)
    venue_links = _uniq_keep_order(venue_links)

    # フォールバック：ページ全体から list ページを拾う
    if not venue_links:
        for m in re.finditer(r'href="(/race_card/list/RACEID/\d+)"', html):
            venue_links.append(BASE + m.group(1))
        venue_links = _uniq_keep_order(venue_links)

    if not venue_links:
        logging.info("Rakutenスクレイピングで本日検出：0 件（開催見つからず）")
        return []

    # 2) 各「レース一覧」ページ内の全R RACEID を抽出
    today = _today_prefix()
    raceids: List[str] = []
    for vurl in venue_links:
        vhtml = _get(vurl)
        if not vhtml:
            continue
        ids = _extract_raceids_any(vhtml)
        # その開催日のものだけ（先頭8桁=YYYYMMDD）
        ids = [i for i in ids if i.startswith(today)]
        raceids.extend(ids)

    raceids = _uniq_keep_order(raceids)
    logging.info("Rakutenスクレイピングで本日検出：%d 件", len(raceids))
    return raceids


# ------------------ オッズ/レース情報 ------------------
def _tanfuku_url(race_id: str) -> str:
    return f"{BASE}/odds/tanfuku/RACEID/{race_id}"


def _racecard_url(race_id: str) -> str:
    return f"{BASE}/race_card/list/RACEID/{race_id}"


def _parse_start_time_and_meta(soup: BeautifulSoup) -> Dict[str, Any]:
    """見出しから開催日・場名・R番号、投票締切や発走時刻を抜く"""
    # 見出し h1: 「盛岡競馬場 7R オッズ」
    h1 = _text(soup.select_one("#headline h1"))
    venue, race_no = "", ""
    m = re.search(r"^(.+?)競馬場?\s+(\d+)R", h1)
    if m:
        venue = m.group(1)
        race_no = f"{m.group(2)}R"

    # 日付は .raceNote > .trackState の最初の <li> などに「2025年8月10日」
    date_li = _text(soup.select_one(".raceNote ul.trackState li"))
    ymd = None
    m2 = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", date_li)
    if m2:
        y, mo, d = map(int, m2.groups())
        ymd = datetime(y, mo, d, tzinfo=JST)

    # 発走時刻（無ければ None）
    start_txt = _text(soup.select_one(".trackMainState dt:contains('発走時刻') + dd")) \
                or _text(soup.select_one(".trackMainState dd"))  # 念のため
    # 例: 16:45
    hhmm = None
    m3 = re.search(r"(\d{1,2}):(\d{2})", start_txt)
    if m3 and ymd:
        hh, mm = int(m3.group(1)), int(m3.group(2))
        hhmm = ymd.replace(hour=hh, minute=mm, second=0, microsecond=0)

    start_iso = hhmm.isoformat() if hhmm else None
    return {"venue": venue or "", "race_no": race_no or "", "start_iso": start_iso}


def fetch_tanfuku_odds(race_id: str) -> Optional[Dict[str, Any]]:
    """
    単勝/複勝ページから:
      - 馬番, 単勝オッズ, 人気(pop)
    を取得。テーブルが空なら None を返す。
    返り値:
      {race_id, venue, race_no, start_at_iso, horses:[{umaban, odds, pop}]}
    """
    url = _tanfuku_url(race_id)
    html = _get(url)
    if not html:
        return None

    soup = _soup(html)

    # メタ（場名/レース番号/発走時刻）
    meta = _parse_start_time_and_meta(soup)

    rows = soup.select("#wakuUmaBanJun table.dataTable tbody tr")
    if not rows:
        # ページ下部の「人気順」テーブルで代替（順位→人気、馬番→number、単勝→win）
        alt_rows = soup.select("#ninkiKohaitoJun .rank table tbody tr")
        horses = []
        for tr in alt_rows:
            num = _text(tr.select_one("th.number"))
            win = _text(tr.select_one("td.win span"))
            pos = _text(tr.select_one("td.position"))
            if not num or not win or not pos:
                continue
            try:
                horses.append({
                    "umaban": int(num),
                    "odds": float(win),
                    "pop": int(pos),
                })
            except Exception:
                continue
        if not horses:
            logging.warning("単勝テーブル抽出に失敗（空） race_id=%s", race_id)
            return None

        horses.sort(key=lambda x: x["pop"])
        return {
            "race_id": race_id,
            "venue": meta.get("venue", ""),
            "race_no": meta.get("race_no", ""),
            "start_at_iso": meta.get("start_iso"),
            "horses": horses,
        }

    # 通常（枠・馬番順の単複テーブル）
    horses: List[Dict[str, Any]] = []
    for tr in rows:
        num = _text(tr.select_one("td.number"))  # 馬番
        win = _text(tr.select_one("td.oddsWin span"))
        rank_txt = _text(tr.select_one("td.rank"))
        if not num or not win:
            continue
        # rank 例: "1番人気"
        pop = None
        m = re.search(r"(\d+)番人気", rank_txt)
        if m:
            pop = int(m.group(1))
        try:
            horses.append({
                "umaban": int(num),
                "odds": float(win),
                "pop": pop if pop is not None else 999,
            })
        except Exception:
            continue

    if not horses:
        logging.warning("単勝テーブル抽出に失敗（行0） race_id=%s", race_id)
        return None

    # 人気が欠けていたらオッズで補正
    if any(h["pop"] == 999 for h in horses):
        horses_sorted = sorted(horses, key=lambda x: x["odds"])
        for i, h in enumerate(horses_sorted, start=1):
            h["pop"] = i
        # もとの順にも反映
        od2pop = {h["umaban"]: h["pop"] for h in horses_sorted}
        for h in horses:
            if h["pop"] == 999:
                h["pop"] = od2pop.get(h["umaban"], 999)

    # pop 昇順で並べておく
    horses.sort(key=lambda x: x["pop"])

    return {
        "race_id": race_id,
        "venue": meta.get("venue", ""),
        "race_no": meta.get("race_no", ""),
        "start_at_iso": meta.get("start_iso"),
        "horses": horses,
    }


def get_race_start_iso(race_id: str) -> Optional[str]:
    """
    オッズページから発走時刻（日付含む）をISO(+09:00)で返す
    """
    url = _tanfuku_url(race_id)
    html = _get(url)
    if not html:
        return None
    soup = _soup(html)
    meta = _parse_start_time_and_meta(soup)
    return meta.get("start_iso")