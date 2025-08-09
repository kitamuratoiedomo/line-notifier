# odds_client.py
import os
import re
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))
HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


# ------------------------------
# 汎用
# ------------------------------
def _get(url: str, timeout: int = 12) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


def list_today_raceids() -> List[str]:
    """
    まずは環境変数 RACEIDS（カンマ区切り）を優先。
    空なら自動探索は行わず、空リストを返す（明日の自動全レースは別途実装予定）。
    """
    env = (os.getenv("RACEIDS") or "").strip()
    if not env:
        logging.info("RACEIDS が未設定のため、レース列挙をスキップ")
        return []
    return [x.strip() for x in env.split(",") if x.strip()]


# ------------------------------
# 単勝ページのスクレイピング
# ------------------------------
def _parse_tanfuku_table(html: str) -> List[Dict[str, Any]]:
    """
    単勝表から [{umaban:int, odds:float, pop:int}, ...] を生成（人気=オッズ昇順）
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for tr in soup.find_all("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(tds) < 3:
            continue

        umaban, odds = None, None

        # 馬番候補（先頭2セルに整数が出ることが多い）
        for cell in tds[:2]:
            if re.fullmatch(r"\d{1,2}", cell):
                umaban = int(cell)
                break

        # 単勝（1.0〜999.9 の数値）
        for cell in tds:
            if re.fullmatch(r"\d+(\.\d+)?", cell):
                v = float(cell)
                if 1.0 <= v <= 999.9:
                    odds = v
                    break

        if umaban is not None and odds is not None:
            rows.append({"umaban": umaban, "odds": odds})

    # 重複排除 & 人気付与
    uniq = {}
    for r in rows:
        uniq[r["umaban"]] = r["odds"]

    out = [{"umaban": k, "odds": v} for k, v in uniq.items()]
    out.sort(key=lambda x: x["odds"])
    for i, x in enumerate(out, 1):
        x["pop"] = i
    return out


def _parse_venue_rno(html: str) -> Dict[str, str]:
    """
    ざっくり場名・レース番号らしき文字を拾う（精度はほどほど）。
    """
    txt = html
    venue = None
    rno = None

    # 例: "佐賀競馬場 8R" / "佐賀 8R" など
    m_r = re.search(r"([0-2]?\d)R", txt)
    if m_r:
        rno = f"{int(m_r.group(1))}R"

    m_v = re.search(r"(門別|盛岡|水沢|浦和|船橋|大井|川崎|名古屋|笠松|金沢|園田|姫路|高知|佐賀|帯広|ばんえい).{0,3}(競馬場)?", txt)
    if m_v:
        venue = m_v.group(0).replace("競馬場", "").strip()

    return {"venue": venue or "地方", "race_no": rno or "—R"}


def fetch_tanfuku_odds(race_id: str) -> Optional[Dict[str, Any]]:
    """
    単勝オッズを取得して、人気順リストを返す。
    さらに場名・レース番号も（拾えれば）付与。
    """
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    try:
        html = _get(url)
    except Exception as e:
        logging.warning("odds取得失敗 %s: %s", race_id, e)
        return None

    horses = _parse_tanfuku_table(html)
    if not horses:
        logging.warning("単勝テーブル抽出に失敗（空） race_id=%s", race_id)
        return None

    meta = _parse_venue_rno(html)
    return {
        "race_id": race_id,
        "venue": meta["venue"],
        "race_no": meta["race_no"],
        "horses": horses,
        "odds_url": url,
    }


# ------------------------------
# 時刻（発走・販売締切）
# ------------------------------
def _to_iso_today_hhmm(hhmm: str) -> str:
    today = datetime.now(JST).strftime("%Y-%m-%d")
    return f"{today}T{hhmm}:00+09:00"


def get_race_start_iso(race_id: str) -> str:
    """
    楽天のレースページから『発走 HH:MM』等を拾って JST ISO8601 文字列で返す
    """
    urls = [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{race_id}",
        f"https://keiba.rakuten.co.jp/race/top/RACEID/{race_id}",
    ]
    pat = re.compile(r"(発走|出走)\s*([0-2]?\d:\d{2})")
    for url in urls:
        try:
            html = _get(url, timeout=8)
            m = pat.search(html)
            if m:
                return _to_iso_today_hhmm(m.group(2))
        except Exception:
            continue
    raise ValueError(f"発走時刻を取得できませんでした: race_id={race_id}")


def get_sale_close_iso(race_id: str) -> str:
    """
    ネット販売の締切は『発走5分前』想定。
    """
    start_iso = get_race_start_iso(race_id)
    dt = datetime.fromisoformat(start_iso)
    return (dt - timedelta(minutes=5)).isoformat()