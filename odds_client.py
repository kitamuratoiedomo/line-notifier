# odds_client.py ー 全部差し替え版
import os
import re
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))
HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def _get(url: str, timeout: int = 12) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


def list_today_raceids() -> List[str]:
    """
    簡易版：環境変数 RACEIDS にカンマ区切りで race_id を渡しておく想定。
    例）RACEIDS=202508073601090301,202508072726110201
    """
    env = os.getenv("RACEIDS", "").strip()
    if not env:
        logging.info("RACEIDS が未設定のため、レース列挙をスキップ")
        return []
    return [x.strip() for x in env.split(",") if x.strip()]


# ----------------- 単勝オッズ抽出 -----------------
def _parse_tanfuku_table(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    candidates = [t for t in tables if "単勝" in (t.get_text(" ", strip=True) or "")]
    if not candidates and tables:
        candidates = tables

    rows = []
    for t in candidates:
        for tr in t.find_all("tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if len(tds) < 3:
                continue

            umaban = None
            odds = None

            # 馬番（先頭2セル内によくある）
            for cell in tds[:2]:
                if re.fullmatch(r"\d{1,2}", cell):
                    umaban = int(cell)
                    break
            # 単勝オッズらしい数値
            for cell in tds:
                if re.fullmatch(r"\d+(\.\d+)?", cell):
                    val = float(cell)
                    if 1.0 <= val <= 999.9:
                        odds = val
                        break
            if umaban is not None and odds is not None:
                rows.append({"umaban": umaban, "odds": odds})

    # 重複排除→オッズ昇順
    uniq = {}
    for r in rows:
        uniq[r["umaban"]] = r["odds"]
    out = [{"umaban": k, "odds": v} for k, v in uniq.items()]
    out.sort(key=lambda x: x["odds"])
    for i, x in enumerate(out, 1):
        x["pop"] = i
    return out


def fetch_tanfuku_odds(race_id: str) -> Optional[Dict[str, Any]]:
    """
    単勝ページを取得して馬番×単勝オッズを返す。
    venue/race_no/start_at は推測（発走は別関数で取得推奨）。
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

    # ざっくり場名/R
    venue = re.search(r"（(.+?)）", html)
    venue = venue.group(1) if venue else "地方"
    rno = re.search(r"(\d{1,2})R", html)
    race_no = f"{rno.group(1)}R" if rno else "—R"

    # 仮の発走（実際は get_race_start_iso を使用）
    start_at_iso = (datetime.now(JST) + timedelta(minutes=10)).isoformat()

    return {
        "race_id": race_id,
        "venue": venue,
        "race_no": race_no,
        "start_at_iso": start_at_iso,
        "horses": horses,  # [{umaban, odds, pop}, ...]
    }


# ----------------- 時刻系（発走・販売締切） -----------------
def _to_iso_today_hhmm(hhmm: str) -> str:
    """'HH:MM' を 今日の日付の JST ISO8601 に変換"""
    today = datetime.now(JST).strftime("%Y-%m-%d")
    return f"{today}T{hhmm}:00+09:00"


def get_race_start_iso(race_id: str) -> str:
    """
    レースIDから JST の発走時刻(ISO8601)を返す。
    取得順序:
      1) 既存の社内関数（存在すれば）: get_race_start_time / get_race_info
      2) 楽天のレースカードを軽量スクレイピングして '発走 HH:MM'
    """
    # 1) 既存関数
    try:
        _fn = globals().get("get_race_start_time")
        if callable(_fn):
            hhmm = _fn(race_id)
            if isinstance(hhmm, str) and re.fullmatch(r"\d{1,2}:\d{2}", hhmm):
                return _to_iso_today_hhmm(hhmm)
    except Exception:
        pass
    try:
        _fn = globals().get("get_race_info")
        if callable(_fn):
            info = _fn(race_id) or {}
            hhmm = (info.get("start_time") or info.get("post_time") or "").strip()
            if isinstance(hhmm, str) and re.fullmatch(r"\d{1,2}:\d{2}", hhmm):
                return _to_iso_today_hhmm(hhmm)
    except Exception:
        pass

    # 2) 楽天スクレイピング
    urls = [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{race_id}",
        f"https://keiba.rakuten.co.jp/race/top/RACEID/{race_id}",
    ]
    pat = re.compile(r"(発走|出走)\s*([0-2]?\d:\d{2})")
    for url in urls:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=8)
            if resp.status_code != 200:
                continue
            m = pat.search(resp.text)
            if m:
                return _to_iso_today_hhmm(m.group(2))
        except Exception:
            continue

    raise ValueError(f"発走時刻を取得できませんでした: race_id={race_id}")


def get_sale_close_iso(race_id: str) -> str:
    """
    販売締切（ネット投票締切）の JST ISO8601 を返す。
    方法②: 発走時刻から 5 分前を締切とみなす。
    """
    start_iso = get_race_start_iso(race_id)  # ここで ValueError を投げることあり
    dt = datetime.fromisoformat(start_iso)   # +09:00 を保持したまま
    close_dt = dt - timedelta(minutes=5)
    return close_dt.isoformat()