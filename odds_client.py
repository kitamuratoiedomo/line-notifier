# odds_client.py
import re
import time
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
import os

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))

HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def _get(url: str, timeout: int = 12) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    # 楽天は UTF-8
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
    ids = [x.strip() for x in env.split(",") if x.strip()]
    return ids

def _parse_tanfuku_table(html: str) -> List[Dict[str, Any]]:
    """
    単勝オッズ表から [ {umaban:int, odds:float}, ... ] を抽出。
    人気順は odds 昇順で擬似決定（同値はそのままの順）。
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1) テーブル探索（クラス名は変わりやすいので柔軟に）
    # “単勝”の文字が近くにあるtableを優先
    tables = soup.find_all("table")
    candidates = []
    for t in tables:
        txt = (t.get_text(" ", strip=True) or "")
        if "単勝" in txt:
            candidates.append(t)
    if not candidates and tables:
        candidates = tables

    rows = []
    for t in candidates:
        for tr in t.find_all("tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if len(tds) < 3:
                continue
            # よくある並び: [馬番, 馬名, 単勝, 複勝...] など
            # 馬番（整数）と 単勝（小数 or “—”）を拾う
            umaban = None
            odds = None

            # 馬番候補
            for cell in tds[:2]:  # 先頭2セルに馬番があることが多い
                m = re.match(r"^\d{1,2}$", cell)
                if m:
                    umaban = int(m.group(0))
                    break

            # 単勝候補（小数 or 整数）
            for cell in tds:
                if re.match(r"^\d+(\.\d+)?$", cell):
                    # オッズにしては妙な巨大値は排除
                    val = float(cell)
                    if 1.0 <= val <= 999.9:
                        odds = val
                        break

            if umaban is not None and odds is not None:
                rows.append({"umaban": umaban, "odds": odds})

    # 重複排除＆ソート
    uniq = {}
    for r in rows:
        uniq[r["umaban"]] = r["odds"]
    out = [{"umaban": k, "odds": v} for k, v in uniq.items()]
    out.sort(key=lambda x: x["odds"])
    # 人気番号を付与
    for i, x in enumerate(out, 1):
        x["pop"] = i
    return out

def fetch_tanfuku_odds(race_id: str) -> Optional[Dict[str, Any]]:
    """
    単勝ページを取得して馬番×単勝オッズを返す。
    venue/race_no/start_at はプレースホルダか、分かる範囲で推測。
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

    # 画面上から場名やRを拾えれば拾う（簡易）
    venue = None
    m = re.search(r"（(.+?)）", html)  # ざっくり
    if m:
        venue = m.group(1)
    if not venue:
        venue = "地方"

    race_no = None
    m = re.search(r"(\d{1,2})R", html)
    if m:
        race_no = f"{m.group(1)}R"
    else:
        race_no = "—R"

    # 発走時刻が拾えないときは「この後10分」で仮置き（本番は別取得）
    start_at_iso = (datetime.now(JST) + timedelta(minutes=10)).isoformat()

    return {
        "race_id": race_id,
        "venue": venue,
        "race_no": race_no,
        "start_at_iso": start_at_iso,
        "horses": horses,  # [{umaban, odds, pop}, ...] 人気順
    }
    
# ===== get_race_start_iso =====================================================
from datetime import datetime, timezone, timedelta
import re

_JST = timezone(timedelta(hours=9))

def _to_iso_today_hhmm(hhmm: str) -> str:
    """'HH:MM' を 今日の日付の JST ISO8601 に変換"""
    today = datetime.now(_JST).strftime("%Y-%m-%d")
    return f"{today}T{hhmm}:00+09:00"

def get_race_start_iso(race_id: str) -> str:
    """
    レースID（例: 202508093230080102）から、JST の ISO8601 文字列を返す。
    取得順序:
      1) 既存の社内関数があればそれを利用（get_race_start_time / get_race_info）
      2) 楽天レースカードを軽量スクレイピングして '発走 HH:MM' を抽出
    失敗時は ValueError を送出
    """
    # 1) 既存関数の利用（プロジェクトによって名称が違う想定）
    try:
        _fn = globals().get("get_race_start_time")
        if callable(_fn):
            hhmm = _fn(race_id)  # 例: '10:25'
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

    # 2) 楽天のレースカードでスクレイピング
    urls = [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{race_id}",
        f"https://keiba.rakuten.co.jp/race/top/RACEID/{race_id}",
    ]
    pat = re.compile(r"(発走|出走)\s*([0-2]?\d:\d{2})")

    import requests  # ファイル先頭にあるなら自動的にそちらが使われます
    for url in urls:
        try:
            resp = requests.get(url, timeout=8)
            if resp.status_code != 200:
                continue
            m = pat.search(resp.text)
            if m:
                return _to_iso_today_hhmm(m.group(2))
        except Exception:
            continue

    raise ValueError(f"発走時刻を取得できませんでした: race_id={race_id}")