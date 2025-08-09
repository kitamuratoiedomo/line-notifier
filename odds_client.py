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