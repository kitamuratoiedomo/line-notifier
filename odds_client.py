# odds_client.py
from __future__ import annotations

import os
import re
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

# ===== 共通 =====
JST = timezone(timedelta(hours=9))
HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def _get(url: str, timeout: int = 12) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


# ===== レースID列挙（簡易版） =====
def list_today_raceids() -> List[str]:
    """
    簡易：環境変数 RACEIDS にカンマ区切りで race_id を渡す。
    例）RACEIDS=202508093230080102,202508093230080201
    """
    env = os.getenv("RACEIDS", "").strip()
    if not env:
        logging.info("RACEIDS が未設定のため、レース列挙をスキップ")
        return []
    return [x.strip() for x in env.split(",") if x.strip()]


# ===== 単勝オッズ抽出 =====
def _parse_tanfuku_table(html: str) -> List[Dict[str, Any]]:
    """
    単勝オッズ表から [{umaban:int, odds:float, pop:int}, ...] を作る。
    """
    soup = BeautifulSoup(html, "html.parser")

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

            umaban = None
            odds = None

            # 馬番候補（先頭付近）
            for cell in tds[:2]:
                if re.fullmatch(r"\d{1,2}", cell):
                    umaban = int(cell)
                    break

            # 単勝候補（数字）
            for cell in tds:
                if re.fullmatch(r"\d+(\.\d+)?", cell):
                    val = float(cell)
                    if 1.0 <= val <= 999.9:
                        odds = val
                        break

            if umaban is not None and odds is not None:
                rows.append({"umaban": umaban, "odds": odds})

    # 重複排除＆人気順付与
    uniq: Dict[int, float] = {}
    for r in rows:
        uniq[r["umaban"]] = r["odds"]
    out = [{"umaban": k, "odds": v} for k, v in uniq.items()]
    out.sort(key=lambda x: x["odds"])
    for i, x in enumerate(out, 1):
        x["pop"] = i
    return out


def fetch_tanfuku_odds(race_id: str) -> Optional[Dict[str, Any]]:
    """
    単勝ページから馬番×単勝オッズ（人気順付き）を取得。
    venue/race_no/start_at_iso は分かる範囲で推測。
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

    # 画面上から場名やRをざっくり拾う
    venue = "地方"
    m = re.search(r"（(.+?)）", html)
    if m:
        venue = m.group(1)

    race_no = "—R"
    m = re.search(r"(\d{1,2})R", html)
    if m:
        race_no = f"{m.group(1)}R"

    # 発走時刻が分からない場合は仮で +10分（本番は get_race_start_iso を使う）
    start_at_iso = (datetime.now(JST) + timedelta(minutes=10)).isoformat()

    return {
        "race_id": race_id,
        "venue": venue,
        "race_no": race_no,
        "start_at_iso": start_at_iso,
        "horses": horses,
    }


# ===== 発走時刻取得（ISO8601） =====
def _to_iso_today_hhmm(hhmm: str) -> str:
    """'HH:MM' -> 今日のJST ISO8601"""
    today = datetime.now(JST).strftime("%Y-%m-%d")
    return f"{today}T{hhmm}:00+09:00"


def get_race_start_iso(race_id: str) -> str:
    """
    レースIDから JST の ISO8601 文字列を返す。
    手順:
      1) 既存の社内関数(get_race_start_time / get_race_info)があれば利用
      2) 楽天のレースカードから '発走 HH:MM' をスクレイピング
    見つからなければ ValueError
    """
    # 1) 既存関数の利用（あれば）
    try:
        _fn = globals().get("get_race_start_time")
        if callable(_fn):
            hhmm = _fn(race_id)  # 例 '10:25'
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

    # 2) 楽天から取得
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


# ===== レースタイトル取得 =====
def get_race_title(race_id: str) -> str:
    """
    楽天競馬のレースタイトルを取得。
    例: '佐賀 2R C2-9組' 等。取得できなければ 'レース名不明'
    """
    urls = [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{race_id}",
        f"https://keiba.rakuten.co.jp/race/top/RACEID/{race_id}",
    ]
    for url in urls:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=8)
            if resp.status_code != 200:
                continue
            # <title>… - 楽天競馬</title>
            m = re.search(r"<title>(.+?)\s*-\s*楽天競馬</title>", resp.text)
            if m:
                return m.group(1).strip()
            # 予備: 見出し h1/h2
            m = re.search(r"<h[12][^>]*>(.+?)</h[12]>", resp.text)
            if m:
                return BeautifulSoup(m.group(0), "html.parser").get_text(" ", strip=True)
        except Exception:
            continue
    return "レース名不明"