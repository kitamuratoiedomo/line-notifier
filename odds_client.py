# odds_client.py  — 全部差し替え版
import os
import re
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))

HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

# ---------------------------------------------------------------------------
# 共通
# ---------------------------------------------------------------------------
def _get(url: str, timeout: int = 12) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

def _yyyymmdd_from_raceid(race_id: str) -> Optional[str]:
    """race_id 先頭の日付（YYYYMMDD）を返す。無ければ None。"""
    m = re.match(r"^(\d{8})", race_id or "")
    return m.group(1) if m else None

def _to_iso(date_yyyymmdd: str, hhmm: str) -> str:
    """YYYYMMDD と HH:MM から JST の ISO8601 を作る。"""
    dt = datetime.strptime(f"{date_yyyymmdd} {hhmm}", "%Y%m%d %H:%M").replace(tzinfo=JST)
    return dt.isoformat()

# ---------------------------------------------------------------------------
# レース列挙（環境変数から）
# ---------------------------------------------------------------------------
def list_today_raceids() -> List[str]:
    """
    簡易版：環境変数 RACEIDS にカンマ区切りで race_id を渡しておく想定。
    例）RACEIDS=202508093230080101,202508093230080102
    """
    env = os.getenv("RACEIDS", "").strip()
    if not env:
        logging.info("RACEIDS が未設定のため、レース列挙をスキップ")
        return []
    return [x.strip() for x in env.split(",") if x.strip()]

# ---------------------------------------------------------------------------
# 単勝オッズ取得（人気順も付与）
# ---------------------------------------------------------------------------
def _parse_tanfuku_table(html: str) -> List[Dict[str, Any]]:
    """
    単勝オッズ表から [{umaban:int, odds:float, pop:int}, ...] を抽出。
    ※ “表の構造ゆれ” に強めの緩い抽出を行い、odds 昇順で人気付与。
    """
    soup = BeautifulSoup(html, "html.parser")

    # 単勝の表に見える table を候補に
    tables = soup.find_all("table")
    candidates = []
    for t in tables:
        txt = (t.get_text(" ", strip=True) or "")
        if "単勝" in txt:
            candidates.append(t)
    if not candidates:
        candidates = tables

    rows = []
    for t in candidates:
        for tr in t.find_all("tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if len(tds) < 2:
                continue

            umaban = None
            odds = None

            # 馬番（先頭付近に数字のみがあることが多い）
            for cell in tds[:2]:
                if re.fullmatch(r"\d{1,2}", cell):
                    umaban = int(cell)
                    break

            # 単勝（1.0〜999.9 の数値らしきもの）
            for cell in tds:
                if re.fullmatch(r"\d+(\.\d+)?", cell):
                    val = float(cell)
                    if 1.0 <= val <= 999.9:
                        odds = val
                        break

            if umaban is not None and odds is not None:
                rows.append({"umaban": umaban, "odds": odds})

    # 馬番でユニーク化
    uniq = {}
    for r in rows:
        uniq[r["umaban"]] = r["odds"]

    out = [{"umaban": k, "odds": v} for k, v in uniq.items()]
    out.sort(key=lambda x: x["odds"])  # オッズ昇順＝人気昇順
    for i, x in enumerate(out, 1):
        x["pop"] = i
    return out

def fetch_tanfuku_odds(race_id: str) -> Optional[Dict[str, Any]]:
    """
    楽天: 単勝ページを取得して [{umaban, odds, pop}, ...] を返す。
    venue/race_no は簡易抽出。start_at_iso は別関数で取得推奨。
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

    # 画面から場名やRをラフに拾う
    venue = None
    m = re.search(r"（(.+?)）", html)
    if m:
        venue = m.group(1)
    venue = venue or "地方"

    race_no = None
    m = re.search(r"(\d{1,2})R", html)
    race_no = f"{m.group(1)}R" if m else "—R"

    # 発走は別途取得するため仮置き（直近10分後）
    start_at_iso = (datetime.now(JST) + timedelta(minutes=10)).isoformat()

    return {
        "race_id": race_id,
        "venue": venue,
        "race_no": race_no,
        "start_at_iso": start_at_iso,
        "horses": horses,
    }

# ---------------------------------------------------------------------------
# 発走時刻取得（強化版）
# ---------------------------------------------------------------------------
def get_race_start_iso(race_id: str) -> str:
    """
    レースID（例: 202508093230080102）から JST の ISO8601 を返す。
    取得順序:
      1) 既存の社内関数（get_race_start_time / get_race_info）があれば利用
      2) 楽天のレースカード/トップで “発走 12:30” 等を多様な書式で抽出
      3) JSON-LD など埋め込み構造化データの startTime を探索
    失敗時は ValueError
    """
    # 1) 既存関数
    try:
        _fn = globals().get("get_race_start_time")
        if callable(_fn):
            hhmm = _fn(race_id)
            if isinstance(hhmm, str) and re.fullmatch(r"\d{1,2}:\d{2}", hhmm):
                ymd = _yyyymmdd_from_raceid(race_id) or datetime.now(JST).strftime("%Y%m%d")
                return _to_iso(ymd, hhmm)
    except Exception:
        pass

    try:
        _fn = globals().get("get_race_info")
        if callable(_fn):
            info = _fn(race_id) or {}
            hhmm = (info.get("start_time") or info.get("post_time") or "").strip()
            if isinstance(hhmm, str) and re.fullmatch(r"\d{1,2}:\d{2}", hhmm):
                ymd = _yyyymmdd_from_raceid(race_id) or datetime.now(JST).strftime("%Y%m%d")
                return _to_iso(ymd, hhmm)
    except Exception:
        pass

    # 2) 楽天ページのスクレイピング（書式ゆれに広く対応）
    ymd = _yyyymmdd_from_raceid(race_id) or datetime.now(JST).strftime("%Y%m%d")
    urls = [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{race_id}",
        f"https://keiba.rakuten.co.jp/race/top/RACEID/{race_id}",
    ]

    # 代表的な書式：
    #   発走 12:30 / 12:30発走 / 発走時刻12:30 / 出走 12:30 など
    pat1 = re.compile(r"(発走|出走|発走時刻)\s*([0-2]?\d:\d{2})")
    pat2 = re.compile(r"([0-2]?\d:\d{2})\s*(発走|出走)")
    # “時刻だけ”が要素として入っている場合に備えて HH:MM を広く拾う
    pat_loose = re.compile(r"\b([0-2]?\d:\d{2})\b")

    for url in urls:
        try:
            html = _get(url, timeout=10)
        except Exception:
            continue

        # 先に明示的な書式を探す
        m = pat1.search(html) or pat2.search(html)
        if m:
            hhmm = m.group(2) if m.lastindex and m.lastindex >= 2 and re.fullmatch(r"[0-2]?\d:\d{2}", m.group(2)) else m.group(1)
            return _to_iso(ymd, hhmm)

        # BeautifulSoup で “発走” を含む周辺テキストから時刻を拾う
        try:
            soup = BeautifulSoup(html, "html.parser")
            # よくあるラベル類
            candidates = []
            for tag in soup.find_all(text=re.compile("(発走|出走|発走時刻)")):
                txt = tag.parent.get_text(" ", strip=True) if tag and tag.parent else str(tag)
                candidates.append(txt)
            for c in candidates:
                mm = pat_loose.search(c)
                if mm:
                    return _to_iso(ymd, mm.group(1))
            # 直接 “HH:MM” とだけ表示されている要素も拾う
            if not candidates:
                for el in soup.find_all(["time", "span", "p", "div"]):
                    txt = (el.get_text(" ", strip=True) or "")
                    if ("発走" in txt) or ("出走" in txt) or ("発走時刻" in txt):
                        mm = pat_loose.search(txt)
                        if mm:
                            return _to_iso(ymd, mm.group(1))
        except Exception:
            pass

        # JSON-LD / 構造化データ内の startTime を探索
        try:
            for script in re.findall(r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>", html, flags=re.S):
                # 時:分パターンを拾う（例: "startTime": "12:30"）
                mm = re.search(r"\"startTime\"\s*:\s*\"([0-2]?\d:\d{2})\"", script)
                if mm:
                    return _to_iso(ymd, mm.group(1))
        except Exception:
            pass

    raise ValueError(f"発走時刻を取得できませんでした: race_id={race_id}")