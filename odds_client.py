# odds_client.py
import os
import re
import time
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

# ===== 基本設定 =====
JST = timezone(timedelta(hours=9))
UA_DESKTOP = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
UA_MOBILE  = "Mozilla/5.0 (Linux; Android 10; Pixel 5)"

def _get(url: str, timeout: int = 12, mobile: bool = False) -> str:
    headers = {"user-agent": (UA_MOBILE if mobile else UA_DESKTOP)}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

# ===== RACEID 列挙（簡易） =====
def list_today_raceids() -> List[str]:
    """
    環境変数 RACEIDS にカンマ区切りで RACEID を渡す想定。
    例）RACEIDS=202508073601090301,202508072726110201
    """
    env = os.getenv("RACEIDS", "").strip()
    if not env:
        logging.info("RACEIDS が未設定のため、レース列挙をスキップ")
        return []
    return [x.strip() for x in env.split(",") if x.strip()]

# ===== 単勝オッズ抽出 =====
def _parse_tanfuku_table(html: str) -> List[Dict[str, Any]]:
    """
    単勝オッズ表から [{umaban:int, odds:float, pop:int}, ...] を抽出。
    人気順は odds（昇順）で擬似決定。
    """
    soup = BeautifulSoup(html, "html.parser")

    # 「単勝」を含む table を優先、無ければ候補を広めに
    tables = soup.find_all("table")
    candidates = []
    for t in tables:
        txt = (t.get_text(" ", strip=True) or "")
        if "単勝" in txt:
            candidates.append(t)
    if not candidates:
        candidates = soup.select("table, .oddsTable, .tblOdds")

    rows: List[Dict[str, Any]] = []
    for t in candidates:
        for tr in t.find_all("tr"):
            # th混在も想定
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
            if len(cells) < 2:
                continue

            umaban = None
            odds = None

            # 馬番（先頭2〜3セルで整数っぽい値）
            for cell in cells[:3]:
                if cell.isdigit():
                    n = int(cell)
                    if 1 <= n <= 18:
                        umaban = n
                        break

            # オッズ候補（1.0〜999.9の数値）
            for cell in cells:
                try:
                    val = float(cell)
                    if 1.0 <= val <= 999.9:
                        odds = val
                        break
                except ValueError:
                    continue

            if umaban is not None and odds is not None:
                rows.append({"umaban": umaban, "odds": odds})

    # 重複排除＆ソート
    uniq: Dict[int, float] = {}
    for r in rows:
        uniq[r["umaban"]] = r["odds"]
    out = [{"umaban": k, "odds": v} for k, v in uniq.items()]
    out.sort(key=lambda x: x["odds"])
    for i, x in enumerate(out, 1):
        x["pop"] = i
    return out

def _extract_venue_and_rno_from_html(html: str) -> Tuple[Optional[str], Optional[str]]:
    venue = None
    rno = None
    try:
        # 全角括弧で場名が入ることが多い
        m1 = re.search(r"（(.+?)）", html)
        if m1:
            venue = m1.group(1).strip()
        m2 = re.search(r"(\d{1,2})R", html)
        if m2:
            rno = f"{m2.group(1)}R"
    except Exception:
        pass
    return venue, rno

def fetch_tanfuku_odds(race_id: str) -> Optional[Dict[str, Any]]:
    """
    単勝ページを取得して馬番×単勝オッズを返す。
    失敗時は None。
    """
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    try:
        html = _get(url)
        horses = _parse_tanfuku_table(html)
        if not horses:
            # 0.8秒待ってモバイルUAで再トライ
            time.sleep(0.8)
            html = _get(url, mobile=True)
            horses = _parse_tanfuku_table(html)
        if not horses:
            logging.warning("単勝テーブル抽出に失敗（空） race_id=%s", race_id)
            return None
    except Exception as e:
        logging.warning("odds取得失敗 %s: %s", race_id, e)
        return None

    venue, rno = _extract_venue_and_rno_from_html(html)
    start_at_iso = (datetime.now(JST) + timedelta(minutes=10)).isoformat()  # 保険（本番は別関数で）

    return {
        "race_id": race_id,
        "venue": venue or "地方",
        "race_no": rno or "—R",
        "start_at_iso": start_at_iso,
        "horses": horses,  # [{umaban, odds, pop}, ...] 人気順
    }

# ===== 発走・販売締切の推定 =====
def _to_iso_today_hhmm(hhmm: str) -> str:
    """'HH:MM' を 今日の日付の JST ISO8601 に変換"""
    today = datetime.now(JST).strftime("%Y-%m-%d")
    return f"{today}T{hhmm}:00+09:00"

def get_race_start_iso(race_id: str) -> str:
    """
    レースIDから、JST の ISO8601 文字列で**発走時刻**を返す。
    楽天のレースカードを軽量スクレイピングし、'発走 HH:MM' を抽出。
    取得失敗時は ValueError。
    """
    urls = [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{race_id}",
        f"https://keiba.rakuten.co.jp/race/top/RACEID/{race_id}",
    ]
    pat = re.compile(r"(発走|出走)\s*([0-2]?\d:\d{2})")
    last_err = None
    for i, url in enumerate(urls):
        try:
            html = _get(url, mobile=(i == 1))
            m = pat.search(html)
            if m:
                return _to_iso_today_hhmm(m.group(2))
        except Exception as e:
            last_err = e
            continue
    raise ValueError(f"発走時刻を取得できませんでした: race_id={race_id} err={last_err}")

def get_sale_close_iso(race_id: str) -> str:
    """
    **販売締切の推定**を JST の ISO8601 で返す。
    ポリシー: 販売締切 ≒ 発走5分前 とみなし、 start-5分 を返却。
    """
    start_iso = get_race_start_iso(race_id)
    t = datetime.fromisoformat(start_iso)
    close = t - timedelta(minutes=5)
    return close.isoformat()

# ===== 通知文用の補助 =====
def get_venue_and_rno(race_id: str) -> Tuple[Optional[str], Optional[str]]:
    """
    場名とレース番号を返す（どちらか/両方 None の可能性あり）
    """
    urls = [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{race_id}",
        f"https://keiba.rakuten.co.jp/race/top/RACEID/{race_id}",
    ]
    venue = None
    rno = None
    for i, url in enumerate(urls):
        try:
            html = _get(url, mobile=(i == 1))
            v, r = _extract_venue_and_rno_from_html(html)
            venue = venue or v
            rno = rno or r
            if venue and rno:
                break
        except Exception:
            continue
    return venue, rno

def get_race_title(race_id: str) -> str:
    """
    「佐賀 8R」のような簡易タイトル（どちらか欠けた場合は race_id を返す）
    """
    v, r = get_venue_and_rno(race_id)
    if v and r:
        return f"{v} {r}"
    return race_id