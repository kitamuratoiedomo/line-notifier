# scripts/backtest_strategies.py
# -*- coding: utf-8 -*-
"""
NAR公式（keiba.go.jp）の結果ページをスクレイピングして、
過去期間の地方競馬レースを対象に、戦略①〜④の
・該当判定
・的中判定（三連単）
・的中率/回収率
を集計するワンショットスクリプト。

注意:
- NAR側のHTML構造が変わると要修正
- ばんえい（帯広ば・ばんえい競馬）は対象外にする
- リクエストは控えめ（レート制御&簡易キャッシュあり）
"""

import os
import re
import json
import time
import argparse
from datetime import datetime, timedelta, timezone
from dateutil.parser import isoparse

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))
HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}
CACHE_DIR = os.path.join(".cache_nar")
os.makedirs(CACHE_DIR, exist_ok=True)

# =========================
# ユーティリティ
# =========================
def _cache_path(tag: str, key: str) -> str:
    safe = re.sub(r"[^0-9A-Za-z_.-]", "_", key)
    return os.path.join(CACHE_DIR, f"{tag}__{safe}.html")

def get(url: str, sleep_sec=0.8) -> str:
    """シンプルGET+ファイルキャッシュ"""
    cp = _cache_path("GET", url)
    if os.path.exists(cp):
        return open(cp, "r", encoding="utf-8", errors="ignore").read()

    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    # 公式は基本UTF-8
    r.encoding = r.apparent_encoding or "utf-8"
    html = r.text
    with open(cp, "w", encoding="utf-8") as f:
        f.write(html)
    time.sleep(sleep_sec)
    return html

def daterange(since: datetime, until: datetime):
    d = since
    while d <= until:
        yield d
        d += timedelta(days=1)

def to_float(text: str):
    try:
        return float(text.replace(",", ""))
    except:
        return None

# =========================
# NARのページ探索（概形）
# =========================
def list_day_meetings(day: datetime):
    """
    指定日の開催一覧ページを取得し、その日の各開催（場）トップURLを返す。
    注: 実運用では、keiba.go.jp の「日別開催一覧」ページURLに合わせて要調整。
    """
    ymd = day.strftime("%Y%m%d")
    # 例URL（サイト構造次第で調整してください）
    # 実運用では公式の「開催日一覧」→「場ごとトップ」→「各レース結果」へ辿ります。
    url = f"https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate={ymd}"
    html = get(url)
    soup = BeautifulSoup(html, "html.parser")

    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # 開催場トップ/番組へ繋がるリンクを大雑把に拾う
        if "TodayRaceInfo" in href or "RaceMarkTable" in href:
            links.append(requests.compat.urljoin(url, href))
    # 重複除去
    out = sorted(set(links))
    return out

def list_meeting_races(meeting_url: str):
    """
    開催場ページから、その日の各レース「結果ページ」へのリンクを列挙。
    """
    html = get(meeting_url)
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # 「レース結果」ページらしきリンクを拾う（要調整）
        if ("RaceResult" in href) or ("RaceMarkTable" in href):
            links.append(requests.compat.urljoin(meeting_url, href))
    return sorted(set(links))

def parse_race_result(race_url: str):
    """
    レース結果ページを解析し、以下を返す:
    - venue: 場名（例: 佐賀）
    - race_no: "8R" 等
    - horses: [{num(馬番), pop(人気), tan_odds(単勝)} ...]
    - trifecta: {"order":[num1,num2,num3], "pay":配当(100円)}
    - is_banei: True/False
    """
    html = get(race_url)
    soup = BeautifulSoup(html, "html.parser")

    # 場名・R
    title_text = soup.get_text(" ", strip=True)
    m1 = re.search(r"([^\s0-9]+)\s*(\d{1,2})R", title_text)
    venue, race_no = None, None
    if m1:
        venue = m1.group(1)
        race_no = f"{int(m1.group(2))}R"

    # ばんえい判定（「ばんえい」「帯広」などを含む）
    is_banei = False
    if venue and ("ばんえい" in venue or "帯広" in venue or "帯広ば" in venue):
        is_banei = True
    if "ばんえい" in title_text:
        is_banei = True

    # 馬ごとの行を探し、馬番・人気・単勝オッズを抽出
    horses = []
    # よくあるパターン: <table>に「人気」「単勝」列がある
    # 何種類かトライ
    tables = soup.find_all("table")
    for t in tables:
        head = [th.get_text(strip=True) for th in t.find_all("th")]
        if not head:
            continue
        has_pop = any("人気" in h for h in head)
        has_tan = any("単勝" in h for h in head)
        has_umaban = any("馬" in h or "馬番" in h for h in head)
        if not (has_pop and has_tan):
            continue

        for tr in t.find_all("tr"):
            tds = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) < 3:
                continue
            # ざっくり列位置探索
            try:
                # 馬番
                num = None
                # 人気・単勝
                pop = None
                tan = None

                # まずヘッダ位置マップ
                idx_pop = None
                idx_tan = None
                idx_num = None
                for i, h in enumerate(head):
                    if "人気" in h: idx_pop = i
                    if "単勝" in h: idx_tan = i
                    if "馬" in h:   idx_num = i

                # Fallback: 既知の並び（手当て）
                if idx_num is None and len(tds) >= 1:
                    # 1列目に馬番っぽい数字があるケース
                    if re.fullmatch(r"\d{1,2}", tds[0]):
                        idx_num = 0

                if idx_num is not None and idx_num < len(tds):
                    if re.fullmatch(r"\d{1,2}", tds[idx_num]):
                        num = int(tds[idx_num])

                if idx_pop is not None and idx_pop < len(tds):
                    if re.fullmatch(r"\d{1,2}", tds[idx_pop]):
                        pop = int(tds[idx_pop])

                if idx_tan is not None and idx_tan < len(tds):
                    v = tds[idx_tan].replace(",", "")
                    if re.fullmatch(r"\d+(\.\d+)?", v):
                        tan = float(v)

                if num and pop and tan:
                    horses.append({"num": num, "pop": pop, "tan_odds": tan})
            except:
                continue

    # 人気重複/欠落ガード：popでソートし直す（同率は適当）
    horses = [h for h in horses if isinstance(h.get("pop"), int)]
    horses.sort(key=lambda x: x["pop"])

    # 三連単の配当と着順（order）を取得
    trifecta = {"order": [], "pay": None}
    # 「払戻金」テーブルを探索して "三連単" 行を取る
    pay_tables = [t for t in tables if "払戻" in t.get_text(" ", strip=True)]
    if not pay_tables:
        pay_tables = tables
    for t in pay_tables:
        txt = t.get_text(" ", strip=True)
        if "三連単" in txt or "3連単" in txt:
            # 出目（例: 1-5-8）と配当（例: 12,340円）
            m_order = re.search(r"(\d{1,2})\s*[-－]\s*(\d{1,2})\s*[-－]\s*(\d{1,2})", txt)
            m_pay   = re.search(r"(三連単|3連単).{0,40}?([\d,]+)\s*円", txt)
            if m_order:
                o1, o2, o3 = int(m_order.group(1)), int(m_order.group(2)), int(m_order.group(3))
                trifecta["order"] = [o1, o2, o3]
            if m_pay:
                trifecta["pay"] = int(m_pay.group(2).replace(",", ""))
            if trifecta["order"]:
                break

    return {
        "venue": venue or "",
        "race_no": race_no or "",
        "horses": horses,           # [{num,pop,tan_odds}, ...] (pop昇順想定)
        "trifecta": trifecta,       # {"order":[..], "pay": int(円/100円あたり)}
        "is_banei": is_banei,
        "race_url": race_url,
    }

# =========================
# 戦略ロジック
# =========================
def strategy_matches(horses):
    """
    horses: pop昇順の配列。各要素 {num,pop,tan_odds}
    各戦略で「条件に合致するか」と「買い目（三連単の組合せ）」を返す。
    """
    # 人気順でアクセスしやすいよう取り直し
    by_pop = {h["pop"]: h for h in horses if isinstance(h.get("tan_odds"), (int, float))}
    if not by_pop.get(1) or not by_pop.get(2) or not by_pop.get(3):
        return {}

    one = by_pop[1]["tan_odds"]
    two = by_pop[2]["tan_odds"]
    three = by_pop[3]["tan_odds"]
    four = by_pop.get(4, {}).get("tan_odds", None)

    res = {}

    # ① 1〜3番人気三連単BOX
    # 1番人気オッズ：2.0〜10.0 / 2〜3番人気：<10.0 / 4番人気：>=15.0
    if one is not None and two is not None and three is not None and four is not None:
        if (2.0 <= one <= 10.0) and (two < 10.0) and (three < 10.0) and (four >= 15.0):
            # 1,2,3のBOX -> 6点
            a,b,c = by_pop[1]["num"], by_pop[2]["num"], by_pop[3]["num"]
            picks = [
                (a,b,c),(a,c,b),
                (b,a,c),(b,c,a),
                (c,a,b),(c,b,a)
            ]
            res["S1"] = {"name": "① 1〜3番人気三連単BOX（6点）", "bets": picks}

    # ② 1番人気1着固定 × 2・3番人気（三連単） -> 2点
    # 1番人気オッズ：<2.0 / 2〜3番人気：<10.0
    if one is not None and two is not None and three is not None:
        if (one < 2.0) and (two < 10.0) and (three < 10.0):
            a,b,c = by_pop[1]["num"], by_pop[2]["num"], by_pop[3]["num"]
            picks = [(a,b,c),(a,c,b)]
            res["S2"] = {"name": "② 1番人気1着固定 × 2・3番人気（2点）", "bets": picks}

    # ③ 1着固定 × 10〜20倍流し（1→相手4→相手4、相手は2番人気以下・単勝10〜20倍）
    # 追加条件: 2番人気オッズが10倍以上
    if one is not None and two is not None:
        if one <= 1.5 and two >= 10.0:
            # 候補: 2番人気以下で 10〜20倍
            cands = [h for h in horses if h["pop"] >= 2 and 10.0 <= (h.get("tan_odds") or 0) <= 20.0]
            cands = cands[:4]  # 最大4頭
            if len(cands) >= 1:
                a = by_pop[1]["num"]
                opp = [x["num"] for x in cands]
                # permutations with distinct second/third
                picks = []
                for i in range(len(opp)):
                    for j in range(len(opp)):
                        if i == j: 
                            continue
                        picks.append((a, opp[i], opp[j]))
                res["S3"] = {"name": f"③ 1着固定 × 10〜20倍流し（相手{len(opp)}頭）", "bets": picks}

    # ④ 3着固定（3番人気固定）三連単（2点）
    # 1・2番人気：<=3.0 / 3番人気：6.0〜10.0 / 4番人気：>=15.0
    if one is not None and two is not None and three is not None and four is not None:
        if (one <= 3.0) and (two <= 3.0) and (6.0 <= three <= 10.0) and (four >= 15.0):
            a,b,c = by_pop[1]["num"], by_pop[2]["num"], by_pop[3]["num"]
            picks = [(a,b,c),(b,a,c)]
            res["S4"] = {"name": "④ 3着固定（3番人気固定）三連単（2点）", "bets": picks}

    return res

# =========================
# 集計実行
# =========================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=(datetime.now(JST) - timedelta(days=365)).strftime("%Y-%m-%d"))
    ap.add_argument("--until", default=datetime.now(JST).strftime("%Y-%m-%d"))
    args = ap.parse_args()

    since = datetime.fromisoformat(args.since)
    until = datetime.fromisoformat(args.until)

    stats = {
        "S1": {"name":"① BOX", "hit":0, "try":0, "bet":0, "ret":0},
        "S2": {"name":"② 1→(2,3)", "hit":0, "try":0, "bet":0, "ret":0},
        "S3": {"name":"③ 1→相手→相手", "hit":0, "try":0, "bet":0, "ret":0},
        "S4": {"name":"④ (1,2)→→3固定", "hit":0, "try":0, "bet":0, "ret":0},
    }

    for day in daterange(since, until):
        try:
            meetings = list_day_meetings(day)
        except Exception:
            continue
        for murl in meetings:
            try:
                race_urls = list_meeting_races(murl)
            except Exception:
                continue
            for rurl in race_urls:
                try:
                    info = parse_race_result(rurl)
                except Exception:
                    continue

                # ばんえい除外
                if info.get("is_banei"):
                    continue

                horses = info.get("horses") or []
                tfx = info.get("trifecta") or {}
                order = tfx.get("order") or []
                pay = tfx.get("pay")

                if len(horses) < 4 or len(order) != 3 or pay is None:
                    continue

                matches = strategy_matches(horses)
                if not matches:
                    continue

                # 実際の三連単的中チェック
                for key, val in matches.items():
                    bets = val["bets"]
                    n_bets = len(bets)
                    if n_bets == 0:
                        continue
                    stats[key]["try"] += 1
                    stats[key]["bet"] += n_bets * 100  # 1点=100円想定

                    if tuple(order) in bets:
                        stats[key]["hit"] += 1
                        stats[key]["ret"] += pay  # 払戻は100円当たりの配当

    # 結果集計
    def safe_pct(a, b):
        return 0.0 if b == 0 else (a / b * 100.0)

    print("=== 過去統計（期間: {} 〜 {}）===".format(args.since, args.until))
    out = {}
    for k, s in stats.items():
        hitrate = safe_pct(s["hit"], s["try"])
        roi = 0.0 if s["bet"] == 0 else (s["ret"] / s["bet"] * 100.0)
        print(f"{k} {s['name']}: 該当 {s['try']}件 / 的中 {s['hit']}件 / 的中率 {hitrate:.1f}% / 回収率 {roi:.1f}%")
        out[k] = {
            "name": s["name"],
            "trials": s["try"],
            "hits": s["hit"],
            "hit_rate": round(hitrate, 1),
            "roi": round(roi, 1),
        }

    # JSONも出す（main.pyへ貼り付けやすい）
    print("\n--- STRAT_STATS.json ---")
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()