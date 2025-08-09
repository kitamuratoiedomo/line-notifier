# scripts/backtest_strategies.py
# -*- coding: utf-8 -*-
"""
地方競馬 4戦略の簡易バックテスト集計スクリプト（全面修正版）
- Rakuten 単勝/複勝ページから最上位人気と単勝オッズを抽出
- 4つの戦略条件を判定し、的中率/回収率（簡易）を概算
- 例外に強く、空ページでも落ちない
- 出力は短い JSON（必要なら末尾の PRINT_LONG を True に）

【使い方】
1) Render もしくはローカルでこのファイルを scripts/ に保存
2) 1年分の race_id リストを scripts/raceids_1y.txt に1行1IDで用意
   （無い場合は環境変数 BACKTEST_RACEIDS にカンマ区切りで渡してもOK）
3) 実行:
   python scripts/backtest_strategies.py

※ 本スクリプトは “オッズ条件一致率ベースの概算” です。
  的中判定はゴール結果を使わず、戦略条件が「成立した回数」を分母とし、
  成立時に想定的中確率×配当期待で粗い回収率を算出します（厳密ではありません）。
"""

import os
import re
import json
import time
import math
import random
import logging
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ---------------------------
# 設定
# ---------------------------
HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
REQ_TIMEOUT = 12
SLEEP_BASE = 0.6        # サイト負荷を避けるためのウェイト秒（±ランダム）
MAX_RACES = 5000        # 読みすぎ防止の上限
MAX_LOG_ROWS = 2000     # ログ肥大化防止用のメモリリングバッファ
PRINT_LONG = False      # True にすると race ごとの詳細を出力

# ---------------------------
# ログ
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# ---------------------------
# 便利構造体
# ---------------------------
@dataclass
class HorseOdds:
    umaban: int
    odds: float
    pop: int

@dataclass
class RaceOdds:
    race_id: str
    horses: List[HorseOdds]  # pop 昇順（=人気順）
    venue: str = ""
    race_no: str = ""


# ---------------------------
# HTML 取得
# ---------------------------
def http_get(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


# ---------------------------
# 単勝テーブル抽出（堅牢版）
# ---------------------------
def parse_tanfuku_table(html: str) -> List[HorseOdds]:
    soup = BeautifulSoup(html, "html.parser")
    # “単勝” の文字が含まれる table を優先
    tables = soup.find_all("table")
    candidates = []
    for t in tables:
        txt = t.get_text(" ", strip=True) or ""
        if "単勝" in txt:
            candidates.append(t)
    if not candidates:
        candidates = tables  # 保険

    found: Dict[int, float] = {}
    for t in candidates:
        for tr in t.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue

            # 馬番候補
            umaban: Optional[int] = None
            for c in cells[:2]:
                m = re.fullmatch(r"\d{1,2}", c)
                if m:
                    umaban = int(m.group(0))
                    break

            # 単勝候補（妥当レンジ）
            odds: Optional[float] = None
            for c in cells:
                if re.fullmatch(r"\d+(\.\d+)?", c):
                    v = float(c)
                    if 1.0 <= v <= 999.9:
                        odds = v
                        break

            if umaban is not None and odds is not None:
                found[umaban] = odds

    if not found:
        return []

    horses = [HorseOdds(umaban=k, odds=v, pop=0) for k, v in found.items()]
    horses.sort(key=lambda x: x.odds)  # 単勝オッズ昇順を人気順とみなす
    for i, h in enumerate(horses, start=1):
        h.pop = i
    return horses


def fetch_race_odds(race_id: str) -> Optional[RaceOdds]:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    try:
        html = http_get(url)
    except Exception as e:
        logging.warning("HTTP 失敗: %s (%s)", race_id, e)
        return None

    horses = parse_tanfuku_table(html)
    if not horses:
        logging.warning("単勝テーブル抽出に失敗（空） race_id=%s", race_id)
        return None

    # 場名・R 番号（簡易）
    venue = ""
    m = re.search(r"（(.+?)）", html)
    if m:
        venue = m.group(1)
    race_no = ""
    m = re.search(r"(\d{1,2})R", html)
    if m:
        race_no = f"{m.group(1)}R"

    return RaceOdds(race_id=race_id, horses=horses, venue=venue, race_no=race_no)


# ---------------------------
# 4戦略の条件判定
# ---------------------------
def pick_top(horses: List[HorseOdds], n: int) -> List[HorseOdds]:
    return horses[:n] if len(horses) >= n else horses[:]

def cond_strategy_1(h: List[HorseOdds]) -> bool:
    # ① 1〜3番人気三連単BOX
    # 1番人気オッズ：2.0〜10.0
    # 2〜3番人気オッズ：10.0未満
    # 4番人気：15.0以上
    if len(h) < 4:
        return False
    a, b, c, d = h[0], h[1], h[2], h[3]
    return (2.0 <= a.odds <= 10.0) and (b.odds < 10.0) and (c.odds < 10.0) and (d.odds >= 15.0)

def cond_strategy_2(h: List[HorseOdds]) -> bool:
    # ② 1番人気1着固定 × 2・3番人気（三連単）
    # 1番人気オッズ：2.0未満
    # 2〜3番人気：10.0未満
    if len(h) < 3:
        return False
    a, b, c = h[0], h[1], h[2]
    return (a.odds < 2.0) and (b.odds < 10.0) and (c.odds < 10.0)

def cond_strategy_3(h: List[HorseOdds]) -> Tuple[bool, List[HorseOdds]]:
    # ③ 1着固定 × 10〜20倍流し（修正版）
    # 1番人気オッズ：1.5以下
    # 相手：2番人気以下から「単勝10〜20倍」の馬を最大4頭
    # 追加条件：2番人気のオッズが 10 以上
    if len(h) < 2:
        return (False, [])
    a = h[0]
    if a.odds > 1.5:
        return (False, [])
    candidates = [x for x in h[1:] if 10.0 <= x.odds <= 20.0]
    if len(h) >= 2 and h[1].odds < 10.0:
        return (False, [])
    return (len(candidates) >= 1, candidates[:4])

def cond_strategy_4(h: List[HorseOdds]) -> bool:
    # ④ 3着固定（3番人気固定）三連単
    # 1・2番人気：3.0以下
    # 3番人気：6.0〜10.0
    # 4番人気：15.0以上
    if len(h) < 4:
        return False
    a, b, c, d = h[0], h[1], h[2], h[3]
    return (a.odds <= 3.0) and (b.odds <= 3.0) and (6.0 <= c.odds <= 10.0) and (d.odds >= 15.0)


# ---------------------------
# 期待回収（超簡易の概算）
# ---------------------------
def expected_roi_for_strategy(tag: str, h: List[HorseOdds], extra: Any) -> float:
    """
    厳密な的中判定/実配当は取得しないため、
    “条件成立時に見込める平均回収倍率の概算”を返す。
    目安のため、実戦の指標ではなく通知用の参考値。
    """
    # ざっくりとしたモデル（調整可）
    if tag == "S1":
        # 3連単BOX 6点、上位拮抗時の平均配当をざっくり 25〜50倍と仮定
        avg_pay = 35.0
        cost = 6.0
        return avg_pay / cost
    if tag == "S2":
        # 1→(2,3) の2点。配当平均 10〜20倍程度を仮定
        avg_pay = 14.0
        cost = 2.0
        return avg_pay / cost
    if tag == "S3":
        # 1→相手4頭→相手4頭（最大 4×3×2=24通り近辺だが重複あり）
        # 候補数 m の時は m*(m-1)*m/?? といった規模。ざっくり 18点想定。
        m = max(1, len(extra) or 1)
        cost = min(24.0, max(6.0, 3.0 * m))  # だいたいの点数
        avg_pay = 60.0  # そこそこ荒れる想定
        return avg_pay / cost
    if tag == "S4":
        # (1,2)→(1,2)→3 固定 2点。配当平均 20倍ぐらいを仮定
        return 20.0 / 2.0
    return 1.0


# ---------------------------
# バックテスト本体
# ---------------------------
def load_race_ids() -> List[str]:
    # 1) ファイル優先
    path = os.path.join(os.path.dirname(__file__), "raceids_1y.txt")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            ids = [ln.strip() for ln in f if ln.strip()]
            return ids[:MAX_RACES]

    # 2) 環境変数
    env = os.getenv("BACKTEST_RACEIDS", "").strip()
    if env:
        ids = [x.strip() for x in env.split(",") if x.strip()]
        return ids[:MAX_RACES]

    logging.warning("raceids_1y.txt も BACKTEST_RACEIDS も見つかりません。対象0件。")
    return []


def backtest() -> Dict[str, Any]:
    race_ids = load_race_ids()
    random.shuffle(race_ids)  # 偏りを減らす
    logs_ring: List[str] = []

    def ring_add(msg: str):
        logs_ring.append(msg)
        if len(logs_ring) > MAX_LOG_ROWS:
            logs_ring.pop(0)

    stats = {
        "S1": {"name": "① 1〜3番人気 三連単BOX(6点)", "trials": 0, "roi_sum": 0.0},
        "S2": {"name": "② 1人気1着固定 × 2・3人気(2点)", "trials": 0, "roi_sum": 0.0},
        "S3": {"name": "③ 1→相手4頭→相手4頭", "trials": 0, "roi_sum": 0.0},
        "S4": {"name": "④ (1,2)→(1,2)→3固定(2点)", "trials": 0, "roi_sum": 0.0},
    }

    for idx, rid in enumerate(race_ids, start=1):
        try:
            # スロットリング
            time.sleep(SLEEP_BASE + random.random() * 0.4)

            ro = fetch_race_odds(rid)
            if not ro or not ro.horses or len(ro.horses) < 3:
                ring_add(f"[SKIP] no-odds {rid}")
                continue

            h = ro.horses

            # S1
            if cond_strategy_1(h):
                stats["S1"]["trials"] += 1
                stats["S1"]["roi_sum"] += expected_roi_for_strategy("S1", h, None)

            # S2
            if cond_strategy_2(h):
                stats["S2"]["trials"] += 1
                stats["S2"]["roi_sum"] += expected_roi_for_strategy("S2", h, None)

            # S3
            ok3, cand = cond_strategy_3(h)
            if ok3:
                stats["S3"]["trials"] += 1
                stats["S3"]["roi_sum"] += expected_roi_for_strategy("S3", h, cand)

            # S4
            if cond_strategy_4(h):
                stats["S4"]["trials"] += 1
                stats["S4"]["roi_sum"] += expected_roi_for_strategy("S4", h, None)

            if idx % 50 == 0:
                logging.info("progress %d/%d", idx, len(race_ids))

        except Exception as e:
            ring_add(f"[ERR] {rid} {e}")
            continue

    # まとめ
    summary: Dict[str, Any] = {"total_races": len(race_ids), "strategies": {}}
    for k, v in stats.items():
        trials = v["trials"]
        roi_avg = (v["roi_sum"] / trials) if trials > 0 else 0.0
        # 成立率（対象レースに対してどれだけ条件に合致したか）
        cond_rate = (trials / len(race_ids)) if race_ids else 0.0
        summary["strategies"][k] = {
            "name": v["name"],
            "trials": trials,
            "cond_rate": round(cond_rate * 100, 2),   # %
            "est_roi": round(roi_avg * 100, 1),       # %
        }

    if PRINT_LONG:
        summary["tail_logs"] = logs_ring[-50:]

    return summary


# ---------------------------
# main
# ---------------------------
if __name__ == "__main__":
    try:
        res = backtest()
        print(json.dumps(res, ensure_ascii=False))
    except Exception as e:
        logging.exception("backtest failed: %s", e)
        # 失敗してもプロセスが落ちっぱなしにならないように空 JSON を返す
        print(json.dumps({"error": str(e)}))