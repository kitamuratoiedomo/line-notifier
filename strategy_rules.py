# -*- coding: utf-8 -*-
"""
戦略①〜④の判定ロジック（デバッグログ付き）
- horses: [{"pop":1, "odds":2.4}, ...] を受け取る
- eval_strategy(..., logger) に logger を渡すと条件評価の詳細を [DEBUG] で出力
"""

from typing import Dict, List, Optional

def _odds_map(horses: List[Dict]) -> Dict[int, float]:
    """人気→単勝オッズの辞書（1始まり）。不足は None。"""
    m: Dict[int, float] = {}
    for h in horses:
        try:
            p = int(h.get("pop"))
            o = float(h.get("odds"))
            if p not in m:
                m[p] = o
        except Exception:
            continue
    return m

def _fmt(o: Optional[float]) -> str:
    return "—" if o is None else f"{o:.1f}"

def _tickets_for(label: str) -> List[str]:
    # 必要に応じて拡張
    if label == "②":
        return ["1-2-3", "1-3-2"]
    if label == "①":
        # 例：BOX6点など、仕様に合わせて
        return ["1-2-3", "1-3-2", "2-1-3", "2-3-1", "3-1-2", "3-2-1"]
    if label == "③":
        # 例：1→相手4頭→相手4頭 固定（ここは仮）
        return ["1-2-3", "1-2-4"]
    if label == "④":
        # 例：@ (1,2)→(1,2)→3 固定（ここは仮）
        return ["1-2-3", "2-1-3"]
    return []

def eval_strategy(horses: List[Dict], logger=None) -> Optional[Dict]:
    """
    horses から上位人気の単勝オッズを取り、戦略①〜④のいずれかに合致すれば
    {strategy, tickets, roi, hit} を返す。合致しなければ None。
    """
    o = _odds_map(horses)
    o1, o2, o3, o4 = o.get(1), o.get(2), o.get(3), o.get(4)

    # デバッグ：人気上位のオッズ出力
    if logger:
        logger.info(
            f"[DEBUG] odds top4 → 1位={_fmt(o1)}, 2位={_fmt(o2)}, 3位={_fmt(o3)}, 4位={_fmt(o4)}"
        )

    # ここから条件（ユーザー指定）-----------------------------------------
    # ① 既存（例）：1〜3番人気 <10.0、1番人気は[2.0,10.0)、4番人気>=15.0
    cond1 = (
        (o1 is not None and 2.0 <= o1 < 10.0) and
        (o2 is not None and o2 < 10.0) and
        (o3 is not None and o3 < 10.0) and
        (o4 is not None and o4 >= 15.0)
    )

    # ②（確定版）：1番人気<2.0、2〜3番人気<10.0、4番人気>=12.0
    cond2 = (
        (o1 is not None and o1 < 2.0) and
        (o2 is not None and o2 < 10.0) and
        (o3 is not None and o3 < 10.0) and
        (o4 is not None and o4 >= 12.0)
    )

    # ③ 既存：1番人気<=1.5 かつ 2番手以降に 10.0〜20.0 が少なくとも1頭
    cond3 = False
    if o1 is not None and o1 <= 1.5:
        for pop, odd in o.items():
            if pop > 1 and odd is not None and 10.0 <= odd <= 20.0:
                cond3 = True
                break

    # ④ 既存：1位<=3.0, 2位<=3.0, 3位が[6.0,10.0], 4位>=15.0
    cond4 = (
        (o1 is not None and o1 <= 3.0) and
        (o2 is not None and o2 <= 3.0) and
        (o3 is not None and 6.0 <= o3 <= 10.0) and
        (o4 is not None and o4 >= 15.0)
    )
    # --------------------------------------------------------------------

    # デバッグ：各条件の評価結果
    if logger:
        logger.info(
            "[DEBUG] checks → ①=%s, ②=%s, ③=%s, ④=%s",
            cond1, cond2, cond3, cond4
        )

    # 優先順位：②→①→③→④（必要なら調整）
    if cond2:
        return {
            "strategy": "② 1番人気1着固定 × 2・3番人気（2点）",
            "tickets": _tickets_for("②"),
            "roi": "想定回収率: —",
            "hit": "的中率: —",
        }
    if cond1:
        return {
            "strategy": "① 1〜3番人気中心（条件内）",
            "tickets": _tickets_for("①"),
            "roi": "想定回収率: —",
            "hit": "的中率: —",
        }
    if cond3:
        return {
            "strategy": "③ 1→相手（10〜20倍含む）",
            "tickets": _tickets_for("③"),
            "roi": "想定回収率: —",
            "hit": "的中率: —",
        }
    if cond4:
        return {
            "strategy": "④ (1,2)→(1,2)→3 固定 + 4番人気高配狙い",
            "tickets": _tickets_for("④"),
            "roi": "想定回収率: —",
            "hit": "的中率: —",
        }

    return None