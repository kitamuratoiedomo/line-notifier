# -*- coding: utf-8 -*-
"""
strategy_rules.py
- 人気順の単勝オッズで各戦略パターンを判定するユーティリティ
- 各戦略の成立可否と、必要に応じて買い目を返す

想定する race 構造:
race = {
    "race_id": "202508111006060501",
    "track": "盛岡",
    "race_no": 1,
    "odds": {1: 2.4, 2: 3.6, 3: 5.3, 4: 4.7, 5: 57.9, ...}  # 人気順
}
"""

from decimal import Decimal, getcontext
from typing import Dict, Any, Optional, Tuple, List

getcontext().prec = 6  # 境界比較の安定性向上


def _D(x) -> Decimal:
    """floatの丸め誤差対策。"""
    return Decimal(str(x))


def _bets_for_strategy(label: str) -> Optional[List[str]]:
    """通知で使う代表的な買い目例を返す。必要ない戦略は None。"""
    if label == "①":
        # 1〜3番人気の三連単BOX（6点）
        return ["1-2-3", "1-3-2", "2-1-3", "2-3-1", "3-1-2", "3-2-1"]
    if label == "②":
        # 1番人気1着固定 × 2・3番人気（2点）
        return ["1-2-3", "1-3-2"]
    # ③, ④ は別ロジック（相手候補抽出など）で組む前提
    return None


def _strategy_1(od: Dict[int, float]) -> bool:
    """
    ①：1〜3番人気 三連単BOX（6点）
    条件: 1番 2.0<=o1<10.0, 2番<10.0, 3番<10.0, 4番>=15.0
    （従来実装を踏襲）
    """
    o1, o2, o3, o4 = _D(od[1]), _D(od[2]), _D(od[3]), _D(od[4])
    return (Decimal("2.0") <= o1 < Decimal("10.0")
            and o2 < Decimal("10.0")
            and o3 < Decimal("10.0")
            and o4 >= Decimal("15.0"))


def _strategy_2(od: Dict[int, float]) -> bool:
    """
    ②：1番人気1着固定 × 2・3番人気（2点）
    【条件】
      1番 < 2.0
      2番 < 10.0
      3番 < 10.0
      4番 >= 12.0   ← ★今回の更新点
    """
    o1, o2, o3, o4 = _D(od[1]), _D(od[2]), _D(od[3]), _D(od[4])
    return (o1 < Decimal("2.0")
            and o2 < Decimal("10.0")
            and o3 < Decimal("10.0")
            and o4 >= Decimal("12.0"))


def _strategy_3(od: Dict[int, float]) -> bool:
    """
    ③：@（1,2）→(1,2)→3固定（2点）相当の“荒れ待ち・相手厚め”系の検出条件
    仕様（ユーザー指示反映）:
      - 1番 <= 1.5
      - 2番 >= 10.0  ← ★ユーザー要望で必須化
      - さらに“人気上位以外に穴（10〜20倍）が少なくとも1つある”
    """
    o1, o2 = _D(od[1]), _D(od[2])
    if not (o1 <= Decimal("1.5") and o2 >= Decimal("10.0")):
        return False
    # 2番人気以降で 10〜20倍 を少なくとも1頭
    for k, v in od.items():
        if k >= 2:
            dv = _D(v)
            if Decimal("10.0") <= dv <= Decimal("20.0"):
                return True
    return False


def _strategy_4(od: Dict[int, float]) -> bool:
    """
    ④：3着固定（1番人気×2番人気）-3番人気（三連単2点）相当の前提条件
    条件: 1番<=3.0, 2番<=3.0, 3番:6.0〜10.0, 4番>=15.0
    （従来実装を踏襲）
    """
    o1, o2, o3, o4 = _D(od[1]), _D(od[2]), _D(od[3]), _D(od[4])
    return (o1 <= Decimal("3.0")
            and o2 <= Decimal("3.0")
            and Decimal("6.0") <= o3 <= Decimal("10.0")
            and o4 >= Decimal("15.0"))


def match_strategies(race: Dict[str, Any]) -> Optional[Tuple[str, Optional[List[str]], str]]:
    """
    戦略ラベル / 買い目 / ログ用理由 を返す。
    該当なしなら None を返す。

    Returns:
        ("①", ["1-2-3", ...], "条件詳細: ...") など
    """
    od = race.get("odds") or {}
    # 人気1〜4番が揃っていない場合は判定不可
    if not all(k in od for k in (1, 2, 3, 4)):
        return None

    if _strategy_2(od):
        return ("②", _bets_for_strategy("②"),
                "② 1番<2.0 かつ 2・3番<10.0 かつ 4番>=12.0")

    if _strategy_1(od):
        return ("①", _bets_for_strategy("①"),
                "① 1番[2.0〜10.0), 2・3番<10.0, 4番>=15.0")

    if _strategy_3(od):
        return ("③", _bets_for_strategy("③"),
                "③ 1番<=1.5 かつ 2番>=10.0 かつ (2番以降に10〜20倍が1頭以上)")

    if _strategy_4(od):
        return ("④", _bets_for_strategy("④"),
                "④ 1番<=3.0 かつ 2番<=3.0 かつ 3番[6.0〜10.0] かつ 4番>=15.0")

    return None