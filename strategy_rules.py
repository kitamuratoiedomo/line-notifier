# strategy_rules.py
# 戦略①〜④の判定ロジックをまとめたモジュール
# main.py からは eval_strategy(race) を呼び出します。
# race には少なくとも race['odds'] が {順位(int)->単勝オッズ(float)} で入っている想定です。

from typing import Dict, Any

def _get_odds_map(race: Dict[str, Any]) -> Dict[int, float]:
    """
    race['odds'] を取り出し、キーを int、値を float に正規化して返す。
    不足キーは非常時のために大きな値を詰めて判定を“外す”ようにする。
    """
    raw = race.get("odds") or {}
    o: Dict[int, float] = {}
    for k, v in raw.items():
        try:
            rk = int(k)
        except Exception:
            # 1,2,3,4以外は無視
            continue
        try:
            rv = float(v)
        except Exception:
            rv = 9999.0
        o[rk] = rv
    # 念のため主要キーがなければ“外す”方向に
    for need in (1, 2, 3, 4):
        if need not in o:
            o[need] = 9999.0
    return o


def eval_strategy(race: Dict[str, Any]) -> str | None:
    """
    戦略一致時に '①' '②' '③' '④' のいずれかを返す。該当なしは None。
    仕様（2025-08-11時点）：
      ① 2.0 <= 1番人気 < 10.0、2&3番人気 < 10.0、4番人気 >= 15.0
      ② 1番人気 < 2.0、2&3番人気 < 10.0、4番人気 >= 12.0   ←最新指示
      ③ 1番人気 <= 1.5、かつ 2番人気のオッズ >= 10.0        ←ユーザー要望で追加条件
      ④ 1番人気 <= 3.0、2番人気 <= 3.0、3番人気 6.0〜10.0、4番人気 >= 15.0
    """
    o = _get_odds_map(race)

    # ①：BOX 3頭（最大？）の想定条件
    if 2.0 <= o[1] < 10.0 and o[2] < 10.0 and o[3] < 10.0 and o[4] >= 15.0:
        return '①'

    # ②：1番人気1着固定 × 2・3番人気（2点）
    #    条件：1番人気 < 2.0、2&3 < 10.0、4番人気 >= 12.0  ← 更新
    if o[1] < 2.0 and o[2] < 10.0 and o[3] < 10.0 and o[4] >= 12.0:
        return '②'

    # ③：1→相手4頭→相手4頭（例）※仕様は検出条件のみ固定
    #    追加条件：2番人気のオッズが 10倍以上
    if o[1] <= 1.5 and o[2] >= 10.0:
        return '③'

    # ④：@ (1,2)→(1,2)→3 固定（2点）に相当する検出条件（以前どおり）
    if o[1] <= 3.0 and o[2] <= 3.0 and 6.0 <= o[3] <= 10.0 and o[4] >= 15.0:
        return '④'

    return None