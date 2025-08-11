# -*- coding: utf-8 -*-
"""
戦略判定ロジック（堅牢化版）
 - どの形式で来ても {人気順位(int): 単勝オッズ(float)} に正規化してから判定
 - 戦略②: 1番<2.0 & 2番<10.0 & 3番<10.0 & 4番>=12.0
"""

from typing import Any, Dict


def _to_float(x: Any, default: float = 99.9) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _get_odds_map(raw: Any) -> Dict[int, float]:
    """
    raw 受け取りの何でも屋:
      - {1: 2.4, 2: 3.6, ...}
      - {"odds": {...}} / {"odds": [...]}
      - [2.4, 3.6, 5.0, ...]   # index0が1番人気
      - [{"rank":1,"odds":2.4}, {"rank":2,"odds":3.6}, ...]
      - [{"popular":1,"odds":2.4}, ...] など
    """
    if raw is None:
        return {}

    # {"odds": ...} を剥がす
    if isinstance(raw, dict) and "odds" in raw:
        raw = raw["odds"]

    # 既に {rank: odds}
    if isinstance(raw, dict):
        out = {}
        for k, v in raw.items():
            try:
                rk = int(k)
            except Exception:
                # '1位' みたいなのを弾く
                continue
            out[rk] = _to_float(v)
        return out

    # 配列形式
    if isinstance(raw, (list, tuple)):
        # 1) 単なる float 配列: [2.4, 3.6, ...]
        if all(not isinstance(x, (dict, list, tuple)) for x in raw):
            return {i + 1: _to_float(raw[i]) for i in range(len(raw))}

        # 2) 要素が dict の配列
        out = {}
        for item in raw:
            if isinstance(item, dict):
                # rank/popu lar 人気順位っぽいキーを探す
                if "rank" in item:
                    rk = int(item["rank"])
                elif "popular" in item:
                    rk = int(item["popular"])
                elif "pop" in item:
                    rk = int(item["pop"])
                else:
                    # インデックス順を人気とみなす fallback
                    rk = len(out) + 1

                # オッズ値っぽいキーを探す
                if "odds" in item:
                    ov = item["odds"]
                elif "value" in item:
                    ov = item["value"]
                elif "tanfuku_odds" in item:
                    ov = item["tanfuku_odds"]
                else:
                    # 浮動小数に変換できそうな最初の値を拾う
                    # （かなり寛容。だめなら 99.9）
                    ov = next(iter(item.values()))
                out[rk] = _to_float(ov)
            else:
                # サブリストなどはスキップ（想定外）
                pass
        if out:
            return out

    # どれにも当てはまらなかった時の保険
    return {}


def eval_strategy(odds_raw: Any) -> str | None:
    """
    引数は「オッズ情報そのもの」を渡す想定（リスト/辞書どちらでもOK）
    ここで正規化してから判定する。
    """
    o = _get_odds_map(odds_raw)

    # キーが欠けていたら大きい値で補完（≒条件に引っ掛からない）
    def get(rank: int, default: float = 99.9) -> float:
        return _to_float(o.get(rank, default))

    o1, o2, o3, o4 = get(1), get(2), get(3), get(4)

    # ② 1番人気1着固定 × 2・3番人気（2点）
    #    1番 < 2.0, 2番 < 10.0, 3番 < 10.0, 4番 >= 12.0
    if o1 < 2.0 and o2 < 10.0 and o3 < 10.0 and o4 >= 12.0:
        return "②"

    # ① 0〜例：2.0<=1番<10.0, 2・3番<10.0, 4番>=15.0（前の仕様を維持）
    if 2.0 <= o1 < 10.0 and o2 < 10.0 and o3 < 10.0 and o4 >= 15.0:
        return "①"

    # ③ 1番<=1.5 かつ その他に10〜20倍が少なくとも1頭
    if o1 <= 1.5 and any(10.0 <= _to_float(v) <= 20.0 for k, v in o.items() if k > 1):
        return "③"

    # ④ 1番<=3.0 かつ 2番<=3.0 かつ 3番が6〜10倍 かつ 4番>=15.0
    if o1 <= 3.0 and o2 <= 3.0 and 6.0 <= o3 <= 10.0 and o4 >= 15.0:
        return "④"

    return None