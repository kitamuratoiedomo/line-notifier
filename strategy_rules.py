# -*- coding: utf-8 -*-
"""
戦略ルール判定（人気順オッズを入力して①〜④を返す）
入力: horses = [{"pop":1,"odds":1.4}, {"pop":2,"odds":6.4}, ...]
出力: None もしくは {
  "strategy": "② 1番人気1着固定 × 2・3番人気（2点）",
  "tickets": ["1-2-3", "1-3-2"],
  "roi": "想定回収率: —",
  "hit": "的中率: —"
}
"""

from typing import List, Dict, Optional

def _to_odds_map(horses: List[Dict]) -> Dict[int, float]:
    """人気→単勝オッズの辞書へ"""
    o: Dict[int, float] = {}
    for h in horses:
        try:
            p = int(h.get("pop"))
            v = float(h.get("odds"))
            if p not in o:
                o[p] = v
        except Exception:
            continue
    return o

def _has_all(o: Dict[int, float], keys) -> bool:
    return all(k in o for k in keys)

def _tickets_box_3(nums=(1,2,3)) -> List[str]:
    a,b,c = nums
    return [f"{a}-{b}-{c}", f"{a}-{c}-{b}",
            f"{b}-{a}-{c}", f"{b}-{c}-{a}",
            f"{c}-{a}-{b}", f"{c}-{b}-{a}"]

def eval_strategy(horses: List[Dict]) -> Optional[Dict]:
    o = _to_odds_map(horses)
    if not _has_all(o, [1,2,3,4]):
        return None

    # ① 1〜3番人気 三連単BOX（6点）
    #    条件: 1番人気2.0〜10.0未満 / 2・3番人気 <10.0 / 4番人気 >=15.0
    if 2.0 <= o[1] < 10.0 and o[2] < 10.0 and o[3] < 10.0 and o[4] >= 15.0:
        return {
            "strategy": "① 1〜3番人気 三連単BOX（6点）",
            "tickets": _tickets_box_3((1,2,3)),
            "roi": "想定回収率: —",
            "hit": "的中率: —",
        }

    # ② 1番人気1着固定 × 2・3番人気（2点）
    #    条件: 1番人気 <2.0 / 2〜3番人気 <10.0 / 4番人気 >=12.0（※ご指定どおり）
    if o[1] < 2.0 and o[2] < 10.0 and o[3] < 10.0 and o[4] >= 12.0:
        return {
            "strategy": "② 1番人気1着固定 × 2・3番人気（2点）",
            "tickets": ["1-2-3", "1-3-2"],
            "roi": "想定回収率: —",
            "hit": "的中率: —",
        }

    # ③ 1番人気 ≤1.5 かつ「2番人気のオッズが10倍以上」
    #    さらにその他人気に 10〜20倍 帯が1頭以上
    if o[1] <= 1.5 and o[2] >= 10.0 and any(10.0 <= v <= 20.0 for k, v in o.items() if k > 2):
        return {
            "strategy": "③ 1→相手 4頭（相手は10〜20倍帯を含む）",
            "tickets": ["1-2-3", "1-2-4", "1-3-2", "1-4-2"],  # 最低限の例示（実運用に合わせて拡張可）
            "roi": "想定回収率: —",
            "hit": "的中率: —",
        }

    # ④ 1,2番人気 ≤3.0 / 3番人気 6.0〜10.0 / 4番人気 ≥15.0
    if o[1] <= 3.0 and o[2] <= 3.0 and 6.0 <= o[3] <= 10.0 and o[4] >= 15.0:
        return {
            "strategy": "④ @ (1,2)→(1,2)→3固定（2点）",
            "tickets": ["1-2-3", "2-1-3"],
            "roi": "想定回収率: —",
            "hit": "的中率: —",
        }

    return None