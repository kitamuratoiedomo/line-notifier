from typing import Dict, Any, List, Optional

def _get_by_pop(horses: List[Dict[str, Any]], pop: int) -> Optional[Dict[str, Any]]:
    for h in horses:
        if h.get("pop") == pop:
            return h
    return None

def eval_strategy(horses: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    戦略①〜④のいずれかに一致したら dict を返す。
    返却: { "strategy": "① …", "tickets": [...], "roi": "...", "hit": "..." }
    """

    p1 = _get_by_pop(horses, 1)
    p2 = _get_by_pop(horses, 2)
    p3 = _get_by_pop(horses, 3)
    p4 = _get_by_pop(horses, 4)
    if not (p1 and p2 and p3 and p4):
        return None

    o1, o2, o3, o4 = p1["odds"], p2["odds"], p3["odds"], p4["odds"]

    # ① 1〜3番人気BOX
    if (2.0 <= o1 <= 10.0) and (o2 < 10.0) and (o3 < 10.0) and (o4 >= 15.0):
        tickets = [f"{p1['pop']}-{p2['pop']}-{p3['pop']}",
                   f"{p1['pop']}-{p3['pop']}-{p2['pop']}",
                   f"{p2['pop']}-{p1['pop']}-{p3['pop']}",
                   f"{p2['pop']}-{p3['pop']}-{p1['pop']}",
                   f"{p3['pop']}-{p1['pop']}-{p2['pop']}",
                   f"{p3['pop']}-{p2['pop']}-{p1['pop']}"]
        return {
            "strategy": "① 1〜3番人気BOX（6点）",
            "tickets": tickets,
            "roi": "想定回収率: 138.5% / 的中率: 22.4%",
            "hit": "対象354Rベース",
        }

    # ② 1番人気1着固定 × 2・3番人気（2点）
    if (o1 < 2.0) and (o2 < 10.0) and (o3 < 10.0):
        tickets = [f"{p1['pop']}-{p2['pop']}-{p3['pop']}", f"{p1['pop']}-{p3['pop']}-{p2['pop']}"]
        return {
            "strategy": "② 1番人気1着固定 × 2・3番人気（2点）",
            "tickets": tickets,
            "roi": "想定回収率: 131.4% / 的中率: 43.7%",
            "hit": "対象217Rベース",
        }

    # ③ 1着固定 × 10〜20倍流し（最大5頭）
    if o1 <= 1.5:
        cand = [h for h in horses if h["pop"] >= 2 and 10.0 <= h["odds"] <= 20.0][:5]
        if cand:
            tickets = [f"{p1['pop']}-{c['pop']}-総流し" for c in cand]
            return {
                "strategy": "③ 1着固定 × 10〜20倍流し（候補最大5頭）",
                "tickets": tickets,
                "roi": "想定回収率: 139.2% / 的中率: 16.8%",
                "hit": "対象89Rベース",
            }

    # ④ 3着固定（3番人気固定）2点
    if (o1 <= 3.0) and (o2 <= 3.0) and (6.0 <= o3 <= 10.0) and (o4 >= 15.0):
        tickets = [f"{p1['pop']}-{p2['pop']}-{p3['pop']}", f"{p2['pop']}-{p1['pop']}-{p3['pop']}"]
        return {
            "strategy": "④ 3着固定（3番人気固定）2点",
            "tickets": tickets,
            "roi": "想定回収率: 133.7% / 的中率: 21.5%",
            "hit": "対象128Rベース",
        }

    return None