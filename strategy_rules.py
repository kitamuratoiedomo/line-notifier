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

def _pick_candidates_10_20(horses: List[Dict]) -> List[int]:
    """
    相手候補：単勝オッズが10.0〜20.0（両端含む）の馬を人気順で抽出（1番人気は除外）。
    返り値は「人気(pop)のリスト」。最大4頭に制限。
    """
    # popularity (pop) 昇順でソートされたエントリを走査
    filtered = []
    for h in sorted(horses, key=lambda x: int(x.get("pop", 999))):
        try:
            pop = int(h.get("pop"))
            odds = float(h.get("odds"))
        except Exception:
            continue
        if pop == 1:
            continue  # 1番人気は軸なので除外
        if 10.0 <= odds <= 20.0:
            filtered.append(pop)
        if len(filtered) >= 4:
            break
    return filtered

def _tickets_perm_with_axis(axis_pop: int, candidates: List[int]) -> List[str]:
    """
    1着を axis_pop に固定し、candidates から 2着・3着の順列を列挙（同一馬重複なし）。
    表記は「pop番号」で統一（例: "1-2-5"）。
    """
    tickets = []
    for a in candidates:
        for b in candidates:
            if a == b:
                continue
            tickets.append(f"{axis_pop}-{a}-{b}")
    return tickets

def _tickets_for(label: str) -> List[str]:
    # 既存（固定テンプレ）が必要なら残す。③は動的生成に切替えるので未使用でもOK。
    if label == "②":
        return ["1-2-3", "1-3-2"]
    if label == "①":
        return ["1-2-3", "1-3-2", "2-1-3", "2-3-1", "3-1-2", "3-2-1"]
    if label == "④":
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

    # ③（更新版）：
    #   条件：
    #     - 1番人気 <= 2.0
    #     - 2番人気 >= 10.0
    #     - 相手候補：単勝オッズが[10.0, 20.0]の馬（1番人気除く）を人気順に最大4頭
    #     - 相手候補が2頭以上（= 3連単の2,3着を埋められる）
    #   買い目：
    #     - 三連単 1番人気（1着固定）→ 相手候補 → 相手候補（重複なし）の順列
    cond3 = False
    tickets3: List[str] = []
    if (o1 is not None and o1 <= 2.0) and (o2 is not None and o2 >= 10.0):
        cands = _pick_candidates_10_20(horses)  # popのリスト、最大4
        if logger:
            logger.info(f"[DEBUG] strategy③ candidates (pop): {cands}")
        if len(cands) >= 2:
            cond3 = True
            tickets3 = _tickets_perm_with_axis(1, cands)

    # ④ 既存：1位<=3.0, 2位<=3.0, 3位が[6.0,10.0], 4位>=15.0
    cond4 = (
        (o1 is not None and o1 <= 3.0) and
        (o2 is not None and o2 <= 3.0) and
        (o3 is not None and 6.0 <= o3 <= 10.0) and
        (o4 is not None and o4 >= 15.0)
    )

    # デバッグ：各条件の評価結果
    if logger:
        logger.info("[DEBUG] checks → ①=%s, ②=%s, ③=%s, ④=%s", cond1, cond2, cond3, cond4)

    # 優先順位：②→①→③→④（現状維持）
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
            "strategy": "③ 1軸 — 相手10〜20倍（最大4頭）",
            "tickets": tickets3,
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