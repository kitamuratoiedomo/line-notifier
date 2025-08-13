# -*- coding: utf-8 -*-
"""
戦略①〜④の判定ロジック（デバッグログ付き）
- 入力: horses = [{"pop":1, "odds":2.4, "num": 4}, ...]
  * pop: 人気（1始まり）
  * odds: 単勝オッズ（float）
  * num: 馬番（int）  ※ページから取れない場合は欠損あり
- 出力: 合致した戦略のディクショナリ
    {
      "strategy": str,
      "tickets": List[str],          # 戦略③は馬番ベース "4-6-8" など（umaban）
      "axis": {"pop":1,"umaban":4,"odds":1.4},               # ③のみ
      "candidates": [{"pop":3,"umaban":6,"odds":12.3}, ...], # ③のみ（最大4頭）
      "roi": "想定回収率: —",
      "hit": "的中率: —",
    }
"""

from typing import Dict, List, Optional

def _odds_map(horses: List[Dict]) -> Dict[int, float]:
    """人気→単勝オッズの辞書（1始まり）。"""
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

def _pick_candidates_10_20(horses: List[Dict]) -> List[Dict]:
    """
    相手候補：単勝オッズが10.0〜20.0（両端含む）の馬を人気昇順で抽出（1番人気は除外）。
    戻り値は辞書の配列: [{"pop":pop, "odds":odds, "umaban":num}, ...]
    最大4頭に制限。
    """
    out = []
    for h in sorted(horses, key=lambda x: int(x.get("pop", 999))):
        try:
            pop = int(h.get("pop"))
            odds = float(h.get("odds"))
        except Exception:
            continue
        if pop == 1:
            continue
        if 10.0 <= odds <= 20.0:
            umaban = h.get("num") if isinstance(h.get("num"), int) else None
            out.append({"pop": pop, "odds": odds, "umaban": umaban})
            if len(out) >= 4:
                break
    return out

def _tickets_perm_with_axis_num(axis_umaban: Optional[int], cand_umanums: List[int]) -> List[str]:
    """
    1着を axis_umaban に固定し、cand_umanums から 2着・3着の順列を列挙（同一馬重複なし）。
    馬番ベースで "4-6-8" の形式を返す。axisが不明 or 相手<2頭なら空配列。
    """
    if axis_umaban is None or len(cand_umanums) < 2:
        return []
    tickets: List[str] = []
    for i in range(len(cand_umanums)):
        for j in range(len(cand_umanums)):
            if i == j:
                continue
            a = cand_umanums[i]
            b = cand_umanums[j]
            tickets.append(f"{axis_umaban}-{a}-{b}")
    return tickets

def _tickets_for(label: str) -> List[str]:
    """固定テンプレ（①②④用）。③は動的生成のため未使用。"""
    if label == "②":
        return ["1-2-3", "1-3-2"]
    if label == "①":
        # 例：BOX6点など、仕様に合わせて
        return ["1-2-3", "1-3-2", "2-1-3", "2-3-1", "3-1-2", "3-2-1"]
    if label == "④":
        # 例：@ (1,2)→(1,2)→3 固定（ここは仮）
        return ["1-2-3", "2-1-3"]
    return []

def eval_strategy(horses: List[Dict], logger=None) -> Optional[Dict]:
    """
    horses から上位人気の単勝オッズを取り、戦略①〜④のいずれかに合致すれば
    {strategy, tickets, roi, hit, axis?, candidates?} を返す。合致しなければ None。
    """
    o = _odds_map(horses)
    o1, o2, o3, o4 = o.get(1), o.get(2), o.get(3), o.get(4)

    # デバッグ：人気上位のオッズ出力
    if logger:
        try:
            logger.info(
                f"[DEBUG] odds top4 → 1位={_fmt(o1)}, 2位={_fmt(o2)}, 3位={_fmt(o3)}, 4位={_fmt(o4)}"
            )
        except Exception:
            pass

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

    # ③（更新版：方式A）
    #   条件：
    #     - 1番人気 <= 2.0
    #     - 2番人気 >= 10.0
    #     - 相手候補：単勝[10.0, 20.0]（1番人気除外）を人気昇順で最大4頭
    #     - 相手候補が2頭以上
    #   返却：
    #     - tickets: 馬番ベース（例 "4-6-8"）。馬番欠損時は人気ベースにフォールバック。
    cond3, tickets3 = False, []
    candidates3: List[Dict] = []
    axis_info3: Optional[Dict] = None

    if (o1 is not None and o1 <= 2.0) and (o2 is not None and o2 >= 10.0):
        candidates3 = _pick_candidates_10_20(horses)  # [{"pop","odds","umaban"}] 最大4
        if logger:
            try:
                logger.info(f"[DEBUG] strategy③ candidates (pop): {[c['pop'] for c in candidates3]}")
            except Exception:
                pass
        if len(candidates3) >= 2:
            cond3 = True

            # 軸（1番人気）umaban 取得
            axis_umaban = None
            for h in horses:
                try:
                    if int(h.get("pop")) == 1:
                        axis_umaban = h.get("num") if isinstance(h.get("num"), int) else None
                        break
                except Exception:
                    continue

            # 馬番で買い目生成（馬番が全部揃っていない場合は人気ベースにフォールバック）
            cand_umanums = [c.get("umaban") for c in candidates3 if c.get("umaban") is not None]
            tickets3 = _tickets_perm_with_axis_num(axis_umaban, cand_umanums)

            if not tickets3:
                # フォールバック（人気ベース: "1-2-5" 形式）
                cand_pops = [c["pop"] for c in candidates3]
                tickets3 = []
                for a in cand_pops:
                    for b in cand_pops:
                        if a == b:
                            continue
                        tickets3.append(f"1-{a}-{b}")

            axis_info3 = {"pop": 1, "umaban": axis_umaban, "odds": o1}

    # ④ 既存：1位<=3.0, 2位<=3.0, 3位が[6.0,10.0], 4位>=15.0
    cond4 = (
        (o1 is not None and o1 <= 3.0) and
        (o2 is not None and o2 <= 3.0) and
        (o3 is not None and 6.0 <= o3 <= 10.0) and
        (o4 is not None and o4 >= 15.0)
    )

    # デバッグ：各条件の評価結果
    if logger:
        try:
            logger.info("[DEBUG] checks → ①=%s, ②=%s, ③=%s, ④=%s", cond1, cond2, cond3, cond4)
        except Exception:
            pass

    # 優先順位：②→①→③→④（現状のまま）
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
            "tickets": tickets3,           # 馬番が取れれば馬番ベース／欠損時は人気ベース
            "axis": axis_info3,            # 例: {"pop":1,"umaban":4,"odds":1.4}
            "candidates": candidates3,     # 例: [{"pop":3,"umaban":6,"odds":12.3}, ...]（最大4）
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