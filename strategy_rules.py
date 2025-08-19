# -*- coding: utf-8 -*-
"""
戦略①〜④の判定ロジック（定数化＋堅牢化＋詳細デバッグ）
- 入力: horses = [{"pop":1, "odds":2.4, "num": 4, "rank":"A" ...}, ...]
- 出力: 戦略一致ディクショナリ（呼び出し側仕様に合わせて match/id を含む）
    一致: {
        "match": True,
        "id": "S2",                     # "S1"|"S2"|"S3"|"S4"
        "label": "② 1番人気1着固定 × 2・3番人気（2点）",
        "tickets": [...],               # ③は馬番ベース/欠損時は人気フォールバック
        "roi": "想定回収率: —",
        "hit": "的中率: —",
    }
    不一致: {"match": False, "why": "..."} または None
"""

from typing import Dict, List, Optional, Any

# ====== 調整しやすい閾値定義 ======
S1_O1_MIN, S1_O1_MAX = 2.0, 10.0       # ①: 1番人気 ∈ [2.0, 10.0)
S1_O23_MAX = 10.0                      # ①: 2,3番人気 < 10.0
S1_O4_MIN  = 15.0                      # ①: 4番人気 >= 15.0

S2_O1_MAX  = 2.0                       # ②: 1番人気 < 2.0
S2_O23_MAX = 10.0                      # ②: 2,3番人気 < 10.0
S2_O4_MIN  = 12.0                      # ②: 4番人気 >= 12.0（★修正点）

S3_O1_MAX  = 2.0                       # ③: 1番人気 <= 2.0
S3_O2_MIN  = 10.0                      # ③: 2番人気 >= 10.0
S3_CAND_MIN, S3_CAND_MAX = 10.0, 20.0  # ③: 相手 10.0〜20.0
S3_CAND_MAX_COUNT = 4                  # ③: 最大4頭
S3_CAND_MIN_COUNT = 2                  # ③: 最低2頭

S4_O1_MAX  = 3.0                       # ④: 1番人気 <= 3.0
S4_O2_MAX  = 3.0                       # ④: 2番人気 <= 3.0
S4_O3_MIN, S4_O3_MAX = 6.0, 10.0       # ④: 3番人気 ∈ [6.0,10.0]
S4_O4_MIN  = 15.0                      # ④: 4番人気 >= 15.0

PRIORITY = ("S2", "S1", "S3", "S4")    # 優先順位（②→①→③→④）

def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None

def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None

def _odds_map(horses: List[Dict]) -> Dict[int, float]:
    """人気→単勝オッズ（1始まり）。最初に見た人気を採用。"""
    m: Dict[int, float] = {}
    for h in horses:
        p = _safe_int(h.get("pop"))
        o = _safe_float(h.get("odds"))
        if p is None or o is None:
            continue
        if p not in m:
            m[p] = o
    return m

def _fmt(o: Optional[float]) -> str:
    return "—" if o is None else f"{o:.1f}"

def _pick_candidates_10_20(horses: List[Dict]) -> List[Dict]:
    """相手候補（10〜20倍、1番人気除外）を人気昇順で最大4頭。"""
    out: List[Dict] = []
    for h in sorted(horses, key=lambda x: _safe_int(x.get("pop")) or 10**9):
        pop = _safe_int(h.get("pop"))
        odds = _safe_float(h.get("odds"))
        if pop is None or odds is None:
            continue
        if pop == 1:
            continue
        if S3_CAND_MIN <= odds <= S3_CAND_MAX:
            num = h.get("num")
            umaban = num if isinstance(num, int) else _safe_int(num)
            out.append({"pop": pop, "odds": odds, "umaban": umaban})
            if len(out) >= S3_CAND_MAX_COUNT:
                break
    return out

def _tickets_perm_with_axis_num(axis_umaban: Optional[int], cand_umanums: List[int]) -> List[str]:
    """axisを1着固定、候補から順列（重複なし）で2・3着。馬番表記。"""
    if axis_umaban is None or len(cand_umanums) < 2:
        return []
    tickets: List[str] = []
    for i in range(len(cand_umanums)):
        for j in range(len(cand_umanums)):
            if i == j:
                continue
            a, b = cand_umanums[i], cand_umanums[j]
            tickets.append(f"{axis_umaban}-{a}-{b}")
    return tickets

def _tickets_for(label: str) -> List[str]:
    """固定テンプレ（人気表記）。③は動的生成。"""
    if label == "S2":
        return ["1-2-3", "1-3-2"]
    if label == "S1":
        return ["1-2-3", "1-3-2", "2-1-3", "2-3-1", "3-1-2", "3-2-1"]
    if label == "S4":
        return ["1-2-3", "2-1-3"]
    return []

def eval_strategy(horses: List[Dict], logger=None) -> Optional[Dict]:
    """
    horses から上位人気オッズを取り、戦略①〜④のいずれかに合致すれば
    {"match": True, "id": "Sx", "label": "...", "tickets": [...], roi, hit, axis?, candidates?}
    を返す。合致しなければ {"match": False, "why": "..."} を返す。
    """
    o = _odds_map(horses)
    o1, o2, o3, o4 = o.get(1), o.get(2), o.get(3), o.get(4)

    # --- デバッグ（崩れにくい1行ログ） ---
    if logger:
        try:
            logger.info("[DEBUG] odds top4 -> o1=%s o2=%s o3=%s o4=%s", _fmt(o1), _fmt(o2), _fmt(o3), _fmt(o4))
        except Exception:
            pass

    # 各条件
    cond1 = (
        (o1 is not None and S1_O1_MIN <= o1 < S1_O1_MAX) and
        (o2 is not None and o2 < S1_O23_MAX) and
        (o3 is not None and o3 < S1_O23_MAX) and
        (o4 is not None and o4 >= S1_O4_MIN)
    )
    cond2 = (
        (o1 is not None and o1 < S2_O1_MAX) and
        (o2 is not None and o2 < S2_O23_MAX) and
        (o3 is not None and o3 < S2_O23_MAX) and
        (o4 is not None and o4 >= S2_O4_MIN)   # ★ >=12.0
    )
    cond4 = (
        (o1 is not None and o1 <= S4_O1_MAX) and
        (o2 is not None and o2 <= S4_O2_MAX) and
        (o3 is not None and S4_O3_MIN <= o3 <= S4_O3_MAX) and
        (o4 is not None and o4 >= S4_O4_MIN)
    )

    # ③（動的生成）
    cond3, tickets3 = False, []
    candidates3: List[Dict] = []
    axis_info3: Optional[Dict] = None
    if (o1 is not None and o1 <= S3_O1_MAX) and (o2 is not None and o2 >= S3_O2_MIN):
        candidates3 = _pick_candidates_10_20(horses)
        if logger:
            try:
                logger.info("[DEBUG] strategy③ candidates(pop) -> %s", [c["pop"] for c in candidates3])
            except Exception:
                pass
        if len(candidates3) >= S3_CAND_MIN_COUNT:
            cond3 = True
            # 軸（1番人気）の馬番
            axis_umaban = None
            for h in horses:
                if _safe_int(h.get("pop")) == 1:
                    num = h.get("num")
                    axis_umaban = num if isinstance(num, int) else _safe_int(num)
                    break
            # 馬番で買い目生成（足りなければ人気フォールバック）
            cand_umanums = [c.get("umaban") for c in candidates3 if c.get("umaban") is not None]
            tickets3 = _tickets_perm_with_axis_num(axis_umaban, cand_umanums)
            if not tickets3:
                cand_pops = [c["pop"] for c in candidates3]
                tickets3 = [f"1-{a}-{b}" for i, a in enumerate(cand_pops) for j, b in enumerate(cand_pops) if i != j]
            axis_info3 = {"pop": 1, "umaban": axis_umaban, "odds": o1}

    # --- 条件真偽（②の内訳も出す） ---
    if logger:
        try:
            logger.info(
                "[DEBUG] checks -> ①=%s ②=%s ③=%s ④=%s | ②(o1<2=%s o2<10=%s o3<10=%s o4>=12=%s)",
                cond1, cond2, cond3, cond4,
                (o1 is not None and o1 < S2_O1_MAX),
                (o2 is not None and o2 < S2_O23_MAX),
                (o3 is not None and o3 < S2_O23_MAX),
                (o4 is not None and o4 >= S2_O4_MIN),
            )
        except Exception:
            pass

    # ===== 優先順位：②→①→③→④ =====
    if cond2:
        return {
            "match": True,
            "id": "S2",
            "label": "② 1番人気1着固定 × 2・3番人気（2点）",
            "tickets": _tickets_for("S2"),
            "roi": "想定回収率: —",
            "hit": "的中率: —",
        }
    if cond1:
        return {
            "match": True,
            "id": "S1",
            "label": "① 1〜3番人気BOX（条件内）",
            "tickets": _tickets_for("S1"),
            "roi": "想定回収率: —",
            "hit": "的中率: —",
        }
    if cond3:
        return {
            "match": True,
            "id": "S3",
            "label": "③ 1軸 — 相手10〜20倍（最大4頭）",
            "tickets": tickets3,
            "axis": axis_info3,
            "candidates": candidates3,
            "roi": "想定回収率: —",
            "hit": "的中率: —",
        }
    if cond4:
        return {
            "match": True,
            "id": "S4",
            "label": "④ (1,2)→(1,2)→3 固定 + 4番人気高配狙い",
            "tickets": _tickets_for("S4"),
            "roi": "想定回収率: —",
            "hit": "的中率: —",
        }

    # 不一致（理由を軽く残す）
    why = []
    if o1 is not None and o2 is not None and o3 is not None and o4 is not None:
        why.append(f"o1={_fmt(o1)},o2={_fmt(o2)},o3={_fmt(o3)},o4={_fmt(o4)}")
    return {"match": False, "why": "; ".join(why) or "-"}