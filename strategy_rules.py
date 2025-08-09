# strategy_rules.py
# 戦略①〜④の条件判定と、通知用の候補整形
from typing import Dict, Any, List, Optional
from jockey_rank import get_jrank

def _top(entries, n=4):
    # 人気順で来ている前提。なければソートが必要
    return entries[:n] if len(entries) >= n else entries[:]

def _find_by_pop(entries, p):
    for e in entries:
        if e["pop"] == p:
            return e
    return None

# --- 戦略条件 ---
def is_strategy_1(entries: List[Dict[str, Any]]) -> bool:
    """
    ① 1〜3番人気BOX
    条件：
      1番人気オッズ: 2.0〜10.0
      2〜3番人気: 10.0未満
      4番人気: 15.0以上
    """
    if len(entries) < 4:
        return False
    e1 = _find_by_pop(entries, 1)
    e2 = _find_by_pop(entries, 2)
    e3 = _find_by_pop(entries, 3)
    e4 = _find_by_pop(entries, 4)
    if not all([e1, e2, e3, e4]):
        return False
    return (2.0 <= e1["odds"] <= 10.0) and (e2["odds"] < 10.0) and (e3["odds"] < 10.0) and (e4["odds"] >= 15.0)

def is_strategy_2(entries: List[Dict[str, Any]]) -> bool:
    """
    ② 1番人気1着固定 × 2・3番人気（2点）
    条件：
      1番人気: 2.0未満
      2〜3番人気: 10.0未満
    """
    if len(entries) < 3:
        return False
    e1 = _find_by_pop(entries, 1)
    e2 = _find_by_pop(entries, 2)
    e3 = _find_by_pop(entries, 3)
    if not all([e1, e2, e3]):
        return False
    return (e1["odds"] < 2.0) and (e2["odds"] < 10.0) and (e3["odds"] < 10.0)

def is_strategy_3(entries: List[Dict[str, Any]]) -> bool:
    """
    ③ 1着固定 × 10〜20倍流し（相手：2番人気以下で10〜20倍）
    条件：
      1番人気オッズ: 1.5以下
      相手：2番人気以下で 単勝 10〜20倍 の馬（最大5頭に丸める）
    """
    if len(entries) < 2:
        return False
    e1 = _find_by_pop(entries, 1)
    if not e1:
        return False
    # 相手が1頭もいないと通知価値が低いので False
    return (e1["odds"] <= 1.5) and any(10.0 <= e.get("odds", 0) <= 20.0 for e in entries[1:])

def is_strategy_4(entries: List[Dict[str, Any]]) -> bool:
    """
    ④ 3着固定（3番人気固定）2点
    条件：
      1・2番人気: 3.0以下
      3番人気: 6.0〜10.0
      4番人気: 15.0以上
    """
    if len(entries) < 4:
        return False
    e1 = _find_by_pop(entries, 1)
    e2 = _find_by_pop(entries, 2)
    e3 = _find_by_pop(entries, 3)
    e4 = _find_by_pop(entries, 4)
    if not all([e1, e2, e3, e4]):
        return False
    return (e1["odds"] <= 3.0) and (e2["odds"] <= 3.0) and (6.0 <= e3["odds"] <= 10.0) and (e4["odds"] >= 15.0)

# --- 通知用の候補整形（最大5頭 & 騎手ランク付与は main.build_message で実施） ---

def build_candidates_strategy_1(entries: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    # 1〜3番人気の3頭
    out = []
    for p in (1,2,3):
        e = _find_by_pop(entries, p)
        if e:
            out.append({"num": e["num"], "horse": e["horse"], "jockey": e["jockey"]})
    return out

def build_candidates_strategy_2(entries: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    # 1番人気(軸) + 2/3番人気
    out = []
    for p in (1,2,3):
        e = _find_by_pop(entries, p)
        if e:
            out.append({"num": e["num"], "horse": e["horse"], "jockey": e["jockey"]})
    return out

def build_candidates_strategy_3(entries: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    # 軸：1番人気、相手：2番人気以下で 10〜20倍 の馬 → 最大5頭
    out = []
    e1 = _find_by_pop(entries, 1)
    if e1:
        out.append({"num": e1["num"], "horse": e1["horse"], "jockey": e1["jockey"]})
    others = []
    for e in entries[1:]:
        if 10.0 <= e["odds"] <= 20.0:
            others.append({"num": e["num"], "horse": e["horse"], "jockey": e["jockey"]})
    return (out + others)[:5]

def build_candidates_strategy_4(entries: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    # 3番人気を3着固定 → 1,2,3番人気の並び強調（通知は候補3頭）
    out = []
    for p in (1,2,3):
        e = _find_by_pop(entries, p)
        if e:
            out.append({"num": e["num"], "horse": e["horse"], "jockey": e["jockey"]})
    return out