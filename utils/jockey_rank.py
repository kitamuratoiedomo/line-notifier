# utils/jockey_rank.py
# -*- coding: utf-8 -*-

import csv
import os
import unicodedata
from functools import lru_cache
from typing import Dict, Optional, Tuple

DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "jockey_ranks.csv")

def _normalize_name(name: str) -> str:
    """全角/半角・空白・敬称などをならして突き合わせ精度を上げる"""
    if not name:
        return ""
    s = unicodedata.normalize("NFKC", name)
    s = s.replace("　", " ").strip()
    # ありがちな接尾辞を除去
    for suf in ("騎手",):
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
    return s

@lru_cache(maxsize=1)
def _load_table() -> Dict[str, Tuple[int, str, str]]:
    """
    CSVを読み込み、キー=正規化した騎手名
    値=(順位, 所属, 元の表示名) を返す辞書を構築
    """
    table: Dict[str, Tuple[int, str, str]] = {}
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"CSV not found: {DATA_PATH}")

    with open(DATA_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # 期待するカラム名:
        # Rank, Name, Belong, W1, W2, W3, Others, Win, Quinella, Show, Prize
        # （あなたがアップしたCSVのヘッダに合わせてください）
        for row in reader:
            try:
                rank = int(row["Rank"])
            except Exception:
                # ヘッダ行/空行などはスキップ
                continue
            name = str(row["Name"]).strip()
            belong = str(row["Belong"]).strip()
            key = _normalize_name(name)
            table[key] = (rank, belong, name)
    return table

def get_rank_letter(jockey_name: str) -> Optional[str]:
    """
    騎手名から 'A' | 'B' | 'C' を返す。
    - ばんえい所属は None（対象外）
    - CSVに無い・特定できない場合は 'C'
    ルール:
      1–70位: A
      71–150位: B
      151位～ or 不明: C
    """
    key = _normalize_name(jockey_name)
    table = _load_table()

    info = table.get(key)
    if info is None:
        # マッチしない場合はC扱い（ただし明示的にNoneにしたい場合はここを変える）
        return "C"

    rank, belong, _ = info

    # ばんえい除外
    if "ばんえ" in belong or belong == "ばんえ":
        return None  # 対象外

    if 1 <= rank <= 70:
        return "A"
    elif 71 <= rank <= 150:
        return "B"
    else:
        return "C"

def debug_lookup(jockey_name: str) -> str:
    """デバッグ用：判定の根拠を文字列で返す"""
    key = _normalize_name(jockey_name)
    table = _load_table()
    info = table.get(key)
    if info is None:
        return f"{jockey_name} -> C (CSV未掲載/未一致)"
    rank, belong, orig = info
    letter = get_rank_letter(jockey_name)
    if letter is None:
        return f"{orig}({belong}) rank={rank} -> 対象外(ばんえい)"
    return f"{orig}({belong}) rank={rank} -> {letter}"