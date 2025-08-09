# jockey_rank.py
# 騎手ランクCSV(data/jockey_ranks.csv)を読み込んで
# 騎手名 -> {"belong":..., "jrank": "A|B|C"} を返すユーティリティ

import csv
import os

CSV_PATH_DEFAULT = "data/jockey_ranks.csv"

# 日本語/英語どちらのヘッダでも読めるように候補を定義
_HEADER_MAP = {
    "rank":   ["Rank", "順位", "ランク"],
    "name":   ["Name", "騎手名", "ジョッキー"],
    "belong": ["Belong", "所属"],
}

def _pick(row, keys):
    for k in keys:
        if k in row and row[k] is not None:
            return str(row[k]).strip()
    raise KeyError(f"Missing columns. tried={keys}, got={list(row.keys())}")

def _normalize_name(name: str) -> str:
    # 前後空白や連続スペースを整理（必要なら追加で正規化）
    return " ".join(name.split())

def load_jrank(csv_path: str = CSV_PATH_DEFAULT) -> dict:
    """
    CSV を読み込み、 {騎手名: {"belong": 所属, "jrank": A/B/C}} を返す
    CSVが無い/壊れている場合は空dict
    """
    table = {}
    if not os.path.exists(csv_path):
        return table

    with open(csv_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                name   = _normalize_name(_pick(row, _HEADER_MAP["name"]))
                belong = _pick(row, _HEADER_MAP["belong"])
                jrank  = row.get("jrank") or row.get("JRank")  # 既に列がある場合
                if not jrank:
                    # ランク列が無いCSVでも、順位から即席ランク付けできるように
                    rank_str = _pick(row, _HEADER_MAP["rank"])
                    rank = int(rank_str)
                    if rank <= 70:
                        jrank = "A"
                    elif rank <= 150:
                        jrank = "B"
                    else:
                        jrank = "C"
                # ばんえい所属は除外（そもそもCSVに入れない運用だがガード）
                if "ばんえ" in belong or "ばんえい" in belong:
                    continue
                table[name] = {"belong": belong, "jrank": jrank}
            except Exception:
                # 空行/合わない行はスキップ
                continue
    return table

# モジュール読み込み時に一度だけロード（必要なら再読込APIも用意）
_JRANK_CACHE = None

def _ensure_loaded():
    global _JRANK_CACHE
    if _JRANK_CACHE is None:
        _JRANK_CACHE = load_jrank()

def get_jrank(name: str, default: str = "C") -> str:
    """
    騎手名から A/B/C を返す。見つからなければ default（既定C）
    """
    _ensure_loaded()
    key = _normalize_name(name or "")
    info = _JRANK_CACHE.get(key) if _JRANK_CACHE else None
    return (info or {}).get("jrank", default)

def get_info(name: str) -> dict | None:
    """
    騎手名から {"belong":..,"jrank":..} を返す（無ければNone）
    """
    _ensure_loaded()
    key = _normalize_name(name or "")
    return (_JRANK_CACHE or {}).get(key)