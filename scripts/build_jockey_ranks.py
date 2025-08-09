# -*- coding: utf-8 -*-
"""
netkeiba 地方(NAR) 騎手リーディングから「複勝率」を取得しランク化してCSV出力します。

ランク基準（ユーザー指定）:
  A: 複勝率 30%以上
  B: 20%以上 30%未満
  C: 15%以上 20%未満
  D: 15%未満

出力: data/jockey_ranks.csv
列  : jockey, fuku (0-1), win (0-1 or ''), rens (0-1 or ''), rank, asof(YYYY-MM-DD)
"""

import os
import re
import csv
import sys
import time
import datetime as dt
from typing import List, Tuple, Optional

import requests
from bs4 import BeautifulSoup

# 取得先（地方騎手リーディング）
URL = "https://db.netkeiba.com/jockey/jockey_leading_nar.html"

# 出力パス
OUT_DIR = "data"
OUT_PATH = os.path.join(OUT_DIR, "jockey_ranks.csv")


# -------------------------
# HTTP / 文字コードまわり
# -------------------------
def fetch_html(url: str, retry: int = 3, sleep_sec: float = 1.0) -> str:
    """EUC-JP(=euc-jp) を明示的に指定して取得する。"""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
            "Mobile/15E148 Safari/604.1"
        ),
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Referer": "https://db.netkeiba.com/",
    }
    last_err = None
    for _ in range(retry):
        try:
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            # ★ metaに euc-jp とあるので、明示的に合わせる
            r.encoding = "euc-jp"
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(sleep_sec)
    raise last_err


# -------------------------
# HTML 解析
# -------------------------
def pick_leading_table(soup: BeautifulSoup) -> Tuple[Optional[object], List[str]]:
    """
    「複勝」を含むセルから親tableを逆引き。さらにヘッダーに「騎手/騎手名」もあるものを採用。
    戻り値: (table要素 or None, 見出しテキスト配列)
    """
    cand_tables = set()

    # 「複勝」文字列を含むセルを探して親テーブル候補に
    for tag in soup.find_all(["th", "td"]):
        txt = re.sub(r"\s+", "", tag.get_text())
        if "複勝" in txt:  # 「複勝率」でもヒット
            tbl = tag.find_parent("table")
            if tbl:
                cand_tables.add(tbl)

    def headers_of(tbl) -> List[str]:
        # thead優先、なければ最初のtr
        thead = tbl.find("thead")
        if thead:
            hdr_tags = thead.find_all(["th", "td"])
        else:
            first_tr = tbl.find("tr")
            hdr_tags = first_tr.find_all(["th", "td"]) if first_tr else []
        return [re.sub(r"\s+", "", t.get_text()) for t in hdr_tags]

    chosen = None
    chosen_hdr: List[str] = []
    for tbl in cand_tables:
        hdr = headers_of(tbl)
        if not hdr:
            continue
        if (("騎手名" in hdr) or ("騎手" in hdr)) and any(("複勝" in h) for h in hdr):
            chosen = tbl
            chosen_hdr = hdr
            break

    return chosen, chosen_hdr


def parse_rows_from_table(tbl, headers: List[str]) -> List[dict]:
    """
    テーブル本体から行を抽出。
    見出し名は部分一致でゆるく解釈（例: "複勝率(%)" など）。
    返却: dictのリスト { jockey, win, rens, fuku }
    値は 0-1 のfloat（欠損は ''）。
    """
    # 見出し->index の辞書（部分一致OK）
    idx_map = {h: i for i, h in enumerate(headers)}

    def find_col(*cands):
        for h, i in idx_map.items():
            for c in cands:
                if c in h:  # 部分一致
                    return i
        return None

    idx_name = find_col("騎手名", "騎手")
    idx_win = find_col("勝率")
    idx_rens = find_col("連対率")
    idx_fuku = find_col("複勝")  # 「複勝率」「複勝」など

    rows = []
    for tr in tbl.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if len(tds) < 3:
            continue
        texts = [re.sub(r"\s+", " ", td.get_text(strip=True)) for td in tds]

        # 見出し行はスキップ
        joined = "".join(texts)
        if ("騎手" in joined) and ("複勝" in joined):
            continue

        def get(i):
            try:
                return texts[i] if i is not None and i < len(texts) else ""
            except Exception:
                return ""

        name = get(idx_name)
        fuku = get(idx_fuku)
        if not name or not fuku:
            continue

        def pct_to_float(s: str):
            m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%?", s)
            return float(m.group(1)) / 100.0 if m else ""

        d = {
            "jockey": name,
            "win": pct_to_float(get(idx_win)),
            "rens": pct_to_float(get(idx_rens)),
            "fuku": pct_to_float(fuku),
        }
        rows.append(d)

    return rows


# -------------------------
# ランク判定 & CSV保存
# -------------------------
def judge_rank(fuku: Optional[float]) -> str:
    """A/B/C/D を返す。fukuは0-1 or ''。"""
    if fuku == "":
        return "D"
    # ユーザー指定の基準
    if fuku >= 0.30:
        return "A"
    if fuku >= 0.20:
        return "B"
    if fuku >= 0.15:
        return "C"
    return "D"


def save_csv(rows: List[dict], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    today = dt.date.today().isoformat()

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["jockey", "fuku", "win", "rens", "rank", "asof"])
        for r in rows:
            rank = judge_rank(r.get("fuku"))
            w.writerow([
                r.get("jockey", ""),
                r.get("fuku", ""),
                r.get("win", ""),
                r.get("rens", ""),
                rank,
                today,
            ])


# -------------------------
# メイン
# -------------------------
def main():
    html = fetch_html(URL)
    soup = BeautifulSoup(html, "html.parser")

    table, headers = pick_leading_table(soup)
    if not table:
        # 解析デバッグ用に先頭を見せる
        preview = re.sub(r"\s+", " ", html[:500])
        raise RuntimeError(f"table not found. preview: {preview}")

    rows = parse_rows_from_table(table, headers)
    if not rows:
        raise RuntimeError("no rows parsed (header mismatch?)")

    save_csv(rows, OUT_PATH)
    print(f"[ok] saved: {OUT_PATH} ({len(rows)} rows)")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Actionsログで見やすいようにスタックも表示
        import traceback
        traceback.print_exc()
        sys.exit(1)