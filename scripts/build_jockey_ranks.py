# -*- coding: utf-8 -*-
"""
netkeiba 地方(NAR) 騎手リーディングから「複勝率（=3着内率）」を取得しランク化してCSV出力します。

ランク:
  A: 30%以上
  B: 20%以上 30%未満
  C: 15%以上 20%未満
  D: 15%未満 or 欠損

出力: data/jockey_ranks.csv
列  : jockey, fuku, win, rens, rank, asof
"""

import os, re, csv, sys, time, datetime as dt
from typing import List, Optional, Tuple
import requests
from bs4 import BeautifulSoup

URL = "https://db.netkeiba.com/jockey/jockey_leading_nar.html"
OUT_DIR = "data"
OUT_PATH = os.path.join(OUT_DIR, "jockey_ranks.csv")

UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
    "Mobile/15E148 Safari/604.1"
)

def fetch_html(url: str, retry: int = 3, sleep_sec: float = 1.0) -> str:
    last = None
    for _ in range(retry):
        try:
            r = requests.get(
                url,
                headers={
                    "User-Agent": UA,
                    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
                    "Referer": "https://db.netkeiba.com/",
                },
                timeout=25,
            )
            r.raise_for_status()
            # ページは EUC-JP。明示指定。
            r.encoding = "euc-jp"
            return r.text
        except Exception as e:
            last = e
            time.sleep(sleep_sec)
    raise last

# ------------------ テーブル検出をかなり緩くする ------------------
FUKU_KEYS = ("複勝", "複勝率", "3着内", "３着内")
NAME_KEYS = ("騎手名", "騎手")

def headers_of_table(tbl) -> List[str]:
    # thead優先、なければ最初のtr
    thead = tbl.find("thead")
    if thead:
        cells = thead.find_all(["th","td"])
    else:
        first = tbl.find("tr")
        cells = first.find_all(["th","td"]) if first else []
    return [re.sub(r"\s+", "", c.get_text()) for c in cells]

def table_score(headers: List[str]) -> int:
    """ヘッダの ‘それっぽさ’ を点数化。高いほど有力。"""
    score = 0
    if any(k in "".join(headers) for k in NAME_KEYS): score += 2
    if any(k in "".join(headers) for k in FUKU_KEYS): score += 3
    if any("勝率" in h for h in headers): score += 1
    if any("連対" in h for h in headers): score += 1
    return score

def pick_candidate_table(soup: BeautifulSoup) -> Tuple[Optional[object], List[str], List[List[str]]]:
    """候補テーブルを全部採点し、最もスコアが高いものを返す。併せてデバッグ用に上位の見出し一覧も返す。"""
    scored = []
    for tbl in soup.find_all("table"):
        headers = headers_of_table(tbl)
        # 列数・行数も一応チェック（データテーブルっぽいもの）
        row_cnt = len(tbl.find_all("tr"))
        col_cnt = len(headers)
        if row_cnt >= 5 and col_cnt >= 4:
            scored.append((table_score(headers), headers, tbl))
    scored.sort(key=lambda x: x[0], reverse=True)
    if not scored:
        return None, [], []
    best = scored[0]
    debug_headers = [h for _, h, _ in scored[:5]]
    return best[2], best[1], debug_headers

def pct_to_float(s: str):
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%?", s)
    return float(m.group(1)) / 100.0 if m else ""

def parse_rows(tbl, headers: List[str]) -> List[dict]:
    # 列インデックスを「部分一致」で探す
    def find_col(cands):
        for i, h in enumerate(headers):
            for c in cands:
                if c in h:
                    return i
        return None

    idx_name = find_col(NAME_KEYS)
    idx_fuku = find_col(FUKU_KEYS)  # 複勝/3着内
    idx_win  = find_col(("勝率",))
    idx_rens = find_col(("連対率","連対"))

    rows = []
    for tr in tbl.find_all("tr"):
        tds = tr.find_all(["td","th"])
        if not tds: continue
        texts = [td.get_text(strip=True) for td in tds]

        # 見出し行（再出現）をスキップ
        joined = re.sub(r"\s+","", "".join(texts))
        if any(k in joined for k in NAME_KEYS) and any(k in joined for k in FUKU_KEYS):
            continue

        def get(i):
            if i is None: return ""
            return texts[i] if i < len(texts) else ""

        name = re.sub(r"\s+"," ", get(idx_name))
        fuku = pct_to_float(get(idx_fuku))
        if not name:           # 名前なければ無効
            continue
        if fuku == "":         # 複勝(3着内)が取れない行も無効
            continue

        rows.append({
            "jockey": name,
            "fuku": fuku,
            "win":  pct_to_float(get(idx_win)),
            "rens": pct_to_float(get(idx_rens)),
        })
    return rows

def rank_of(fuku):
    if fuku == "": return "D"
    if fuku >= 0.30: return "A"
    if fuku >= 0.20: return "B"
    if fuku >= 0.15: return "C"
    return "D"

def save_csv(items: List[dict], path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    today = dt.date.today().isoformat()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["jockey","fuku","win","rens","rank","asof"])
        for r in items:
            w.writerow([r["jockey"], r["fuku"], r["win"], r["rens"], rank_of(r["fuku"]), today])

def main():
    html = fetch_html(URL)
    soup = BeautifulSoup(html, "html.parser")

    tbl, headers, debug_headers = pick_candidate_table(soup)
    if not tbl:
        preview = re.sub(r"\s+"," ", html[:500])
        raise RuntimeError(f"table not found. preview: {preview}")

    rows = parse_rows(tbl, headers)
    if not rows:
        # デバッグの助けになるよう、上位候補のヘッダを表示
        raise RuntimeError("parsed 0 rows. headers_top="
                           + " | ".join([",".join(h) for h in debug_headers[:3]]))

    save_csv(rows, OUT_PATH)
    print(f"[ok] saved: {OUT_PATH} ({len(rows)} rows)")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)