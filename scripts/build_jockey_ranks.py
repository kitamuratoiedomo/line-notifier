# -*- coding: utf-8 -*-
"""
netkeiba 地方(NAR) 騎手リーディングの「複勝率(=3着内率相当)」を取得し、
A/B/C/D ランクを付けて data/jockey_ranks.csv を出力。

ランク:
  A: 30%以上
  B: 20%以上 30%未満
  C: 15%以上 20%未満
  D: 15%未満 or 欠損
"""

import os, re, csv, sys, time, datetime as dt
from typing import List, Optional, Tuple
import requests
from bs4 import BeautifulSoup

# ★ まずは “公式パラメータURL” を優先
CANDIDATE_URLS = [
    "https://db.netkeiba.com/?pid=jockey_leading&mode=nar",      # 新
    "https://db.netkeiba.com/?pid=jockey_leading&year=&mode=nar",# 年指定空
    "https://db.netkeiba.com/jockey/jockey_leading_nar.html",    # 旧
]

OUT_DIR = "data"
OUT_PATH = os.path.join(OUT_DIR, "jockey_ranks.csv")

UA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Referer": "https://db.netkeiba.com/",
}

FUKU_KEYS = ("複勝", "複勝率", "3着内", "３着内")
NAME_KEYS = ("騎手名", "騎手")

ARTIFACT_PATH = "/tmp/jockey_leading.html"

def fetch_html_first_success(urls) -> Tuple[str, str]:
    last_err = None
    for url in urls:
        try:
            r = requests.get(url, headers=UA_HEADERS, timeout=25)
            r.raise_for_status()
            # meta が euc-jp 指定なので合わせる（ページによってはUTF-8の日もあるがまずEUCを優先）
            r.encoding = "euc-jp"
            html = r.text
            # 解析用に保存（常に上書き）
            try:
                with open(ARTIFACT_PATH, "w", encoding="euc-jp", errors="ignore") as f:
                    f.write(html)
            except Exception:
                pass
            print(f"[info] fetched: {url}  size={len(html)}")
            return html, url
        except Exception as e:
            last_err = e
            print(f"[warn] fetch failed: {url}  err={e}")
            time.sleep(0.8)
    raise RuntimeError(f"all fetch failed: {last_err}")

def headers_of_table(tbl) -> List[str]:
    thead = tbl.find("thead")
    if thead:
        cells = thead.find_all(["th", "td"])
    else:
        first = tbl.find("tr")
        cells = first.find_all(["th", "td"]) if first else []
    return [re.sub(r"\s+", "", c.get_text()) for c in cells]

def pct_to_float(s: str):
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%?", s)
    return float(m.group(1)) / 100.0 if m else ""

def pick_candidate_table(soup: BeautifulSoup) -> Tuple[Optional[object], List[str]]:
    all_tables = soup.find_all("table")
    print(f"[diag] table count: {len(all_tables)}")

    best_tbl = None
    best_hdr: List[str] = []
    best_score = -1

    def score(headers: List[str]) -> int:
        sc = 0
        joined = " ".join(headers)
        if any(k in joined for k in NAME_KEYS): sc += 2
        if any(k in joined for k in FUKU_KEYS): sc += 3
        if any("勝率" in h for h in headers): sc += 1
        if any("連対" in h for h in headers): sc += 1
        return sc

    for idx, tbl in enumerate(all_tables, start=1):
        hdr = headers_of_table(tbl)
        print(f"[diag] table#{idx} headers: {','.join(hdr[:12])}")
        sc = score(hdr)
        if sc > best_score and sc >= 4:  # ある程度スコアが高いもの
            best_tbl, best_hdr, best_score = tbl, hdr, sc

    return best_tbl, best_hdr

def parse_rows(tbl, headers: List[str]) -> List[dict]:
    # 列位置（部分一致で探す）
    def find_col(cands):
        for i, h in enumerate(headers):
            for c in cands:
                if c in h:
                    return i
        return None

    idx_name = find_col(NAME_KEYS)
    idx_fuku = find_col(FUKU_KEYS)      # 複勝/3着内
    idx_win  = find_col(("勝率",))
    idx_rens = find_col(("連対率","連対"))

    rows = []
    for tr in tbl.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if not tds:
            continue
        texts = [td.get_text(strip=True) for td in tds]

        # 見出し行の再出現スキップ
        joined = re.sub(r"\s+", "", "".join(texts))
        if any(k in joined for k in NAME_KEYS) and any(k in joined for k in FUKU_KEYS):
            continue

        def get(i):
            if i is None: return ""
            return texts[i] if i < len(texts) else ""

        name = re.sub(r"\s+", " ", get(idx_name))
        fuku = pct_to_float(get(idx_fuku))
        if not name or fuku == "":
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
    html, used_url = fetch_html_first_success(CANDIDATE_URLS)
    print(f"[info] parsing from: {used_url}")

    # lxml が入っていれば使う。なければhtml.parserでOK
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    tbl, headers = pick_candidate_table(soup)
    if not tbl:
        preview = re.sub(r"\s+"," ", html[:800])
        print(f"[debug] preview: {preview}")
        print(f"[hint] downloaded HTML at: {ARTIFACT_PATH}")
        raise RuntimeError("table not found")

    rows = parse_rows(tbl, headers)
    if not rows:
        print(f"[hint] headers used: {headers}")
        print(f"[hint] downloaded HTML at: {ARTIFACT_PATH}")
        raise RuntimeError("parsed 0 rows")

    save_csv(rows, OUT_PATH)
    print(f"[ok] saved: {OUT_PATH} ({len(rows)} rows)")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)