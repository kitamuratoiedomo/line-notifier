# scripts/build_jockey_ranks.py
import csv
import re
import time
import sys
from typing import List, Tuple

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://db.netkeiba.com/jockey/jockey_leading_nar.html"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
    ),
    "Accept-Language": "ja,en;q=0.8",
}

def pick_table(soup: BeautifulSoup):
    """ヘッダに '騎手' と '複勝' を含むテーブルを1つ選ぶ"""
    tables = soup.find_all("table")
    for tbl in tables:
        ths = [th.get_text(strip=True) for th in tbl.find_all("th")]
        if not ths:
            continue
        if any("騎手" in h or "騎手名" in h for h in ths) and any("複勝" in h for h in ths):
            return tbl
    return None

def parse_table(tbl) -> List[Tuple[str, float]]:
    """(騎手名, 複勝率[0-1]) のリストに変換"""
    out = []
    for tr in tbl.find_all("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(tds) < 5:
            continue
        # 想定：順位, 騎手名, 所属, 乗鞍, 勝率, 連対率, 複勝率, ...
        text_row = " ".join(tds)
        if "騎手" in text_row and "複勝" in text_row:
            # ヘッダー行をスキップ
            continue

        jockey = tds[1]  # 2番目が騎手名の想定
        # 右側から%が入るセルを探す（列ズレ耐性）
        fuku_cell = None
        for td in tds[::-1]:
            if "%" in td:
                fuku_cell = td
                break
        if not fuku_cell:
            continue
        m = re.search(r"(\d+(?:\.\d+)?)\s*%", fuku_cell)
        if not m:
            continue
        rate = float(m.group(1)) / 100.0
        out.append((jockey, rate))
    return out

def rank(rate: float) -> str:
    # 指定の基準：A=30%以上 / B=20〜29.9% / C=19.9%以下
    if rate >= 0.30:
        return "A"
    if rate >= 0.20:
        return "B"
    return "C"

def fetch_all_pages() -> List[Tuple[str, float]]:
    """ページング対応 (?page=2 ...)。表が無くなれば終了。"""
    results: List[Tuple[str, float]] = []
    for page in range(1, 11):  # 予備で10ページまで
        url = BASE_URL if page == 1 else f"{BASE_URL}?page={page}"
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        tbl = pick_table(soup)
        if not tbl:
            break
        rows = parse_table(tbl)
        if not rows:
            break
        results.extend(rows)
        time.sleep(1.0)  # マナー
    # 重複名があれば先勝ちでユニーク化
    uniq = {}
    for name, rate in results:
        uniq.setdefault(name, rate)
    return [(k, v) for k, v in uniq.items()]

def main():
    rows = fetch_all_pages()
    rows.sort(key=lambda x: x[0])
    out_path = "data/jockey_ranks.csv"
    print(f"write: {out_path} ({len(rows)} rows)")
    # data/ フォルダはActions側で作成せずともCSV書き込みでOK（git管理は後段のcommitステップ）
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["jockey_name", "fukusho_rate", "rank"])
        for name, rate in rows:
            w.writerow([name, f"{rate:.3f}", rank(rate)])

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        raise