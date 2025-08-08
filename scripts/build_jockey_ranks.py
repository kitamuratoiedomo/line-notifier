# scripts/build_jockey_ranks.py
# netkeiba 地方(NAR)リーディングの「複勝率」から騎手ランクCSVを作成
# 出力: data/jockey_ranks.csv
# ランク基準: A >=30% / B >=20% / C >=10% / D <10%

import os
import sys
import time
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup

URL = "https://db.netkeiba.com/jockey/jockey_leading_nar.html"
UA = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1"

OUT_DIR = "data"
OUT_PATH = os.path.join(OUT_DIR, "jockey_ranks.csv")

def fetch_html(url: str, retry: int = 3, sleep_sec: float = 1.5) -> str:
    last = None
    for i in range(retry):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=20)
            r.raise_for_status()
            # netkeiba 側の都合で文字コード宣言がズレる場合があるので任せる
            r.encoding = r.apparent_encoding or r.encoding
            return r.text
        except Exception as e:
            last = e
            time.sleep(sleep_sec)
    raise RuntimeError(f"request failed: {last}")

def find_table(soup: BeautifulSoup):
    """
    テーブルの候補を総当りで探す:
      - クラス名の変化に強くするため CSSセレクタを複数試す
      - ヘッダーに『騎手名』『複勝率』などが含まれるものを採用
    """
    selectors = [
        "table.db_h_race_results",            # 以前よく見かけたパターン
        "table.jockey_table",                 # 仮: 似た命名
        "table.race_table_01",                # netkeiba 汎用テーブル
        "table",                              # 最後の手段: すべてのtable
    ]
    header_keywords = ("騎手", "騎手名")
    fukusho_keywords = ("複勝率", "複勝")

    for sel in selectors:
        for tbl in soup.select(sel):
            # ヘッダー行っぽい<th>群を抽出
            th_text = " ".join(th.get_text(strip=True) for th in tbl.find_all("th"))
            if any(k in th_text for k in header_keywords) and any(k in th_text for k in fukusho_keywords):
                return tbl
    return None

def parse_table(tbl) -> pd.DataFrame:
    # ヘッダーを抽出
    headers = [th.get_text(strip=True) for th in tbl.find_all("th")]
    # 行を抽出
    rows = []
    for tr in tbl.find_all("tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not tds:
            continue
        rows.append(tds)

    if not rows:
        raise RuntimeError("no data rows found in table")

    # 列数が合わない場合は、よく使う列だけ拾う
    df = pd.DataFrame(rows)
    # 列名の当て込み（安全のため可変）
    # 期待: 「順位, 騎手名, 所属, 1着, 2着, 3着, 着外, 連対率, 複勝率, 勝率, ・・・」等
    # とにかく『騎手名』『複勝率』が取れれば良い
    name_col = None
    fuku_col = None

    # ヘッダから位置が分かれば使う
    if headers and len(headers) == df.shape[1]:
        for i, h in enumerate(headers):
            if "騎手" in h:
                name_col = i
            if "複勝" in h:
                fuku_col = i

    # 分からなければ推測: 文字列が長い列→騎手名、パーセント含む列→複勝率
    if name_col is None:
        name_col = df.apply(lambda s: s.str.len().fillna(0)).mean().idxmax()
    if fuku_col is None:
        pct_like = df.apply(lambda col: col.str.contains(r"%|％", regex=True, na=False)).sum()
        if (pct_like > 0).any():
            fuku_col = int(pct_like.idxmax())
        else:
            # 仕方ないので一番右っぽい列
            fuku_col = df.shape[1]-1

    jockey = df[name_col].astype(str)
    fuku_raw = df[fuku_col].astype(str)

    # 「xx.x%」「xx％」などを数値化
    def to_pct(s: str) -> float:
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*[%％]", s)
        if m:
            return float(m.group(1))
        # 0.32 のような小数(=32%)で置かれている場合
        m2 = re.fullmatch(r"0?\.(\d+)", s)
        if m2:
            return float("0."+m2.group(1)) * 100.0
        # 数字だけ (例: 28.6)
        m3 = re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", s)
        if m3:
            return float(m3.group(0))
        return float("nan")

    fuku = fuku_raw.map(to_pct)

    out = pd.DataFrame({
        "jockey": jockey,
        "fukusho_rate": fuku,  # %
    })

    # ゴミ行（合計・備考など）を落とす
    out = out[out["jockey"].str.len() > 0]
    out = out.dropna(subset=["fukusho_rate"])

    # ランク判定
    def to_rank(pct: float) -> str:
        if pct >= 30.0:
            return "A"
        if pct >= 20.0:
            return "B"
        if pct >= 10.0:
            return "C"
        return "D"

    out["rank"] = out["fukusho_rate"].map(to_rank)
    return out.reset_index(drop=True)

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    html = fetch_html(URL)
    soup = BeautifulSoup(html, "html.parser")

    tbl = find_table(soup)
    if tbl is None:
        # 失敗時はプレビューを出して落とす（ワークフローのログで構造を確認できる）
        preview = re.sub(r"\s+", " ", html[:800])
        raise RuntimeError(f"table not found. preview: {preview}")

    df = parse_table(tbl)
    # 何件か表示しておくとログで見やすい
    print(f"[info] scraped {len(df)} rows")
    print(df.head().to_string(index=False))

    # 安全のためソート（複勝率降順）
    df = df.sort_values("fukusho_rate", ascending=False)

    df.to_csv(OUT_PATH, index=False, encoding="utf-8")
    print(f"[ok] wrote: {OUT_PATH}")

if __name__ == "__main__":
    main()