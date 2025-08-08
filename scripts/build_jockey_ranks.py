# scripts/build_jockey_ranks.py
# --- 概要 -----------------------------------------------------------
# netkeiba 地方（NAR）リーディングの複勝率から
# 騎手ランク CSV を生成します。
# A: 複勝率 30%以上 / B: 20%以上30%未満 / C: 20%未満
# 出力: data/jockey_ranks.csv
# ------------------------------------------------------------------

import os

OUT = "data/jockey_ranks.csv"
os.makedirs(os.path.dirname(OUT), exist_ok=True)
with open(OUT, "w", encoding="utf-8", newline="") as f:
    ...
import sys
import time
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup

URL = "https://db.netkeiba.com/jockey/jockey_leading_nar.html"
UA = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"

OUT_DIR = "data"
OUT_PATH = os.path.join(OUT_DIR, "jockey_ranks.csv")

def fetch_html(url: str, retry: int = 3, sleep_sec: float = 1.5) -> str:
    """HTML を取得（軽いリトライ付き）"""
    last_err = None
    for i in range(retry):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=20)
            r.raise_for_status()
            # 一部ページは charset 指定が曖昧なので、requests の推定に任せる
            r.encoding = r.apparent_encoding
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(sleep_sec)
    raise RuntimeError(f"fetch_html failed: {last_err}")

def parse_table(html: str) -> pd.DataFrame:
    """
    netkeiba の表をパースして DataFrame にする。
    カラムは状況により増減することがあるため、名前解決を柔軟に行う。
    """
    soup = BeautifulSoup(html, "lxml")  # lxml が無ければ bs4 の html.parser でも動きます
    table = soup.select_one("table.db_main_s")
    if table is None:
        raise RuntimeError("table not found (selector: table.db_main_s)")

    # ヘッダー
    headers = [th.get_text(strip=True) for th in table.select("thead tr th")]
    # tbody行
    rows = []
    for tr in table.select("tbody tr"):
        tds = [td.get_text(strip=True) for td in tr.select("td")]
        if tds:
            rows.append(tds)

    if not headers and rows:
        # ヘッダが取れないケースは列数から推測（フォールバック）
        # 代表的な並びに合わせる
        # 例: 順位, 騎手名, 所属, 騎乗数, 勝数, 連対数, 3着内数, 勝率, 連対率, 複勝率, 獲得賞金
        n = max(len(r) for r in rows)
        guessed = [
            "順位","騎手名","所属","騎乗数","勝数","連対数","3着内数",
            "勝率","連対率","複勝率","獲得賞金"
        ]
        headers = guessed[:n]

    df = pd.DataFrame(rows, columns=headers[:len(rows[0])])

    # 重要列の正規化（列名が微妙に変わっても拾えるようにする）
    def find_col(patterns):
        for p in patterns:
            for c in df.columns:
                if re.search(p, c):
                    return c
        return None

    col_name = find_col(["騎手名"])
    col_place = find_col(["所属"])
    col_fukusho = find_col(["複勝率"])
    col_n_rides = find_col(["騎乗数"])

    keep_cols = []
    if col_name: keep_cols.append(col_name)
    if col_place: keep_cols.append(col_place)
    if col_n_rides: keep_cols.append(col_n_rides)
    if col_fukusho: keep_cols.append(col_fukusho)

    if not col_name or not col_fukusho:
        raise RuntimeError(f"必要列が見つかりません（騎手名 or 複勝率）。columns={list(df.columns)}")

    df = df[keep_cols].copy()
    df.rename(columns={
        col_name: "騎手名",
        col_place: "所属" if col_place else "所属",
        col_n_rides: "騎乗数" if col_n_rides else "騎乗数",
        col_fukusho: "複勝率"
    }, inplace=True)

    # 複勝率 → float（%を削除）
    df["複勝率"] = (
        df["複勝率"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    # 騎乗数がある場合は数値化
    if "騎乗数" in df.columns:
        df["騎乗数"] = (
            df["騎乗数"]
            .astype(str)
            .str.replace(",", "", regex=False)
            .replace("", 0)
        )
        # 数値化に失敗した行は 0 に
        df["騎乗数"] = pd.to_numeric(df["騎乗数"], errors="coerce").fillna(0).astype(int)

    # ランク付け
    def to_rank(x: float) -> str:
        if x >= 30.0:
            return "A"
        if x >= 20.0:
            return "B"
        return "C"

    df["ランク"] = df["複勝率"].apply(to_rank)

    # 出力に使う順序
    out_cols = ["騎手名", "所属", "騎乗数", "複勝率", "ランク"]
    # 無い列は除外して出力
    out_cols = [c for c in out_cols if c in df.columns]
    df = df[out_cols].copy()

    # 騎手名重複をまとめたいならここで groupby 等しても可
    return df

def main():
    html = fetch_html(URL)
    df = parse_table(html)

    os.makedirs(OUT_DIR, exist_ok=True)
    df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
    print(f"[OK] wrote: {OUT_PATH}  rows={len(df)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)