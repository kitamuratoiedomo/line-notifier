# -*- coding: utf-8 -*-
"""
scripts/build_jockey_ranks.py
netkeiba 地方(NAR)ジョッキー リーディングから複勝率を取得して
data/jockey_ranks.csv を生成します。

ランク:
  A: 複勝率 >= 30
  B: 20 <= 複勝率 < 30
  C: 10 <= 複勝率 < 20
  D: 複勝率 < 10
"""

import os
import re
import sys
import time
import csv
import requests
from bs4 import BeautifulSoup

URL = "https://db.netkeiba.com/jockey/jockey_leading_nar.html"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

OUT_DIR = "data"
OUT_PATH = os.path.join(OUT_DIR, "jockey_ranks.csv")


def fetch_html(url: str, retry: int = 3, sleep_sec: float = 1.2) -> str:
    last = None
    for _ in range(retry):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=15)
            r.raise_for_status()
            # netkeibaはUTF-8
            r.encoding = "utf-8"
            return r.text
        except Exception as e:
            last = e
            time.sleep(sleep_sec)
    raise RuntimeError(f"failed to GET {url}: {last}")


def normalize_percent(txt: str) -> float | None:
    if not txt:
        return None
    # 半角全角の%と空白を除去
    s = re.sub(r"[％%]", "", txt)
    s = re.sub(r"\s", "", s)
    # 例: '31.4' / '31' / '-' を想定
    try:
        return float(s)
    except Exception:
        return None


def detect_table(soup: BeautifulSoup):
    """
    1) <table>を総当たり
    2) thead / tr のヘッダ文字列を拾う
    3) '騎手' か '騎手名' を含み、かつ '複勝率' を含むテーブルを採用
    """
    candidates = []
    for tbl in soup.find_all("table"):
        # 全ヘッダ文字列
        headers = []
        thead = tbl.find("thead")
        if thead:
            for th in thead.find_all("th"):
                headers.append(th.get_text(strip=True))
        if not headers:
            # theadがない場合もあるので最初の行を見出し扱い
            first_tr = tbl.find("tr")
            if first_tr:
                for th in first_tr.find_all(["th", "td"]):
                    headers.append(th.get_text(strip=True))

        joined = " ".join(headers)
        if re.search(r"騎手|騎手名", joined) and ("複勝率" in joined):
            candidates.append((tbl, headers))

    if not candidates:
        return None, None
    # 一番列数が多いものを優先
    candidates.sort(key=lambda x: len(x[1]), reverse=True)
    return candidates[0]


def rank_by_place(place_pct: float | None) -> str:
    if place_pct is None:
        return ""
    if place_pct >= 30.0:
        return "A"
    if place_pct >= 20.0:
        return "B"
    if place_pct >= 10.0:
        return "C"
    return "D"


def parse_rows(tbl, headers):
    # 欄名インデックス
    def idx(keyword, alt=None):
        for i, h in enumerate(headers):
            if keyword in h:
                return i
        if alt:
            for i, h in enumerate(headers):
                if alt in h:
                    return i
        return -1

    jname_i = idx("騎手", "騎手名")
    place_i = idx("複勝率")

    # 予備: あると便利な列（なくてもOK）
    starts_i = idx("騎乗", "出走")
    win_i = idx("勝率")

    if jname_i < 0 or place_i < 0:
        raise RuntimeError("table headers not recognized: " + " / ".join(headers))

    data = []
    # tbody のみ対象（なければ全 tr）
    body = tbl.find("tbody") or tbl
    for tr in body.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        # 列数が足りない行はスキップ
        if max(jname_i, place_i, starts_i, win_i) >= len(tds):
            # 最低限必要な2列があるかだけ確認
            if max(jname_i, place_i) >= len(tds):
                continue

        jname = tds[jname_i].get_text(strip=True)
        place_txt = tds[place_i].get_text(strip=True)
        starts_txt = tds[starts_i].get_text(strip=True) if 0 <= starts_i < len(tds) else ""
        win_txt = tds[win_i].get_text(strip=True) if 0 <= win_i < len(tds) else ""

        place = normalize_percent(place_txt)
        win_pct = normalize_percent(win_txt)

        data.append(
            {
                "jockey_name": jname,
                "place_pct": place,
                "win_pct": win_pct,
                "starts": starts_txt,
                "rank": rank_by_place(place),
            }
        )
    return data


def main():
    html = fetch_html(URL)
    soup = BeautifulSoup(html, "html.parser")

    tbl, headers = detect_table(soup)
    if not tbl:
        # デバッグ用に先頭2KBを出力して失敗させる
        preview = re.sub(r"\s+", " ", html[:2000])
        raise RuntimeError(f"table not found. preview: {preview}")

    rows = parse_rows(tbl, headers)
    if not rows:
        raise RuntimeError("parsed 0 rows.")

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["jockey_name", "place_pct", "win_pct", "starts", "rank"])
        for r in rows:
            w.writerow(
                [r["jockey_name"], r["place_pct"], r["win_pct"], r["starts"], r["rank"]]
            )

    print(f"✅ wrote {OUT_PATH} ({len(rows)} rows)")


if __name__ == "__main__":
    main()