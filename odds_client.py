# odds_client.py
# 楽天競馬の複勝/単勝オッズページから
# 馬番・馬名・騎手名・オッズ(複勝優先)・人気(pop) を取得するユーティリティ
#
# 使い方:
#   from odds_client import fetch_odds
#   entries = fetch_odds("202508072135050400")
#   # => [{'num': 1, 'horse': 'ホースA', 'jockey': '矢野貴之', 'odds': 2.1, 'pop': 3}, ...]
#
# 備考:
# - ページ構造変化に強めの実装（ヘッダを走査して列位置を推定）
# - 人気が明示されていない場合は、取得オッズで昇順ランキングして擬似popを付与
# - 取得できない/行が壊れている場合はスキップ（堅牢性重視）

from __future__ import annotations

import os
import re
import time
import logging
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup

UA = os.getenv(
    "SCRAPE_UA",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
)

ODDS_URL_TMPL = "https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{raceid}?bmode=1"


def _fetch_html(url: str, retry: int = 3, sleep_sec: float = 0.8) -> str:
    last_err = None
    for i in range(retry):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=10)
            r.raise_for_status()
            # 楽天は UTF-8
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(sleep_sec)
    raise RuntimeError(f"GET failed: {url} last_err={last_err}")


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _to_float(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.replace(",", "").replace("－", "").replace("–", "")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        v = float(m.group(1))
        if 0.9 <= v <= 999.9:
            return v
    except Exception:
        pass
    return None


def _to_int(s: str) -> Optional[int]:
    if not s:
        return None
    s = s.replace(",", "")
    m = re.search(r"(\d+)", s)
    if not m:
        return None
    try:
        v = int(m.group(1))
        if 0 < v < 1000:
            return v
    except Exception:
        pass
    return None


def _find_target_table(soup: BeautifulSoup):
    """
    ヘッダーに '馬名' or '騎手' を含み、かつ '単勝' or '複勝' を含むテーブルを探す
    """
    for tbl in soup.find_all("table"):
        ths = [ _norm(th.get_text()) for th in tbl.find_all("th") ]
        joined = " ".join(ths)
        if ("馬名" in joined or "馬名" in joined) and ("複勝" in joined or "単勝" in joined):
            return tbl
    # 次善策：thが無いテーブルも見てみる
    for tbl in soup.find_all("table"):
        sample = _norm(tbl.get_text())[:200]
        if ("馬" in sample and "騎手" in sample) and ("複勝" in sample or "単勝" in sample):
            return tbl
    return None


def _header_index_map(table) -> Dict[str, int]:
    """
    テーブルヘッダから列位置を推定し、使うキーを返す
    戻り: {'num': 1, 'horse': 3, 'jockey': 5, 'odds_fuku': 8, 'odds_tan': 7, 'pop': 9} のような dict
    見つからない列は存在しないキーとして扱う
    """
    ths = [ _norm(th.get_text()) for th in table.find_all("th") ]
    idx = {}
    for i, h in enumerate(ths):
        if re.search(r"馬\s*番|馬番", h):
            idx["num"] = i
        elif "馬名" in h:
            idx["horse"] = i
        elif "騎手" in h:
            idx["jockey"] = i
        elif "人気" in h:
            idx["pop"] = i
        elif "複勝" in h:
            idx["odds_fuku"] = i
        elif "単勝" in h:
            idx["odds_tan"] = i
    return idx


def fetch_odds(raceid: str) -> List[Dict]:
    """
    楽天競馬 複勝/単勝オッズページから、エントリ一覧を返す。
    返却: [{'num': int, 'horse': str, 'jockey': str, 'odds': float, 'pop': int}, ...]
    """
    url = ODDS_URL_TMPL.format(raceid=raceid)
    html = _fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    table = _find_target_table(soup)
    if table is None:
        # テーブルが見つからない場合、ページ全文から素朴に tr を拾ってゴリ押し抽出
        logging.warning("odds table not found. trying fallback parsing.")
        rows = []
        for tr in soup.find_all("tr"):
            tds = [ _norm(td.get_text()) for td in tr.find_all(["td","th"]) ]
            if len(tds) < 4:
                continue
            text = " ".join(tds)
            # 馬番（1-18）を含むっぽい行のみ
            if not re.search(r"\b([1-9]|1[0-8])\b", text):
                continue
            # 大雑把に抽出
            num = _to_int(tds[0]) or _to_int(re.search(r"\b([1-9]|1[0-8])\b", text).group(1))
            horse = ""
            jockey = ""
            odds = None
            pop = None

            # 馬名っぽい最長日本語トークン
            for tok in tds:
                if re.search(r"[一-鷗ぁ-ゔァ-ヴー々〆〤ヶ]", tok) and len(tok) >= 2:
                    horse = tok
                    break
            # 騎手名（漢字2〜3字+漢字1〜2字 が多い）
            for tok in tds:
                if re.fullmatch(r"[一-龥]{1,3}[一-龥]{1,2}", tok):
                    jockey = tok
                    break
            # オッズ候補
            for tok in tds[::-1]:
                v = _to_float(tok)
                if v is not None:
                    odds = v
                    break
            # 人気
            for tok in tds[::-1]:
                v = _to_int(tok)
                if v is not None and 1 <= v <= 20:
                    pop = v
                    break

            if num and horse and jockey and odds is not None:
                rows.append({"num": num, "horse": horse, "jockey": jockey,
                             "odds": odds, "pop": pop})
        # 人気が無いものはオッズ昇順で補完
        if rows and any(r.get("pop") is None for r in rows):
            rows_sorted = sorted(rows, key=lambda r: (r["odds"], r["num"]))
            for i, r in enumerate(rows_sorted, start=1):
                if r.get("pop") is None:
                    r["pop"] = i
            # 元の順序は馬番順にそろえる
            rows = sorted(rows_sorted, key=lambda r: r["num"])
        return rows

    # 正規ルート: テーブルヘッダから列を特定
    idx = _header_index_map(table)
    use_fuku = "odds_fuku" in idx
    use_tan  = "odds_tan"  in idx

    entries: List[Dict] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        cells = [ _norm(td.get_text()) for td in tds ]
        if len(cells) < 3:
            continue

        # 列位置が分からない場合に備えて保険
        num    = _to_int(cells[idx["num"]])    if "num" in idx and idx["num"]    < len(cells) else _to_int(cells[0])
        horse  = cells[idx["horse"]]           if "horse" in idx and idx["horse"] < len(cells) else ""
        jockey = cells[idx["jockey"]]          if "jockey" in idx and idx["jockey"] < len(cells) else ""
        pop    = _to_int(cells[idx["pop"]])    if "pop" in idx and idx["pop"]    < len(cells) else None

        odds_cands: List[Optional[float]] = []
        if use_fuku and idx["odds_fuku"] < len(cells):
            odds_cands.append(_to_float(cells[idx["odds_fuku"]]))
        if use_tan and idx["odds_tan"] < len(cells):
            odds_cands.append(_to_float(cells[idx["odds_tan"]]))
        # 後方セルからも救済（数字っぽい物を拾う）
        for c in cells[::-1]:
            v = _to_float(c)
            if v is not None:
                odds_cands.append(v)
                break

        odds = None
        for v in odds_cands:
            if v is not None:
                odds = v
                break

        if not (num and horse and jockey and odds is not None):
            continue

        entries.append({
            "num": num,
            "horse": horse,
            "jockey": jockey,
            "odds": odds,
            "pop": pop,
        })

    # 人気が無いものはオッズ昇順で補完
    if entries and any(e.get("pop") is None for e in entries):
        sorted_by_odds = sorted(entries, key=lambda x: (x["odds"], x["num"]))
        for i, e in enumerate(sorted_by_odds, start=1):
            if e.get("pop") is None:
                e["pop"] = i
        entries = sorted(sorted_by_odds, key=lambda x: x["num"])

    return entries


if __name__ == "__main__":
    # 手元検証用: 環境変数 RACEID があれば叩く
    rid = os.getenv("RACEID")
    if not rid:
        print("set RACEID env to test, e.g. RACEID=202508072135050400")
    else:
        rows = fetch_odds(rid)
        for r in rows:
            print(r)