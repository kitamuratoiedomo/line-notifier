# jockey_rank.py
import re
import logging
from typing import Dict, Tuple
from datetime import timezone, timedelta

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))
HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ---- 任意：簡易ランク表（必要に応じて更新） ----------------------------
# 実運用ではここをスプレッドシート等に差し替えてもOK
_RANK_TABLE = {
    # 例
    "矢野貴之": "B",
    "本田正重": "C",
    "笹川翼": "C",
    # 未登録はデフォルト C とする
}

def _get(url: str, timeout: int = 12) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

def _norm_jname(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"\s+", "", name)
    # 括弧の所属など除去
    name = re.sub(r"（.*?）|\(.*?\)", "", name)
    return name

def get_jockey_rank(name: str) -> str:
    name = _norm_jname(name)
    if not name:
        return "-"
    return _RANK_TABLE.get(name, "C")

def get_jockey_map(race_id: str) -> Dict[int, Tuple[str, str]]:
    """
    楽天のレースカードから「馬番 -> (騎手名, ランク)」を返す。
    失敗時は空 dict。
    """
    urls = [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{race_id}",
        f"https://keiba.rakuten.co.jp/race/top/RACEID/{race_id}",
    ]
    for url in urls:
        try:
            html = _get(url, timeout=10)
            soup = BeautifulSoup(html, "html.parser")

            # 馬番・騎手名が同じ行に並ぶ table/section を広めに探索
            cand = soup.find_all(["table", "section", "div"])
            pairs = {}
            for blk in cand:
                rows = blk.find_all("tr")
                for tr in rows:
                    texts = [td.get_text(" ", strip=True) for td in tr.find_all(["th", "td"])]
                    if len(texts) < 3:
                        continue
                    # 馬番
                    umaban = None
                    for t in texts[:2]:
                        if re.fullmatch(r"\d{1,2}", t):
                            umaban = int(t)
                            break
                    if umaban is None:
                        continue
                    # 騎手名候補
                    jname = None
                    for t in texts:
                        # 「騎手」「斤量」等の文字列が近傍にあることもあるが、
                        # とにかく漢字/カナ2〜4字程度を優先的に拾う
                        if re.search(r"[一-龥ァ-ヶー]{2,}", t):
                            # 明らかに馬名や厩舎・調教師と紛れることがあるため
                            # “斤量”“厩舎”“父”“母”“馬体重”などを含むセルは避ける
                            if any(x in t for x in ["斤量", "厩舎", "父", "母", "馬体重", "タイム"]):
                                continue
                            jname = _norm_jname(t)
                            # 騎手名はだいたい 2〜4 文字程度
                            if 1 <= len(jname) <= 6:
                                break
                    if umaban is not None and jname:
                        pairs[umaban] = (jname, get_jockey_rank(jname))
            if pairs:
                return pairs
        except Exception as e:
            logging.warning("get_jockey_map failed url=%s err=%s", url, e)

    return {}