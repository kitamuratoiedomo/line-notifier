# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視ワンファイル版（戦略①②③④内蔵・完全差し替え）

機能
- 楽天競馬「出馬表(一覧)」から RACEID を抽出（2系統で堅牢化）
- 各RACEIDの 単勝/複勝 オッズページ(odds/tanfuku)へ疎通 → オッズを軽量パース
- 戦略①②③④（ユーザー提供ロジック）で判定
- 判定の可否と理由をログ出力
- 環境変数:
    KILL_SWITCH       : True/False でジョブ停止
    DRY_RUN           : True なら通知（send_line）は実行しない
    NOTIFY_ENABLED    : 0/1 で通知ON/OFF（0で止める）
    NOTIFIED_PATH     : 既通知記録の保存先（JSON）
    DEBUG_RACEIDS     : カンマ区切りでRACEIDを直接指定してテスト
    USER_AGENT        : 任意のUser-Agentを上書き
"""

import os
import re
import json
import time
import random
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

# ========================
# 設定
# ========================
JST = timezone(timedelta(hours=9))

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": os.getenv("USER_AGENT") or (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
})

TIMEOUT = (10, 20)           # (connect, read)
RETRY = 3
SLEEP_BETWEEN = (0.8, 1.5)

NOTIFIED_PATH = os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json")
DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"
KILL_SWITCH = os.getenv("KILL_SWITCH", "False").lower() == "true"
NOTIFY_ENABLED = int(os.getenv("NOTIFY_ENABLED", "1"))
DEBUG_RACEIDS = [s.strip() for s in os.getenv("DEBUG_RACEIDS", "").split(",") if s.strip()]

# ========================
# 共通ユーティリティ
# ========================
def jst_now_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

def yyyymmdd_today() -> str:
    return datetime.now(JST).strftime("%Y%m%d")

def fetch(url: str) -> str:
    last_err = None
    for i in range(1, RETRY + 1):
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:
            last_err = e
            wait = random.uniform(*SLEEP_BETWEEN)
            logging.warning(f"[WARN] fetch失敗({i}/{RETRY}) {e} -> {wait:.1f}s待機: {url}")
            time.sleep(wait)
    raise last_err

def load_notified() -> Dict[str, float]:
    try:
        with open(NOTIFIED_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_notified(d: Dict[str, float]) -> None:
    os.makedirs(os.path.dirname(NOTIFIED_PATH), exist_ok=True)
    with open(NOTIFIED_PATH, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)

# ========================
# RACEID 収集
# ========================
RACEID_RE = re.compile(r"/RACEID/(\d{18})")

def list_today_raceids() -> List[str]:
    """楽天 出馬表(一覧)ページから RACEID を抽出（2系統）"""
    ymd = yyyymmdd_today()
    url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    # 系統1: #todaysTicket（本日の発売情報）
    cnt1 = 0
    ids: List[str] = []
    todays = soup.find(id="todaysTicket")
    if todays:
        links = todays.select("td.nextRace a[href], td a[href]")
        for a in links:
            m = RACEID_RE.search(a.get("href", ""))
            if m:
                ids.append(m.group(1))
                cnt1 += 1

    # 系統2: ページ中の全リンクから /RACEID/ を総なめ（保険）
    cnt2 = 0
    for a in soup.find_all("a", href=True):
        m = RACEID_RE.search(a["href"])
        if m:
            ids.append(m.group(1))
            cnt2 += 1

    # 正規化
    ids = sorted(set(ids))
    logging.info(f"[INFO] Rakuten#1 本日の発売情報: {cnt1}件")
    logging.info(f"[INFO] Rakuten#2 出馬表一覧: {len(ids)}件")

    return ids

# ========================
# オッズページ解析
# ========================
def extract_tanfuku_rows(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """
    単勝/複勝テーブルから最低限の情報を抽出
    返却: [{"num": 馬番(int), "win": 単勝オッズ(float|None)}]
    """
    tables = []
    # summary に「オッズ」「単勝」が入るテーブルを優先
    for t in soup.find_all("table"):
        summary = (t.get("summary") or "").strip()
        if "オッズ" in summary or "単勝" in summary:
            tables.append(t)
    if not tables:
        # 最後の保険：table全体から“単勝”見出しを含むもの
        for t in soup.find_all("table"):
            if "単勝" in t.get_text(" ", strip=True):
                tables.append(t)

    rows_out: List[Dict[str, Any]] = []
    for t in tables:
        for tr in t.find_all("tr"):
            txt = tr.get_text(" ", strip=True)
            # ざっくりフィルタ：馬番らしきもの・数値オッズが含まれる行
            tds = tr.find_all(["td", "th"])
            if len(tds) < 2:
                continue

            # 馬番候補
            num = None
            # 左側に馬番が入る前提で、整数に解釈できるものを拾う
            for cell in tds[:3]:
                s = cell.get_text(strip=True)
                if s.isdigit():
                    num = int(s)
                    break
            if num is None:
                continue

            # 単勝オッズ候補（小数含む）
            win = None
            for cell in tds:
                s = cell.get_text(strip=True).replace(",", "")
                # "1.5" "12.1" など、小数点を含む数値をザックリ拾う
                if re.fullmatch(r"\d+(\.\d+)?", s):
                    try:
                        v = float(s)
                        # 単勝として常識的な範囲だけ許容
                        if 1.0 <= v <= 999.9:
                            win = v
                            break
                    except Exception:
                        pass

            rows_out.append({"num": num, "win": win})

    # 馬番でユニーク化（重複対策）
    uniq = {}
    for r in rows_out:
        if r["win"] is None:
            continue
        uniq[r["num"]] = r
    return list(uniq.values())

def parse_tanfuku_meta(soup: BeautifulSoup) -> Dict[str, Any]:
    """見出し・時刻・オッズ行・人気順など"""
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else "N/A"
    nowtime = soup.select_one(".withUpdate .nowTime")
    now_label = nowtime.get_text(strip=True) if nowtime else ""

    rows = extract_tanfuku_rows(soup)
    horses_count = len(rows)

    # 人気順（単勝オッズの昇順）
    sorted_rows = sorted(rows, key=lambda r: (r["win"] if r["win"] is not None else 9999))

    # 戦略ロジック用データ
    horses_data = []
    for i, r in enumerate(sorted_rows, start=1):
        horses_data.append({
            "pop": i,
            "umaban": r["num"],
            "odds": r["win"]
        })

    return {
        "title": title,
        "now": now_label,
        "horses": horses_count,
        "rows": rows,
        "fav": sorted_rows[:5],
        "odds_table_found": bool(rows),
        "horses_data": horses_data
    }

# ========================
# 戦略①②③④（ユーザー提供ロジックを内蔵）
# ========================
def _get_by_pop(horses: List[Dict[str, Any]], pop: int) -> Optional[Dict[str, Any]]:
    for h in horses:
        if h.get("pop") == pop:
            return h
    return None

def eval_strategy(horses: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    戦略①〜④のいずれかに一致したら dict を返す。
    返却: { "strategy": "...", "tickets": [...], "roi": "...", "hit": "..." }
    """
    p1 = _get_by_pop(horses, 1)
    p2 = _get_by_pop(horses, 2)
    p3 = _get_by_pop(horses, 3)
    p4 = _get_by_pop(horses, 4)

    if not (p1 and p2 and p3 and p4):
        return None

    o1, o2, o3, o4 = p1["odds"], p2["odds"], p3["odds"], p4["odds"]

    # ① 1〜3番人気BOX（6点）
    # 1番 2.0〜10.0 / 2〜3番 <10.0 / 4番 ≥15.0
    if (2.0 <= o1 <= 10.0) and (o2 < 10.0) and (o3 < 10.0) and (o4 >= 15.0):
        tickets = [f"{p1['pop']}-{p2['pop']}-{p3['pop']}",
                   f"{p1['pop']}-{p3['pop']}-{p2['pop']}",
                   f"{p2['pop']}-{p1['pop']}-{p3['pop']}",
                   f"{p2['pop']}-{p3['pop']}-{p1['pop']}",
                   f"{p3['pop']}-{p1['pop']}-{p2['pop']}",
                   f"{p3['pop']}-{p2['pop']}-{p1['pop']}"]
        return {
            "strategy": "① 1〜3番人気BOX（6点）",
            "tickets": tickets,
            "roi": "想定回収率: 138.5% / 的中率: 22.4%",
            "hit": "対象354Rベース",
        }

    # ② 1番人気1着固定 × 2・3番人気（2点）
    # 1番 <2.0 / 2〜3 <10.0
    if (o1 < 2.0) and (o2 < 10.0) and (o3 < 10.0):
        tickets = [f"{p1['pop']}-{p2['pop']}-{p3['pop']}", f"{p1['pop']}-{p3['pop']}-{p2['pop']}"]
        return {
            "strategy": "② 1番人気1着固定 × 2・3番人気（2点）",
            "tickets": tickets,
            "roi": "想定回収率: 131.4% / 的中率: 43.7%",
            "hit": "対象217Rベース",
        }

    # ③ 1着固定 × 10〜20倍流し（相手は2番人気以降の中から 10〜20倍に該当する最大5頭）
    # 1番 ≤1.5
    if o1 <= 1.5:
        cand = [h for h in horses if h["pop"] >= 2 and 10.0 <= h["odds"] <= 20.0]
        cand = cand[:5]
        if cand:
            tickets = [f"{_get_by_pop(horses,1)['pop']}-{c['pop']}-総流し" for c in cand]
            return {
                "strategy": "③ 1着固定 × 10〜20倍流し（候補最大5頭）",
                "tickets": tickets,
                "roi": "想定回収率: 139.2% / 的中率: 16.8%",
                "hit": "対象89Rベース",
            }

    # ④ 3着固定（3番人気固定）2点
    # 1・2 ≤3.0 / 3が 6〜10 / 4 ≥15
    if (o1 <= 3.0) and (o2 <= 3.0) and (6.0 <= o3 <= 10.0) and (o4 >= 15.0):
        tickets = [f"{p1['pop']}-{p2['pop']}-{p3['pop']}", f"{p2['pop']}-{p1['pop']}-{p3['pop']}"]
        return {
            "strategy": "④ 3着固定（3番人気固定）2点",
            "tickets": tickets,
            "roi": "想定回収率: 133.7% / 的中率: 21.5%",
            "hit": "対象128Rベース",
        }

    return None

def judge_strategies(meta: Dict[str, Any]) -> Tuple[bool, str]:
    horses_data = meta.get("horses_data")
    if not horses_data or len(horses_data) < 4:
        return False, "horses<4 で判定不可"

    result = eval_strategy(horses_data)
    if result:
        detail = (
            f"{result['strategy']}\n"
            f"買い目: {', '.join(result['tickets'])}\n"
            f"{result['roi']} / {result['hit']}"
        )
        return True, detail
    return False, "戦略①〜④: 条件未充足"

# ========================
# 疎通 & 通知
# ========================
def check_tanfuku_page(race_id: str) -> Optional[Dict[str, Any]]:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    meta = parse_tanfuku_meta(soup)
    if not meta.get("odds_table_found"):
        logging.warning(f"[WARN] オッズテーブル未検出: {url}")
        return None

    meta.update({"url": url})
    return meta

def send_line(message: str) -> None:
    """通知実装は既存のLINE連携に接続してください。ここではスイッチのみ。"""
    if NOTIFY_ENABLED != 1:
        return
    if DRY_RUN:
        logging.info("[DRY_RUN] 通知はスキップ")
        return
    # ここに実装を接続
    # ex) line_notify(message)
    logging.info(f"[NOTIFY] {message}")

# ========================
# メイン
# ========================
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [INFO] %(message)s")
    logging.info(f"[INFO] ジョブ開始 host={os.uname().nodename} pid={os.getpid()}")
    logging.info(f"[INFO] NOTIFIED_PATH={NOTIFIED_PATH} KILL_SWITCH={KILL_SWITCH} DRY_RUN={DRY_RUN}")
    logging.info(f"[INFO] NOTIFY_ENABLED={NOTIFY_ENABLED}")

    if KILL_SWITCH:
        logging.info("[INFO] KILL_SWITCH=True のため、処理をスキップします。")
        return

    # RACEID列挙
    if DEBUG_RACEIDS:
        raceids = DEBUG_RACEIDS
        logging.info(f"[INFO] DEBUG_RACEIDS 指定: {len(raceids)}件")
    else:
        raceids = list_today_raceids()
        if not raceids:
            logging.info("[INFO] 発見RACEIDなし")
            return
        logging.info(f"[INFO] 発見RACEID数: {len(raceids)}")
        for rid in raceids:
            logging.info(f"  - {rid} -> tanfuku")

    notified = load_notified()
    hits = 0
    matches = 0

    for rid in raceids:
        try:
            meta = check_tanfuku_page(rid)
            if not meta:
                continue

            title = meta.get("title", "N/A")
            now_label = meta.get("now", "")
            url = meta["url"]
            # ログ（疎通OK）
            stamp = ""
            m = re.search(r"(\d{2}:\d{2})", now_label)
            if m:
                stamp = f"{m.group(1)}時点"
            logging.info(f"[OK] tanfuku疎通: {rid} {title} {stamp} {url}")
            hits += 1

            # 判定
            ok, detail = judge_strategies(meta)
            if ok:
                matches += 1
                logging.info(f"[MATCH] {rid} 条件詳細: {detail.replace(os.linesep, ' / ')}")

                # 通知メッセージ
                msg = (
                    f"【戦略ヒット】\n"
                    f"RACEID: {rid}\n"
                    f"{title} {stamp}\n"
                    f"{detail}\n"
                    f"{url}"
                )
                send_line(msg)
                notified[rid] = time.time()
            else:
                logging.info(f"[NO MATCH] {rid} 条件詳細: {detail}")

            time.sleep(random.uniform(*SLEEP_BETWEEN))

        except Exception as e:
            logging.warning(f"[WARN] {rid} 処理中に例外: {e}")

    logging.info(f"[INFO] HITS={hits} / MATCHES={matches}")
    if matches == 0:
        # 1件も通知していないなら空保存（重複通知防止の意味は薄いので初期化）
        save_notified({})
    else:
        save_notified(notified)
    logging.info(f"[INFO] notified saved: {NOTIFIED_PATH} (bytes={len(json.dumps(notified, ensure_ascii=False))})")
    logging.info("[INFO] ジョブ終了")

if __name__ == "__main__":
    main()