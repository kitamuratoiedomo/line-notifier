# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視ワーカー（完全差し替え版）
- JST 10:00〜22:00 のみ稼働（FORCE_RUN=1 で強制実行）
- RACEID収集:
    1) 本日の発売情報 (#todaysTicket) の「投票受付中」リンク
    2) 出馬表一覧 (/race_card/list/RACEID/{yyyymmdd}0000000000) のレース一覧リンク
  を併用し、壊れにくく収集
- DEBUG_RACEIDS があればそれだけを対象に検証
- 単複オッズ（tanfuku）ページで人気順オッズを抽出し、戦略①〜④で判定
- 通知は NOTIFY_ENABLED=1 かつ DRY_RUN=False のときのみ送信（ここではログ通知）
- 多重通知防止: NOTIFIED_PATH に記録
環境変数:
  DRY_RUN=[0|1]             : 通知を実行せずログのみ
  KILL_SWITCH=[0|1]         : 1で全スキップ
  NOTIFIED_PATH=/tmp/notified_races.json
  NOTIFY_ENABLED=[0|1]      : 1のときだけ通知（送信部分を有効扱い）
  DEBUG_RACEIDS="id1,id2"   : ここに入れたRACEIDだけチェック（カンマ区切り）
  FORCE_RUN=[0|1]           : 門限を無視して実行する
"""

import os
import re
import json
import time
import random
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

from strategy_rules import eval_strategy  # 戦略①〜④の本判定

# -------------------------
# 設定
# -------------------------
JST = timezone(timedelta(hours=9))

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
})

TIMEOUT = (10, 20)  # (connect, read)
RETRY = 3
SLEEP_BETWEEN = (0.7, 1.4)

NOTIFIED_PATH = os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json")
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"
KILL_SWITCH = os.getenv("KILL_SWITCH", "0") == "1"
NOTIFY_ENABLED = os.getenv("NOTIFY_ENABLED", "1") == "1"
FORCE_RUN = os.getenv("FORCE_RUN", "0") == "1"

DEBUG_RACEIDS = [s.strip() for s in os.getenv("DEBUG_RACEIDS", "").split(",") if s.strip()]

# -------------------------
# ユーティリティ
# -------------------------
def within_active_window_jst(now: datetime) -> bool:
    """監視するのは 10:00 <= 時刻 < 22:00 (JST)"""
    return 10 <= now.hour < 22

def get_today_str() -> str:
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

# -------------------------
# RACEID 収集
# -------------------------
RACEID_RE = re.compile(r"/RACEID/(\d{18})")

def _list_from_todays_ticket() -> List[str]:
    ymd = get_today_str()
    list_url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    html = fetch(list_url)
    soup = BeautifulSoup(html, "lxml")

    ids: List[str] = []
    tbl = soup.find(id="todaysTicket")
    if tbl:
        links = tbl.select("td.nextRace a[href], td a[href]")
    else:
        links = soup.find_all("a", href=True)

    for a in links:
        text = (a.get_text(strip=True) or "")
        m = RACEID_RE.search(a.get("href", ""))
        if not m:
            continue
        rid = m.group(1)
        # 「投票受付中」や「発走」等、直近開催のシグナルに限定
        if ("投票受付中" in text) or ("発走" in text):
            ids.append(rid)

    ids = sorted(set(ids))
    logging.info(f"[INFO] Rakuten#1 本日の発売情報: {len(ids)}件")
    return ids

def _list_from_race_card() -> List[str]:
    ymd = get_today_str()
    list_url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    html = fetch(list_url)
    soup = BeautifulSoup(html, "lxml")

    ids: List[str] = []
    for a in soup.find_all("a", href=True):
        m = RACEID_RE.search(a["href"])
        if m:
            ids.append(m.group(1))
    ids = sorted(set(ids))
    logging.info(f"[INFO] Rakuten#2 出馬表一覧: {len(ids)}件")
    return ids

def list_today_raceids() -> List[str]:
    # DEBUG_RACEIDSがあればそれを採用
    if DEBUG_RACEIDS:
        logging.info(f"[INFO] DEBUG_RACEIDS 指定: {len(DEBUG_RACEIDS)}件")
        return DEBUG_RACEIDS

    ids1 = _list_from_todays_ticket()
    ids2 = _list_from_race_card()

    # 例：当日以外の総合ID（末尾が0000000000など）を除外（念のため）
    def plausible(rid: str) -> bool:
        # RACEIDの下10桁がすべて0のような“総合ページ”は除外
        return not rid.endswith("0000000000")

    merged = sorted({rid for rid in (ids1 + ids2) if plausible(rid)})
    logging.info(f"[INFO] 発見RACEID数: {len(merged)}")
    for rid in merged:
        logging.info(f"  - {rid} -> tanfuku")
    return merged

# -------------------------
# tanfuku 解析
# -------------------------
ODDS_TABLE_SUMMARY_RE = re.compile(r"オッズ")

def parse_tanfuku(h: str) -> Dict[str, Any]:
    """tanfukuページから最低限のメタと人気順単勝オッズを抽出"""
    soup = BeautifulSoup(h, "lxml")

    # タイトル要素（例: <h1>盛岡競馬場 11R オッズ</h1>）
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else "地方競馬 オッズ"

    # 更新時刻（例: .withUpdate .nowTime -> "00:29時点"）
    nowtime = soup.select_one(".withUpdate .nowTime")
    now_label = nowtime.get_text(strip=True) if nowtime else ""

    # オッズテーブル
    odds_table = soup.find("table", {"summary": ODDS_TABLE_SUMMARY_RE})
    if not odds_table:
        return {"title": title, "now": now_label, "horses": [], "ok": False}

    horses: List[Dict[str, Any]] = []
    # テーブル行から 馬番/人気/単勝オッズ をできる範囲で抽出
    # 楽天の構造は変わることがあるため、強依存を避けて “数字らしいもの” を抽出していく
    for tr in odds_table.find_all("tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if len(tds) < 3:
            continue

        # 例: [馬番, 馬名..., 人気, 単勝, 複勝下限, 複勝上限] のような並びを想定しつつ、数値を拾う
        # 人気と単勝を特定するルール（数字抽出の堅牢版）
        nums = [s for s in tds if re.fullmatch(r"\d+(\.\d+)?", s)]
        if not nums:
            # "1.5〜2.3" のような複勝表記は無視
            continue

        # 人気は整数のはず、単勝は浮動小数のことが多い
        pop = None
        odds = None
        for s in nums:
            if pop is None and re.fullmatch(r"\d+", s):
                # 1〜18あたりのレンジにあるものを人気候補とみなす
                iv = int(s)
                if 1 <= iv <= 18:
                    pop = iv
                    continue
            if odds is None and re.fullmatch(r"\d+(\.\d+)?", s):
                # 0.0以上の小数（例: 1.3, 12.4, 56 など）を単勝オッズ候補
                fv = float(s)
                if fv > 0:
                    odds = fv

        if pop is not None and odds is not None:
            horses.append({"pop": pop, "odds": odds})

    # 人気の重複があれば最小オッズで代表に絞る
    by_pop: Dict[int, float] = {}
    for h0 in horses:
        p, o = h0["pop"], h0["odds"]
        by_pop[p] = min(by_pop.get(p, o), o)
    horses = [{"pop": p, "odds": o} for p, o in sorted(by_pop.items(), key=lambda x: x[0])]

    return {"title": title, "now": now_label, "horses": horses, "ok": True}

def check_tanfuku_page(race_id: str) -> Optional[Dict[str, Any]]:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    html = fetch(url)
    meta = parse_tanfuku(html)
    if not meta.get("ok"):
        logging.warning(f"[WARN] オッズテーブル未検出: {url}")
        return None

    title = meta["title"]
    now_label = meta["now"]
    logging.info(f"[OK] tanfuku疎通: {race_id} {title} {now_label} {url}")

    horses = meta["horses"]
    if len(horses) < 4:
        # 戦略判定に最低4人気まで必要
        logging.info(f"[NO MATCH] {race_id} 条件詳細: horses<4 で判定不可")
        return {"race_id": race_id, "title": title, "now": now_label, "url": url, "horses": horses, "match": None}

    match = eval_strategy(horses)  # ここが戦略①〜④の本判定
    return {
        "race_id": race_id,
        "title": title,
        "now": now_label,
        "url": url,
        "horses": horses,
        "match": match,
    }

# -------------------------
# 通知（ここではログ出力のみ）
# -------------------------
def notify(match: Dict[str, Any], meta: Dict[str, Any]) -> None:
    """
    実際のLINE送信に繋ぐ場合はここを差し替え。
    いまはログで内容を確認できるようにしている。
    """
    tickets = match.get("tickets", [])
    msg = (
        "[NOTIFY] 【戦略ヒット】\n\n"
        f"RACEID: {meta['race_id']}\n\n"
        f"{meta['title']} {meta['now']}\n\n"
        f"{match['strategy']}\n\n"
        f"買い目: {', '.join(tickets)}\n\n"
        f"{match.get('roi','')}"
        f" / {match.get('hit','')}\n\n"
        f"{meta['url']}"
    )
    logging.info(msg)

# -------------------------
# メイン
# -------------------------
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    logging.info("[INFO] ジョブ開始 host=%s pid=%s",
                 os.uname().nodename if hasattr(os, "uname") else "local",
                 os.getpid())
    logging.info(f"[INFO] NOTIFIED_PATH={NOTIFIED_PATH} KILL_SWITCH={KILL_SWITCH} DRY_RUN={DRY_RUN}")
    if NOTIFY_ENABLED:
        logging.info("[INFO] NOTIFY_ENABLED=1")

    # 門限チェック（JST 10:00〜22:00 以外はスキップ）
    now_jst = datetime.now(JST)
    if not FORCE_RUN and not within_active_window_jst(now_jst):
        logging.info(f"[INFO] 監視休止時間のためスキップ（JST={now_jst:%H:%M} / 稼働=10:00-22:00）")
        return

    if KILL_SWITCH:
        logging.info("[INFO] KILL_SWITCH=True のため、処理をスキップします。")
        return

    raceids = list_today_raceids()
    if not raceids:
        logging.info("[INFO] 対象レースIDがありません（開催なし or 取得失敗）")
        save_notified({})
        logging.info("[INFO] ジョブ終了")
        return

    notified = load_notified()
    hits = 0
    matches = 0

    for rid in raceids:
        try:
            meta = check_tanfuku_page(rid)
        except Exception as e:
            logging.warning(f"[WARN] tanfuku取得失敗 {rid}: {e}")
            continue
        if not meta:
            continue

        hits += 1

        match = meta.get("match")
        if not match:
            continue

        # すでに通知済みならスキップ
        if notified.get(rid):
            logging.info(f"[INFO] 既通知のためスキップ: {rid}")
            continue

        # 通知
        logging.info(
            f"[MATCH] {rid} 条件詳細: {match['strategy']} / "
            f"買い目: {', '.join(match.get('tickets', []))} / "
            f"{match.get('roi','')} / {match.get('hit','')}"
        )

        if NOTIFY_ENABLED and not DRY_RUN:
            notify(match, meta)
        else:
            logging.info("[DRY_RUN] 通知はスキップ")

        notified[rid] = time.time()
        matches += 1

        time.sleep(random.uniform(*SLEEP_BETWEEN))

    logging.info(f"[INFO] HITS={hits} / MATCHES={matches}")
    save_notified(notified if matches else {})
    logging.info(f"[INFO] notified saved: {NOTIFIED_PATH} (bytes={len(json.dumps(notified if matches else {}, ensure_ascii=False))})")
    logging.info("[INFO] ジョブ終了")

if __name__ == "__main__":
    main()