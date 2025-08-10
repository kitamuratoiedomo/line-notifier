# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視・通知バッチ（完全版）
- 門限（JST 10:00〜22:00）で夜間はスキップ
- RACEID列挙は2系統で堅牢化
  A) 本日の発売情報 (#todaysTicket)
  B) 出馬表一覧（当日/翌日も拾えるリンク群フォールバック）
- DEBUG_RACEIDS で任意テスト
- 戦略①〜④ (strategy_rules.py の eval_strategy) をそのまま適用
- オッズテーブル解析は壊れ耐性高め（複数セレクタ/非数値スキップ）
- 重複通知制御（NOTIFY_TTL_SECで抑制）
- DRY_RUN/NOTIFY_ENABLED/KILL_SWITCH/門限時刻(START_HOUR/END_HOUR)を環境変数で制御
"""

import os
import re
import json
import time
import random
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from strategy_rules import eval_strategy  # ←戦略①〜④をここから呼び出し

# ========= 基本設定 =========
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

TIMEOUT = (10, 25)        # (connect, read)
RETRY = 3
SLEEP_BETWEEN = (0.6, 1.2)

# ========= 環境変数 =========
NOTIFIED_PATH    = os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json")
DRY_RUN          = os.getenv("DRY_RUN", "False").lower() == "true"
KILL_SWITCH      = os.getenv("KILL_SWITCH", "False").lower() == "true"
NOTIFY_ENABLED   = os.getenv("NOTIFY_ENABLED", "1") == "1"
DEBUG_RACEIDS    = [s.strip() for s in os.getenv("DEBUG_RACEIDS", "").split(",") if s.strip()]
NOTIFY_TTL_SEC   = int(os.getenv("NOTIFY_TTL_SEC", "1800"))  # 同一RACEIDの連投抑止
START_HOUR       = int(os.getenv("START_HOUR", "10"))        # 門限開始（含む）
END_HOUR         = int(os.getenv("END_HOUR",   "22"))        # 門限終了（含まない）

# ========= 正規表現 =========
RACEID_RE = re.compile(r"/RACEID/(\d{18})")

# ========= ユーティリティ =========
def now_jst() -> datetime:
    return datetime.now(JST)

def within_operating_hours() -> bool:
    """門限チェック（JST）"""
    h = now_jst().hour
    return START_HOUR <= h < END_HOUR

def fetch(url: str) -> str:
    """GET with retry"""
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

def should_skip_by_ttl(notified: Dict[str, float], rid: str) -> bool:
    """同一RACEIDの通知をTTLで抑制"""
    ts = notified.get(rid)
    if not ts:
        return False
    return (time.time() - ts) < NOTIFY_TTL_SEC

# ========= RACEID 列挙（A: 本日の発売情報） =========
def list_raceids_today_ticket(ymd: str) -> List[str]:
    url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")
    table = soup.find(id="todaysTicket")
    if not table:
        logging.info("[INFO] #todaysTicket なし")
        return []
    links = table.select("td.nextRace a[href], td a[href]")
    raceids: List[str] = []
    for a in links:
        text = (a.get_text(strip=True) or "")
        m = RACEID_RE.search(a.get("href", ""))
        if not m:
            continue
        rid = m.group(1)
        if "投票受付中" in text or "発走" in text:
            raceids.append(rid)
    raceids = sorted(set(raceids))
    logging.info(f"[INFO] Rakuten#1 本日の発売情報: {len(raceids)}件")
    return raceids

# ========= RACEID 列挙（B: 出馬表一覧フォールバック） =========
def list_raceids_from_card_lists(ymd: str, ymd_next: str) -> List[str]:
    """
    出馬表一覧のリンク群からRACEIDを広めに拾う。
    当日と翌日の “レース一覧/出馬表/払戻/分析” 等リンクにもRACEIDが出現するケースがあるので、
    文言に強依存せず href から抽出 → 18桁RACEIDだけ採用。
    """
    urls = [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000",
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd_next}0000000000",
    ]
    rids: List[str] = []
    for u in urls:
        try:
            html = fetch(u)
            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                m = RACEID_RE.search(a["href"])
                if m:
                    rids.append(m.group(1))
        except Exception as e:
            logging.warning(f"[WARN] 出馬表一覧スキャン失敗: {e} ({u})")
    rids = sorted(set(rids))
    logging.info(f"[INFO] Rakuten#2 出馬表一覧: {len(rids)}件")
    return rids

# ========= オッズ解析 =========
def parse_odds_table(soup: BeautifulSoup) -> Tuple[List[Dict[str, float]], Optional[str], Optional[str]]:
    """
    単勝/複勝オッズテーブルから
      - horses: [{"pop":1,"odds":2.4}, ...]
      - venue_race: "盛岡競馬場 11R" など（可能なら）
      - now_label: "00:28時点" など（可能なら）
    を返す。表構造が変わっても耐えるように緩めに実装。
    """
    # テーブル候補（summaryにオッズを含む/thead内に“単勝/複勝/人気”等を含む）
    odds_table = soup.find("table", {"summary": re.compile("オッズ")})
    if not odds_table:
        # 予備：classやデータ属性で拾う
        for t in soup.find_all("table"):
            summary = (t.get("summary") or "") + " " + " ".join(t.get("class", []))
            if "オッズ" in summary:
                odds_table = t
                break

    # レース名や更新時刻（あれば）
    venue_race = None
    h1 = soup.find("h1")
    if h1:
        venue_race = h1.get_text(strip=True)
    nowtime = soup.select_one(".withUpdate .nowTime") or soup.select_one(".nowTime")
    now_label = nowtime.get_text(strip=True) if nowtime else None

    horses: List[Dict[str, float]] = []
    if not odds_table:
        return horses, venue_race, now_label

    # 行を走査して、人気/オッズらしき数字を抽出
    trs = odds_table.find_all("tr")
    # 先頭行がヘッダっぽければスキップ
    start = 1 if trs and ("人気" in trs[0].get_text() or "馬番" in trs[0].get_text()) else 0

    for tr in trs[start:]:
        tds = tr.find_all(["td", "th"])
        if len(tds) < 2:
            continue

        # 人気（整数）っぽいもの
        pop = None
        for cand in tds:
            s = cand.get_text(strip=True).replace(",", "")
            if s.isdigit():
                try:
                    pop = int(s)
                    break
                except:
                    pass

        # オッズ（小数）っぽいもの（右寄せや%などは除去）
        odds = None
        for cand in tds[::-1]:  # 右のセルから優先的に
            s = cand.get_text(strip=True).replace(",", "")
            # "—", "-" は無視
            if s in {"—", "-", ""}:
                continue
            try:
                # 例: "2.4" / "2.4倍" → 数字部分だけ
                m = re.search(r"\d+(\.\d+)?", s)
                if not m:
                    continue
                odds = float(m.group(0))
                break
            except:
                continue

        if (pop is not None) and (odds is not None):
            horses.append({"pop": pop, "odds": odds})

    # 人気順でユニーク化（同一人気が重複して拾われた場合の保険）
    uniq = {}
    for h in sorted(horses, key=lambda x: x["pop"]):
        uniq.setdefault(h["pop"], h)
    horses = [uniq[k] for k in sorted(uniq.keys())]
    return horses, venue_race, now_label

def check_tanfuku_page(race_id: str) -> Optional[Dict]:
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    horses, venue_race, now_label = parse_odds_table(soup)
    if not horses:
        logging.warning(f"[WARN] オッズテーブル未検出: {url}")
        return None

    # venue_raceが空なら保険で何か出す
    if not venue_race:
        venue_race = "地方競馬"

    return {
        "race_id": race_id,
        "url": url,
        "horses": horses,
        "venue_race": venue_race,
        "now": now_label or "",
    }

# ========= 通知ダミー（実運用の送信処理に差し替えてください） =========
def send_notification(msg: str) -> None:
    """
    実運用ではここを LINE / Slack / Discord / Google Chat 等の既存連携へ差し替え。
    今はログ出力のみ。
    """
    logging.info(f"[NOTIFY] {msg}")

# ========= メイン =========
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if KILL_SWITCH:
        logging.info("[INFO] KILL_SWITCH=True のため終了")
        return

    # 門限
    if not within_operating_hours():
        logging.info(f"[INFO] 監視休止時間のためスキップ（JST={now_jst():%H:%M} / 稼働={START_HOUR:02d}:00-{END_HOUR:02d}:00）")
        return

    logging.info("[INFO] ジョブ開始")
    logging.info(f"[INFO] NOTIFIED_PATH={NOTIFIED_PATH} KILL_SWITCH={KILL_SWITCH} DRY_RUN={DRY_RUN}")
    if NOTIFY_ENABLED:
        logging.info("[INFO] NOTIFY_ENABLED=1")
    else:
        logging.info("[INFO] NOTIFY_ENABLED=0（通知抑止）")

    notified = load_notified()
    hits = 0
    matches = 0

    # 対象RACEID
    if DEBUG_RACEIDS:
        logging.info(f"[INFO] DEBUG_RACEIDS 指定: {len(DEBUG_RACEIDS)}件")
        target_raceids = DEBUG_RACEIDS
    else:
        ymd = now_jst().strftime("%Y%m%d")
        ymd_next = (now_jst() + timedelta(days=1)).strftime("%Y%m%d")
        r1 = list_raceids_today_ticket(ymd)
        r2 = list_raceids_from_card_lists(ymd, ymd_next)
        # どちらでも拾えた値を採用（A優先、B補完）
        target_raceids = sorted(set(r1) | set(r2))
        logging.info(f"[INFO] 発見RACEID数: {len(target_raceids)}")
        for rid in target_raceids:
            logging.info(f"  - {rid} -> tanfuku")

    for rid in target_raceids:
        # TTLでの重複抑制
        if should_skip_by_ttl(notified, rid):
            logging.info(f"[SKIP] TTL抑制: {rid}")
            continue

        meta = check_tanfuku_page(rid)
        if not meta:
            continue
        horses = meta["horses"]
        if len(horses) < 4:
            logging.info(f"[NO MATCH] {rid} 条件詳細: horses<4 で判定不可")
            continue

        hits += 1

        strategy = eval_strategy(horses)
        if strategy:
            matches += 1
            ticket_str = ", ".join(strategy["tickets"])
            detail = (
                f"{strategy['strategy']} / 買い目: {ticket_str} / "
                f"{strategy['roi']} / {strategy['hit']}"
            )
            logging.info(f"[MATCH] {rid} 条件詳細: {detail}")

            # 通知
            if NOTIFY_ENABLED and not DRY_RUN:
                msg = (
                    f"【戦略ヒット】\n"
                    f"RACEID: {rid}\n"
                    f"{meta['venue_race']} {meta['now']}\n"
                    f"{strategy['strategy']}\n"
                    f"買い目: {ticket_str}\n"
                    f"{strategy['roi']} / {strategy['hit']}\n"
                    f"{meta['url']}"
                )
                send_notification(msg)
            else:
                logging.info("[DRY_RUN] 通知はスキップ")

            # 通知記録
            notified[rid] = time.time()
        else:
            logging.info(f"[NO MATCH] {rid} 条件詳細: パターン①〜④に非該当")

        # サイト負荷配慮
        time.sleep(random.uniform(*SLEEP_BETWEEN))

    logging.info(f"[INFO] HITS={hits} / MATCHES={matches}")
    save_notified(notified)
    logging.info(f"[INFO] notified saved: {NOTIFIED_PATH} (bytes={len(json.dumps(notified, ensure_ascii=False))})")
    logging.info("[INFO] ジョブ終了")


if __name__ == "__main__":
    main()