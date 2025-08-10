# -*- coding: utf-8 -*-
"""
Rakuten競馬：本日のレースID抽出 → 単複オッズ取得 → 戦略①〜④の条件判定 → 通知（任意）
- 2系統でRACEIDを拾います（堅牢化）：
   Rakuten#1: 本日の発売情報 (#todaysTicket) の「投票受付中」行
   Rakuten#2: 本日の出馬表一覧（開催場ごとの「レース一覧」リンク）から翌日RACEIDも捕捉
- tanfukuページはテーブル依存を最小化し、単勝オッズを主に解析
  （「2.9-3.6」「2.9～3.6」のようなレンジは中点を使用）
- 人気(pop)はオッズから再計算（ページ内で拾えなかった場合の保険）
- 戦略①〜④の判定時に、各条件の True/False を whylog として詳細出力
- 通知は環境変数 NOTIFY_ENABLED=1 のときのみ送信関数を呼ぶ（デフォはログだけ）

ENV例:
  NOTIFIED_PATH=/tmp/notified_races.json
  KILL_SWITCH=False
  DRY_RUN=False
  NOTIFY_ENABLED=0      # 1で通知ON
  LINE_TOKEN=xxxxx      # 通知をLINEに飛ばすなら（ダミー可）
"""

import os
import re
import json
import time
import random
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

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
DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"
KILL_SWITCH = os.getenv("KILL_SWITCH", "False").lower() == "true"
NOTIFY_ENABLED = int(os.getenv("NOTIFY_ENABLED", "0") or "0")

LINE_TOKEN = os.getenv("LINE_TOKEN", "")

# -------------------------
# ユーティリティ
# -------------------------
def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

def today_ymd() -> str:
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

def parse_float(text: str) -> Optional[float]:
    """単勝オッズ文字列 → float。'2.9-3.6' や '2.9～3.6' は中点、'—'等はNone"""
    if not text:
        return None
    t = text.strip().replace(",", "")
    if t in {"-", "—", "―", "－", "取止", "取消"}:
        return None
    # レンジ
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s*[～\-~]\s*(\d+(?:\.\d+)?)\s*$", t)
    if m:
        a, b = float(m.group(1)), float(m.group(2))
        return (a + b) / 2.0
    # 単値
    m2 = re.search(r"(\d+(?:\.\d+)?)", t)
    if m2:
        return float(m2.group(1))
    return None

# -------------------------
# RACEID 抽出（2系統）
# -------------------------
RACEID_RE = re.compile(r"/RACEID/(\d{18})")

def list_raceids_rakuten_1() -> List[str]:
    """#todaysTicket『投票受付中』のhrefから抽出"""
    url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{today_ymd()}0000000000"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")
    table = soup.find(id="todaysTicket")
    links = table.select("td.nextRace a[href]") if table else soup.find_all("a", href=True)
    ret = []
    for a in links:
        text = (a.get_text(strip=True) or "")
        if ("投票受付中" in text) or ("発走" in text):
            m = RACEID_RE.search(a["href"])
            if m:
                ret.append(m.group(1))
    return sorted(set(ret))

def list_raceids_rakuten_2() -> List[str]:
    """出馬表一覧ページ内の『レース一覧』リンク群から、その日の各開催の頭RACEIDを拾う（翌日分も捕捉しやすい）"""
    url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{today_ymd()}0000000000"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")
    links = soup.select("td.raceState a[href], a[href*='/race_card/list/RACEID/']")
    ret = []
    for a in links:
        m = RACEID_RE.search(a.get("href", ""))
        if m:
            # 末尾が ..0400, ..0500 など「開催一覧」の先頭IDのことが多い
            ret.append(m.group(1))
    return sorted(set(ret))

# -------------------------
# 単複オッズ取得
# -------------------------
def fetch_tanfuku_odds(race_id: str) -> Dict:
    """
    戻り値:
      {
        "race_id": str,
        "title": str,
        "now": str,
        "horses": [
           {"umaban": int, "name": str, "odds": float, "pop": int},
           ...
        ],
        "url": str
      }
    """
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    h1 = soup.find("h1")
    race_title = h1.get_text(strip=True) if h1 else ""
    nowtime = soup.select_one(".withUpdate .nowTime")
    now_label = nowtime.get_text(strip=True) if nowtime else ""

    # できるだけ緩くテーブル検出（summaryにオッズ/単勝/複勝を含む）
    tables = []
    for t in soup.find_all("table"):
        summary = (t.get("summary") or "") + " " + " ".join(t.get("class", []))
        if any(key in summary for key in ["オッズ", "単勝", "複勝", "odds"]):
            tables.append(t)

    horses: List[Dict] = []

    def try_parse_table(tbl) -> List[Dict]:
        rows = tbl.find_all("tr")
        parsed = []
        for tr in rows:
            tds = tr.find_all(["td", "th"])
            if len(tds) < 3:
                continue
            text_cells = [c.get_text(" ", strip=True) for c in tds]
            # 馬番は数値セル（1〜16くらい）
            umaban = None
            for tx in text_cells[:3]:
                m = re.fullmatch(r"\d{1,2}", tx)
                if m:
                    umaban = int(m.group(0))
                    break
            if umaban is None:
                continue

            # 単勝オッズらしいセルを探す（日本語「単勝」見出しが無くても、数値/レンジを探す）
            o_val: Optional[float] = None
            for tx in text_cells:
                val = parse_float(tx)
                # 1.0〜200.0くらいをオッズ候補とみなす
                if val is not None and 1.0 <= val <= 200.0:
                    o_val = val
                    break
            # 馬名
            name = ""
            # たいてい馬名は a タグか、2〜3番目セル
            a_name = tr.find("a")
            if a_name and a_name.get_text(strip=True):
                name = a_name.get_text(strip=True)
            if not name:
                # それっぽいセルを拾う（漢字/カタカナ/英字混在）
                for tx in text_cells:
                    if re.search(r"[ぁ-んァ-ン一-龥A-Za-z]", tx):
                        # ただしオッズっぽいものは除外
                        if parse_float(tx) is None:
                            name = tx
                            break

            if o_val is None:
                continue

            parsed.append({"umaban": umaban, "name": name or f"#{umaban}", "odds": o_val})

        return parsed

    for t in tables:
        horses.extend(try_parse_table(t))
    # 重複（複数テーブル）を馬番でユニーク化（最も小さいオッズを採用）
    uniq: Dict[int, Dict] = {}
    for h in horses:
        u = h["umaban"]
        if u not in uniq or h["odds"] < uniq[u]["odds"]:
            uniq[u] = h
    horses = sorted(uniq.values(), key=lambda x: x["umaban"])

    # 人気(pop) 算出（ページから拾えない想定の保険）：単勝オッズ昇順で1位→…
    by_odds = sorted(horses, key=lambda x: (x["odds"], x["umaban"]))
    pop_map = {h["umaban"]: i + 1 for i, h in enumerate(by_odds)}
    for h in horses:
        h["pop"] = pop_map.get(h["umaban"], 999)

    return {"race_id": race_id, "title": race_title, "now": now_label, "horses": horses, "url": url}

# -------------------------
# 戦略①〜④ 判定
# -------------------------
def top_odds_by_pop(horses: List[Dict], n: int = 4) -> Tuple[float, float, float, float, List[int]]:
    top = sorted(horses, key=lambda x: (x["pop"], x["odds"]))[:n]
    # 人気1〜4位のオッズ&馬番
    o = [9999.0] * n
    nums = []
    for i, h in enumerate(top):
        o[i] = float(h.get("odds", 9999.0))
        nums.append(h["umaban"])
    # 足りない場合もあるのでパディング済
    return (o[0], o[1], o[2], o[3], nums)

def find_strategy_for_race(od: Dict) -> Tuple[Optional[Dict], str]:
    horses = od.get("horses", [])
    if len(horses) < 4:
        return None, "horses<4 で判定不可"

    o1, o2, o3, o4, top_nums = top_odds_by_pop(horses, 4)
    why = []

    # 戦略①：o1 in [2,10], o2<10, o3<10, o4>=15
    cond1 = (2.0 <= o1 <= 10.0) and (o2 < 10.0) and (o3 < 10.0) and (o4 >= 15.0)
    why.append(f"① o1∈[2,10]={2.0<=o1<=10.0} o2<10={o2<10} o3<10={o3<10} o4>=15={o4>=15}  (o1={o1},o2={o2},o3={o3},o4={o4})")
    if cond1:
        return ({"strategy": "①", "buy": f"三連単BOX 人気1-3位 {top_nums[:3]}"},
                " / ".join(why))

    # 戦略②：o1<2, o2<10, o3<10
    cond2 = (o1 < 2.0) and (o2 < 10.0) and (o3 < 10.0)
    why.append(f"② o1<2={o1<2.0} o2<10={o2<10.0} o3<10={o3<10.0}")
    if cond2:
        return ({"strategy": "②", "buy": f"相手広め/馬連・三連複中心 人気1-3位={top_nums[:3]}"},
                " / ".join(why))

    # 戦略③：o1<=1.5, o2>=10
    cond3 = (o1 <= 1.5) and (o2 >= 10.0)
    why.append(f"③ o1<=1.5={o1<=1.5} o2>=10={o2>=10.0}")
    if cond3:
        # 相手はオッズ6〜20のゾーンから拾う等、細則は必要に応じて
        return ({"strategy": "③", "buy": f"本命絶対視：1着固定流し（相手手広く） 人気1位={top_nums[:1]}"},
                " / ".join(why))

    # 戦略④：o1<=3, o2<=3, 6<=o3<=10, o4>=15
    cond4 = (o1 <= 3.0) and (o2 <= 3.0) and (6.0 <= o3 <= 10.0) and (o4 >= 15.0)
    why.append(f"④ o1<=3={o1<=3.0} o2<=3={o2<=3.0} 6<=o3<=10={6.0<=o3<=10.0} o4>=15={o4>=15.0}")
    if cond4:
        return ({"strategy": "④", "buy": f"人気2頭軸+妙味3番手：三連単/三連複軸流し {top_nums[:3]}"},
                " / ".join(why))

    return None, " / ".join(why)

# -------------------------
# 通知（任意：ここはお好みの実装に差し替え可）
# -------------------------
def send_notification(msg: str) -> None:
    if NOTIFY_ENABLED != 1:
        logging.info(f"[NOTIFY] disabled: {msg}")
        return
    # ここに実通知処理（例：LINE Notify）
    if not LINE_TOKEN:
        logging.info(f"[NOTIFY] 実トークン未設定のためログのみ: {msg}")
        return
    try:
        requests.post(
            "https://notify-api.line.me/api/notify",
            headers={"Authorization": f"Bearer {LINE_TOKEN}"},
            data={"message": msg},
            timeout=TIMEOUT,
        )
        logging.info("[NOTIFY] 送信完了")
    except Exception as e:
        logging.warning(f"[NOTIFY] 送信失敗: {e}")

# -------------------------
# メイン
# -------------------------
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    if KILL_SWITCH:
        logging.info("[INFO] KILL_SWITCH=True のため、処理スキップ")
        return

    logging.info(f"[INFO] ジョブ開始 host={os.uname().nodename} pid={os.getpid()}")
    logging.info(f"[INFO] NOTIFIED_PATH={NOTIFIED_PATH} KILL_SWITCH={KILL_SWITCH} DRY_RUN={DRY_RUN}")
    logging.info(f"[INFO] NOTIFY_ENABLED={NOTIFY_ENABLED}")

    # --- RACEID 抽出（2系統）---
    r1 = list_raceids_rakuten_1()
    logging.info(f"[INFO] Rakuten#1 本日の発売情報: {len(r1)}件")

    r2 = list_raceids_rakuten_2()
    logging.info(f"[INFO] Rakuten#2 出馬表一覧: {len(r2)}件")

    raceids = sorted(set(r1 + r2))
    logging.info(f"[INFO] 発見RACEID数: {len(raceids)}")
    for rid in raceids:
        logging.info(f"  - {rid} -> tanfuku")

    if not raceids:
        logging.info("[INFO] 対象レースIDがありません（開催なし or 取得失敗）")
        logging.info("[INFO] HITS=0")
        logging.info("[INFO] 戦略一致なし（通知なし）")
        save_notified({})
        logging.info("[INFO] ジョブ終了")
        return

    notified = load_notified()
    hits = 0
    matches = 0

    for rid in raceids:
        try:
            od = fetch_tanfuku_odds(rid)
            hits += 1
            logging.info(f"[OK] tanfuku疎通: {rid} {od['title']} {od['now']} {od['url']}")

            strat, whylog = find_strategy_for_race(od)
            if strat:
                matches += 1
                logging.info(f"[MATCH] {rid} 戦略={strat['strategy']} 条件詳細: {whylog}")
                # 実際の通知メッセージ
                msg = (f"[{now_jst_str()}]\n"
                       f"{od['title']} / RACEID={rid}\n"
                       f"戦略{strat['strategy']} HIT\n"
                       f"買い目: {strat.get('buy','')}\n"
                       f"{od['url']}")
                send_notification(msg)
                # 既読管理（通知済みの印）
                notified[rid] = time.time()
            else:
                logging.info(f"[NO MATCH] {rid} 条件詳細: {whylog}")

            time.sleep(random.uniform(*SLEEP_BETWEEN))

        except Exception as e:
            logging.warning(f"[WARN] {rid} 取得/解析失敗: {e}")

    logging.info(f"[INFO] HITS={hits} / MATCHES={matches}")
    save_notified(notified)
    logging.info(f"[INFO] notified saved: {NOTIFIED_PATH} (bytes={len(json.dumps(notified, ensure_ascii=False))})")
    logging.info("[INFO] ジョブ終了")

if __name__ == "__main__":
    main()