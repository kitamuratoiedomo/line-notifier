# main.py — シンプル版（送信ログなし）

import os
import json
import time
import logging
import socket
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone

import requests

# ==== 外部モジュール ====
from odds_client import (
    list_today_raceids,
    fetch_tanfuku_odds,       # オッズ取得（単勝）
    get_sale_close_iso,       # 販売締切(ISO)
    get_venue_and_rno,        # 場名とR番号
)

# 受信者はスプレッドシート優先（失敗時は単一宛先にフォールバック）
SHEET_AVAILABLE = False
try:
    from sheets_client import fetch_recipients
    SHEET_AVAILABLE = True
except Exception as e:
    logging.warning("sheets_client 読み込み不可（単一宛先にフォールバック）: %s", e)
    SHEET_AVAILABLE = False

# ==== 設定 ====
JST = timezone(timedelta(hours=9))
NOTIFIED_PATH = Path(os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json"))
NOTIFIED_PATH.parent.mkdir(parents=True, exist_ok=True)

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

# フラグ
KILL_SWITCH = os.getenv("NOTIFY_ENABLED", "1") != "1"  # True なら送らない
DRY_RUN     = os.getenv("DRY_RUN_MESSAGE", "0") == "1"

# 通知の時間窓（販売締切の “5分前想定” 基準）
WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "5"))  # 既定=5
WINDOW_AFTER_MIN  = int(os.getenv("WINDOW_AFTER_MIN", "0"))  # 既定=0

# テスト: 強制で1件流すとき（0/1）
TEST_HOOK = os.getenv("TEST_HOOK", "0") == "1"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ==== 共通ユーティリティ ====
def now_jst() -> datetime:
    return datetime.now(JST)

def _load_notified() -> Dict[str, Any]:
    if NOTIFIED_PATH.exists():
        try:
            return json.loads(NOTIFIED_PATH.read_text())
        except Exception as e:
            logging.warning("通知済みDB読取失敗: %s", e)
    return {}

def _save_notified(obj: Dict[str, Any]) -> None:
    try:
        NOTIFIED_PATH.write_text(json.dumps(obj, ensure_ascii=False))
        logging.info("notified saved: %s (bytes=%d)", NOTIFIED_PATH, NOTIFIED_PATH.stat().st_size)
    except Exception as e:
        logging.warning("通知済みDB保存失敗: %s", e)

def prune_notified(store: Dict[str, Any], keep_date: str) -> Dict[str, Any]:
    pruned = {k: v for k, v in store.items() if keep_date in k}
    if len(pruned) != len(store):
        logging.info("notified pruned: %d -> %d", len(store), len(pruned))
    return pruned

def within_window(target_iso: str) -> bool:
    """
    target_iso を基準に [−WINDOW_BEFORE_MIN, +WINDOW_AFTER_MIN] の範囲に今が入っているか。
    """
    t = datetime.fromisoformat(target_iso)
    now = now_jst()
    return (t - timedelta(minutes=WINDOW_BEFORE_MIN)) <= now <= (t + timedelta(minutes=WINDOW_AFTER_MIN))


# ==== LINE 送信 ====
def _push_line(user_id: str, text: str) -> None:
    if not LINE_ACCESS_TOKEN or not user_id:
        logging.warning("LINE トークン or user_id 未設定で送信不可")
        return
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {LINE_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": text}]}
    r = requests.post(url, headers=headers, json=payload, timeout=10)
    if r.status_code != 200:
        logging.warning("LINE送信失敗 user=%s status=%s body=%s", user_id, r.status_code, r.text)

def send_line_message(text: str) -> None:
    if KILL_SWITCH:
        logging.info("KILL_SWITCH=True のため送信しません")
        return
    if DRY_RUN:
        logging.info("[DRY RUN] %s", text.replace("\n", " "))
        return

    # シートが使えれば複数宛。なければ単一宛
    if SHEET_AVAILABLE:
        try:
            recs = fetch_recipients()
            active = [r for r in recs if r.get("enabled") and r.get("userId")]
        except Exception as e:
            logging.warning("fetch_recipients 失敗（単一宛先にフォールバック）: %s", e)
            active = []

        if active:
            for r in active:
                _push_line(r["userId"], text)
                time.sleep(0.15)  # rate limit 予防
            logging.info("LINE送信OK（複数宛 %d件）", len(active))
            return

    if LINE_USER_ID:
        _push_line(LINE_USER_ID, text)
        logging.info("LINE送信OK（単一宛）")
    else:
        logging.warning("宛先なし（SHEETもLINE_USER_IDも無効）")


# ==== 戦略ロジック（簡略版：①〜④判定） ====
def pick_top(horses: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    return sorted(horses, key=lambda x: x["odds"])[:n]

def match_strategies(r: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    r: {
      race_id, venue, race_no, sale_close_iso, horses=[{umaban, odds, pop}, ...]
    }
    戻り値: [{strategy_id, title, picks_text}, ...]
    """
    out = []
    hs = sorted(r["horses"], key=lambda x: x["odds"])
    if len(hs) < 3:
        return out

    # 上位のオッズ
    o1 = hs[0]["odds"]
    o2 = hs[1]["odds"]
    o3 = hs[2]["odds"]
    o4 = hs[3]["odds"] if len(hs) >= 4 else 999.0

    # ① 1〜3番人気 三連単BOX
    if 2.0 <= o1 <= 10.0 and o2 < 10.0 and o3 < 10.0 and o4 >= 15.0:
        out.append({
            "strategy_id": "①",
            "title": "1〜3番人気 三連単BOX（6点）",
            "picks_text": "\n".join([f"#1 単勝:{o1}", f"#2 単勝:{o2}", f"#3 単勝:{o3}"])
        })

    # ② 1番人気1着固定 × 2・3番人気（三連単 2点）
    if o1 < 2.0 and o2 < 10.0 and o3 < 10.0:
        out.append({
            "strategy_id": "②",
            "title": "1番人気1着固定 × 2・3番人気（三連単 2点）",
            "picks_text": "\n".join([f"#1 単勝:{o1}", f"#2 単勝:{o2}", f"#3 単勝:{o3}"])
        })

    # ③ 1着固定 × 10〜20倍流し（1→相手4頭→相手4頭）
    #    1番人気オッズ：1.5以下、相手：2番人気以下で10〜20倍の馬 最大4頭（未満ならあるだけ）
    #    通知文では最大5頭表示だがここでは上限4頭
    ten_twenty = [h for h in hs[1:] if 10.0 <= h["odds"] <= 20.0][:4]
    if o1 <= 1.5 and ten_twenty:
        pick_lines = [f"#1 単勝:{o1}"] + [f"#{h['pop']} 単勝:{h['odds']}" for h in ten_twenty]
        out.append({
            "strategy_id": "③",
            "title": "1→相手4頭→相手4頭（三連単）",
            "picks_text": "\n".join(pick_lines)
        })

    # ④ 3着固定（3番人気固定）三連単 2点
    if o1 <= 3.0 and o2 <= 3.0 and 6.0 <= o3 <= 10.0 and o4 >= 15.0:
        out.append({
            "strategy_id": "④",
            "title": "3着固定（3番人気固定）三連単（2点）",
            "picks_text": "\n".join([f"#1 単勝:{o1}", f"#2 単勝:{o2}", f"#3 単勝:{o3}"])
        })

    return out


# ==== メイン ====
def main():
    logging.info("ジョブ開始 host=%s pid=%s", socket.gethostname(), os.getpid())
    logging.info("NOTIFIED_PATH=%s KILL_SWITCH=%s DRY_RUN=%s SHEET=%s TEST_HOOK=%s",
                 NOTIFIED_PATH, KILL_SWITCH, DRY_RUN, SHEET_AVAILABLE, os.getenv("TEST_HOOK","0"))

    notified = _load_notified()
    today = now_jst().strftime("%Y-%m-%d")
    notified = prune_notified(notified, keep_date=today)

    race_ids = list_today_raceids()

    # テスト用：TEST_HOOKでダミー1件
    if TEST_HOOK:
        race_ids = race_ids or ["TEST-2025-08-09-01"]

    hits = 0

    for rid in race_ids:
        # 1) オッズ取得
        info = fetch_tanfuku_odds(rid)
        if not info or not info.get("horses"):
            logging.warning("単勝テーブル抽出に失敗（空） race_id=%s", rid)
            continue

        # 2) 販売締切ISO
        try:
            sale_iso = get_sale_close_iso(rid)
        except Exception as e:
            logging.warning("販売締切取得失敗 rid=%s err=%s", rid, e)
            continue
        info["sale_close_iso"] = sale_iso

        # 3) 場名・R
        venue, rno = get_venue_and_rno(rid)
        info["venue"] = venue or info.get("venue") or "地方"
        info["race_no"] = rno or info.get("race_no") or "—R"

        # 4) 時間窓判定（販売締切を基準）
        if not within_window(sale_iso):
            logging.info("時間窓外のためスキップ: %s %s", rid, sale_iso)
            continue

        # 5) 戦略判定
        strategies = match_strategies({
            "race_id": rid,
            "venue": info["venue"],
            "race_no": info["race_no"],
            "sale_close_iso": sale_iso,
            "horses": info["horses"],
        })
        if not strategies:
            continue

        # 6) 重複防止キー（race_id|strategy_id|date）
        date_key = sale_iso[:10]
        for st in strategies:
            key = f"{rid}|{st['strategy_id']}|{date_key}"
            if notified.get(key):
                continue

            # 7) 通知本文
            hhmm = datetime.fromisoformat(sale_iso).astimezone(JST).strftime("%H:%M")
            url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"
            text = (
                f"【戦略一致】{st['strategy_id']} {st['title']}\n"
                f"{info['venue']} {info['race_no']}（発走予定 {hhmm}）\n"
                f"◎ 候補（最大5頭）\n{st['picks_text']}\n\n"
                "※この通知は『販売締切（発走5分前想定）5分前』基準です。\n"
                f"{url}"
            )

            send_line_message(text)
            notified[key] = int(time.time())
            hits += 1

    logging.info("HITS=%d", hits)
    _save_notified(notified)
    logging.info("ジョブ終了")


if __name__ == "__main__":
    main()