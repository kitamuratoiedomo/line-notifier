import os
import json
import time
import logging
import socket
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone

import requests
from jockey_rank import get_jrank
from odds_client import list_today_raceids, fetch_tanfuku_odds, get_race_start_iso

# （★）スプレッドシート対応の読み込み（存在しない場合はフォールバック）
USE_SHEET = os.getenv("USE_SHEET", "1") == "1"
SHEET_AVAILABLE = False
try:
    if USE_SHEET:
        from sheets_client import fetch_recipients
        SHEET_AVAILABLE = True
except Exception as e:
    logging.warning("sheets_client を読み込めませんでした（単一宛先にフォールバック）: %s", e)
    SHEET_AVAILABLE = False

# ====== 設定 ======
NOTIFIED_PATH = Path(os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json"))
NOTIFIED_PATH.parent.mkdir(parents=True, exist_ok=True)

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

# キルスイッチ / ドライラン
KILL_SWITCH = os.getenv("NOTIFY_ENABLED", "1") != "1"
DRY_RUN = os.getenv("DRY_RUN_MESSAGE", "0") == "1"

# 通知許可の時間窓
WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "5"))
WINDOW_AFTER_MIN  = int(os.getenv("WINDOW_AFTER_MIN", "0"))

# ログ
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
JST = timezone(timedelta(hours=9))


# ============ ユーティリティ ============
def now_jst() -> datetime:
    return datetime.now(JST)

def _load_notified() -> Dict[str, Any]:
    if NOTIFIED_PATH.exists():
        try:
            return json.loads(NOTIFIED_PATH.read_text())
        except Exception as e:
            logging.warning("通知済みファイル読み込み失敗: %s", e)
            return {}
    return {}

def _save_notified(obj: Dict[str, Any]) -> None:
    try:
        NOTIFIED_PATH.write_text(json.dumps(obj, ensure_ascii=False))
        logging.info("notified saved: %s (bytes=%d)", NOTIFIED_PATH, NOTIFIED_PATH.stat().st_size)
    except Exception as e:
        logging.warning("通知済み保存に失敗: %s", e)

def prune_notified(store: Dict[str, Any], keep_date: str) -> Dict[str, Any]:
    pruned = {k: v for k, v in store.items() if keep_date in k}
    if len(pruned) != len(store):
        logging.info("notified pruned: %d -> %d", len(store), len(pruned))
    return pruned

def within_window(start_iso: str) -> bool:
    try:
        t = datetime.fromisoformat(start_iso)
    except Exception:
        base = now_jst().strftime("%Y-%m-%d")
        t = datetime.fromisoformat(f"{base}T{start_iso}:00+09:00")
    now = now_jst()
    return (t - timedelta(minutes=WINDOW_BEFORE_MIN)) <= now <= (t + timedelta(minutes=WINDOW_AFTER_MIN))


# ============ LINE送信 ============
def _push_line(user_id: str, text: str) -> None:
    if not LINE_ACCESS_TOKEN or not user_id:
        logging.warning("LINE 環境変数 or user_id 未設定のため送信しません。")
        return
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {LINE_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": text}]}
    r = requests.post(url, headers=headers, json=payload, timeout=10)
    if r.status_code != 200:
        logging.warning("LINE送信失敗 user=%s status=%s body=%s", user_id, r.status_code, r.text)

def send_line_message(text: str) -> None:
    if KILL_SWITCH:
        logging.info("KILL_SWITCH有効のため送信しません。")
        return
    if DRY_RUN:
        logging.info("[DRY RUN] %s", text.replace("\n", " "))
        return

    if SHEET_AVAILABLE:
        recs = fetch_recipients()
        active = [r for r in recs if r.get("enabled") and r.get("userId")]
        if not active:
            logging.info("有効な受信者がシートに存在しません。フォールバックで単一宛へ送信します。")
            if LINE_USER_ID:
                _push_line(LINE_USER_ID, text)
            return
        for r in active:
            _push_line(r["userId"], text)
            time.sleep(0.15)  # rate-limit 予防
        logging.info("LINE送信OK（複数宛 %d件）", len(active))
    else:
        if LINE_USER_ID:
            _push_line(LINE_USER_ID, text)
            logging.info("LINE送信OK（単一宛）")
        else:
            logging.warning("宛先がありません（SHEETもLINE_USER_IDもなし）")


# ============ 戦略判定ダミー ============
def find_strategy_matches() -> List[Dict[str, Any]]:
    sample = {
        "race_id": "TEST-2025-08-09-01",
        "venue": "テスト競馬場",
        "race_no": "1R",
        "start_at_iso": (now_jst() + timedelta(minutes=10)).isoformat(),
        "strategy": "（テスト）",
        "message": "【テスト】複数宛先送信/シート経由の配信テストです。\n※このメッセージが全員に届けばOK。",
    }
    return [sample]


# ============ メイン ============
def main():
    logging.info("ジョブ開始 host=%s pid=%s", socket.gethostname(), os.getpid())
    logging.info("NOTIFIED_PATH=%s KILL_SWITCH=%s DRY_RUN=%s", NOTIFIED_PATH, KILL_SWITCH, DRY_RUN)

    notified = _load_notified()
    today = now_jst().strftime("%Y-%m-%d")
    notified = prune_notified(notified, keep_date=today)

    hits = find_strategy_matches()
    logging.info("HITS=%s", len(hits))

    if not hits:
        logging.info("戦略一致なし（通知なし）")
        _save_notified(notified)
        return

    for h in hits:
        race_id = h.get("race_id", "")
        strategy = h.get("strategy", "")
        start_iso = h.get("start_at_iso", "")

        if start_iso and not within_window(start_iso):
            logging.info("時間窓外のためスキップ: %s %s", race_id, start_iso)
            continue

        key_date = start_iso[:10] if start_iso else today
        key = f"{race_id}|{strategy}|{key_date}"
        if notified.get(key):
            logging.info("重複通知を回避: %s", key)
            continue

        msg = (h.get("message") or "").strip()
        if not msg:
            logging.info("message空のためスキップ: %s", key)
            continue

        send_line_message(msg)
        notified[key] = int(time.time())

    _save_notified(notified)
    logging.info("ジョブ終了")


if __name__ == "__main__":
    main()