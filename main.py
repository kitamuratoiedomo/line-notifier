import os
import json
import time
import logging
import socket
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone

import requests

# ====== 設定 ======
# RenderでDiskを追加し Mount Path を /var/data にしてください
NOTIFIED_PATH = Path(os.getenv("NOTIFIED_PATH", "/var/data/notified_races.json"))
NOTIFIED_PATH.parent.mkdir(parents=True, exist_ok=True)

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

# キルスイッチ（NOTIFY_ENABLED=0 で即時停止）
KILL_SWITCH = os.getenv("NOTIFY_ENABLED", "1") != "1"

# 通知許可の時間窓（発走○分前～○分後）
WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "15"))  # 15分前
WINDOW_AFTER_MIN  = int(os.getenv("WINDOW_AFTER_MIN", "5"))    # 5分後

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
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
    """キーに当日(YYYY-MM-DD)が含まれないものを削除して肥大化を防止"""
    pruned = {k: v for k, v in store.items() if keep_date in k}
    if len(pruned) != len(store):
        logging.info("notified pruned: %d -> %d", len(store), len(pruned))
    return pruned

def within_window(start_iso: str) -> bool:
    """
    start_iso: "2025-08-08T15:10:00+09:00" のようなISO8601文字列を想定
    発走15分前〜5分後の時間窓のみ True
    """
    try:
        t = datetime.fromisoformat(start_iso)
    except Exception:
        # 古い実装の互換: "HH:MM" だけ来る場合は当日扱いで補正
        base = now_jst().strftime("%Y-%m-%d")
        t = datetime.fromisoformat(f"{base}T{start_iso}:00+09:00")
    now = now_jst()
    return (t - timedelta(minutes=WINDOW_BEFORE_MIN)) <= now <= (t + timedelta(minutes=WINDOW_AFTER_MIN))


# ============ 送信部分 ============

def send_line_message(text: str) -> None:
    if KILL_SWITCH:
        logging.info("KILL_SWITCH有効のため送信しません。")
        return
    if not LINE_ACCESS_TOKEN or not LINE_USER_ID:
        logging.warning("LINE 環境変数が未設定のため送信しません。")
        return

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {LINE_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "to": LINE_USER_ID,
        "messages": [{"type": "text", "text": text}],
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        if r.status_code != 200:
            logging.warning("LINE送信失敗 status=%s body=%s", r.status_code, r.text)
        else:
            logging.info("LINE送信OK")
    except Exception as e:
        logging.exception("LINE送信で例外: %s", e)


# ============ 戦略判定の土台 ============
def find_strategy_matches() -> List[Dict[str, Any]]:
    """
    ここで『対象レース抽出＋人気/オッズ解析→戦略判定』を行い、
    ヒットしたレースを返す。今はダミーで空配列を返す。
    返却フォーマット例:
      {
        "race_id": "20250808-KAWASAKI-03R",
        "venue": "川崎",
        "race_no": "3R",
        "start_at_iso": "2025-08-08T16:00:00+09:00",
        "strategy": "鉄板1-2-3型",
        "message": "（LINEに投げる本文）"
      }
    """
    # TODO: 楽天等からオッズ取得→各戦略に合致するか判定
    return []


# ============ メイン ============

def main():
    logging.info("ジョブ開始 host=%s pid=%s", socket.gethostname(), os.getpid())
    logging.info("NOTIFIED_PATH=%s KILL_SWITCH=%s", NOTIFIED_PATH, KILL_SWITCH)

    notified = _load_notified()
    # 当日以外のキーを掃除
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
        start_iso = h.get("start_at_iso", "")  # ISO推奨

        # 時間窓チェック（発走前後のみ送る）
        if start_iso and not within_window(start_iso):
            logging.info("時間窓外のためスキップ: %s %s", race_id, start_iso)
            continue

        # 強めのデデュープキー（日付込み）
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
