import os
import json
import time
import logging
from pathlib import Path
from typing import List, Dict, Any

import requests

# ====== 設定 ======
LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

# 通知済みレースの記録（重複通知防止）
NOTIFIED_PATH = Path("/tmp/notified_races.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def send_line_message(text: str) -> None:
    """LINE Push API にメッセージ送信"""
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

def _load_notified() -> Dict[str, Any]:
    if NOTIFIED_PATH.exists():
        try:
            return json.loads(NOTIFIED_PATH.read_text())
        except Exception:
            return {}
    return {}

def _save_notified(obj: Dict[str, Any]) -> None:
    try:
        NOTIFIED_PATH.write_text(json.dumps(obj, ensure_ascii=False))
    except Exception as e:
        logging.warning("通知済み保存に失敗: %s", e)

# ============= ここから本番ロジックの土台 =============
# - 「戦略に合致したレースを見つけたら」だけ通知する
# - まだ楽天オッズのスクレイピング実装は入れていません（このあと実装）
# - まずは heartbeat を完全に止め、無駄な通知を出さない状態へ

def find_strategy_matches() -> List[Dict[str, Any]]:
    """
    ここで『対象レース抽出＋人気/オッズ解析→戦略判定』を行い、
    ヒットしたレースを返す。今はダミーで空配列を返す。
    次のステップで楽天オッズのパーサを実装していきます。
    返却フォーマット例:
      {
        "race_id": "202508072726110300-10R",
        "venue": "園田",
        "race_no": "10R",
        "start_at": "15:10",
        "strategy": "②",
        "message": "（LINEに投げる本文）"
      }
    """
    # TODO: 楽天『単・複オッズ』ページから人気/オッズを取得し戦略①〜④を判定
    return []

def main():
    logging.info("ジョブ開始（本番モード）")

    notified = _load_notified()

    hits = find_strategy_matches()
    if not hits:
        logging.info("戦略一致なし（通知なし）")
        return

    for h in hits:
        key = f"{h.get('race_id','')}_{h.get('strategy','')}"
        if notified.get(key):
            logging.info("重複通知を回避: %s", key)
            continue

        msg = h.get("message", "").strip()
        if not msg:
            logging.info("message空のためスキップ: %s", key)
            continue

        send_line_message(msg)
        notified[key] = int(time.time())

    _save_notified(notified)
    logging.info("ジョブ終了")

if __name__ == "__main__":
    main()