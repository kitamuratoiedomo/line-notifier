# main.py
import os
import json
import time
import logging
import socket
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone

import requests
from jockey_rank import get_jrank  # ランク取得: 'A' | 'B' | 'C' | None

# ====== 設定 ======
# Render の Disks → Mount Path は /var/data を想定
NOTIFIED_PATH = Path(os.getenv("NOTIFIED_PATH", "/var/data/notified_races.json"))
NOTIFIED_PATH.parent.mkdir(parents=True, exist_ok=True)

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

# キルスイッチ（NOTIFY_ENABLED=0 で送信停止）
KILL_SWITCH = os.getenv("NOTIFY_ENABLED", "1") != "1"

# 通知許可の時間窓（発走○分前～○分後）
WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "15"))  # 15分前
WINDOW_AFTER_MIN  = int(os.getenv("WINDOW_AFTER_MIN", "5"))    # 5分後

# テスト用：ダミー通知を強制送信（本番では 0/未設定）
DRY_RUN_MESSAGE = os.getenv("DRY_RUN_MESSAGE", "0") == "1"

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
        # 互換: "HH:MM" だけ来る場合は当日扱いで補正
        base = now_jst().strftime("%Y-%m-%d")
        t = datetime.fromisoformat(f"{base}T{start_iso}:00+09:00")
    now = now_jst()
    return (t - timedelta(minutes=WINDOW_BEFORE_MIN)) <= now <= (t + timedelta(minutes=WINDOW_AFTER_MIN))

# ============ LINE 送信 ============

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

# ============ メッセージ生成 ============

def _fmt_time_hm(iso: str) -> str:
    try:
        return datetime.fromisoformat(iso).strftime("%H:%M")
    except Exception:
        return iso  # フォールバック

def build_message(hit: Dict[str, Any]) -> str:
    """
    hit 例:
      {
        "race_id": "20250808-KAWASAKI-03R",
        "venue": "川崎",
        "race_no": "3R",
        "start_at_iso": "2025-08-08T16:00:00+09:00",
        "strategy": "戦略③: 3連単軸1頭マルチ（候補<=5）",
        "candidates": [
            {"num":"1","horse":"ホースA","jockey":"矢野貴之"},
            {"num":"2","horse":"ホースB","jockey":"本田正重"},
            ...
        ],
        "note": "任意の補足"
      }
    """
    venue = hit.get("venue", "")
    race_no = hit.get("race_no", "")
    start_iso = hit.get("start_at_iso", "")
    hhmm = _fmt_time_hm(start_iso)
    strategy = hit.get("strategy", "戦略")
    note = hit.get("note", "")
    cands: List[Dict[str, str]] = hit.get("candidates", [])

    # 候補は最大5頭（仕様）
    cands = cands[:5]

    # 騎手ランク付与（ばんえいは対象外：jockey_rank.get_jrank 側で除外実装済みを想定）
    lines: List[str] = []
    for c in cands:
        num = c.get("num", "?")
        horse = c.get("horse", "")
        jockey = c.get("jockey", "")
        jrank = get_jrank(jockey) or "-"
        lines.append(f"{num}. {horse}（{jockey} / Rk:{jrank}）")

    cand_block = "\n".join(lines) if lines else "（候補なし）"

    msg = (
        f"【戦略アラート】{strategy}\n"
        f"{venue}{race_no}  発走 {hhmm}\n"
        f"\n"
        f"◎ 候補（最大5頭）\n"
        f"{cand_block}\n"
    )
    if note:
        msg += f"\n注記: {note}\n"

    # 複数戦略や補助情報を足すなら、ここで追記する
    return msg.strip()

# ============ 戦略判定（ダミー/将来実装） ============

def find_strategy_matches() -> List[Dict[str, Any]]:
    """
    ここで『対象レース抽出＋人気/オッズ解析→戦略判定』を行い、ヒットを返す。
    いまはダミー。環境変数 DRY_RUN_MESSAGE=1 のときだけテスト用1件を返します。
    """
    if not DRY_RUN_MESSAGE:
        return []

    # いまの時刻から10分後を発走にして、時間窓テストを通す
    start_at = (now_jst() + timedelta(minutes=10)).isoformat()

    dummy = {
        "race_id": "20250808-KAWASAKI-03R",
        "venue": "川崎",
        "race_no": "3R",
        "start_at_iso": start_at,
        "strategy": "戦略③: 3連単軸1頭マルチ（候補<=5）",
        "candidates": [
            {"num":"1","horse":"ホースA","jockey":"矢野貴之"},
            {"num":"2","horse":"ホースB","jockey":"本田正重"},
            {"num":"3","horse":"ホースC","jockey":"笹川翼"},
        ],
        "note": "DRY_RUN_MESSAGE=1 による動作確認",
    }
    return [dummy]

# ============ メイン ============

def main():
    logging.info("ジョブ開始 host=%s pid=%s", socket.gethostname(), os.getpid())
    logging.info("NOTIFIED_PATH=%s KILL_SWITCH=%s DRY_RUN=%s",
                 NOTIFIED_PATH, KILL_SWITCH, DRY_RUN_MESSAGE)

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

        # --- メッセージ生成 ---
        try:
            msg = build_message(h)
        except Exception as e:
            logging.exception("メッセージ生成失敗: %s", e)
            continue

        if not msg.strip():
            logging.info("message空のためスキップ: %s", key)
            continue

        send_line_message(msg)
        notified[key] = int(time.time())

    _save_notified(notified)
    logging.info("ジョブ終了")

if __name__ == "__main__":
    main()