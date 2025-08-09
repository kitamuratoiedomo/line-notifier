# main.py
import os
import json
import time
import logging
import socket
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone

import requests

# 騎手ランク（無ければ警告だけ出して続行）
try:
    from jockey_rank import get_jrank
except Exception as e:
    def get_jrank(_: str) -> str:  # フォールバック（全部C）
        logging.warning("jockey_rank を読み込めませんでした: %s", e)
        return "C"

# 受信者スプレッドシート（任意）
USE_SHEET = os.getenv("USE_SHEET", "1") == "1"
SHEET_AVAILABLE = False
if USE_SHEET:
    try:
        from sheets_client import fetch_recipients  # -> List[Dict] {userId, name, enabled}
        SHEET_AVAILABLE = True
    except Exception as e:
        logging.warning("sheets_client を読み込めませんでした（単一宛先にフォールバック）: %s", e)
        SHEET_AVAILABLE = False

# ====== 設定 ======
# 書き込み不可パスが来ても /tmp に逃がす
def _resolve_notified_path() -> Path:
    p = Path(os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json"))
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    except Exception:
        alt = Path("/tmp/notified_races.json")
        alt.parent.mkdir(parents=True, exist_ok=True)
        logging.warning("NOTIFIED_PATH への作成に失敗。/tmp にフォールバック: %s", alt)
        return alt

NOTIFIED_PATH = _resolve_notified_path()

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

# フラグ
KILL_SWITCH = os.getenv("NOTIFY_ENABLED", "1") != "1"           # 1以外で停止
DRY_RUN = os.getenv("DRY_RUN_MESSAGE", "0") == "1"              # 1で本文だけログ＆送信スキップ
TEST_HOOK = os.getenv("TEST_HOOK", "0") == "1"                  # 1でダミー通知を常に1件生成

# 通知許可の時間窓（デフォ 5分前～5分後）
WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "5"))
WINDOW_AFTER_MIN  = int(os.getenv("WINDOW_AFTER_MIN", "5"))

# ログ
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
JST = timezone(timedelta(hours=9))


# ============ ユーティリティ ============
def now_jst() -> datetime:
    return datetime.now(JST)

def _load_notified() -> Dict[str, Any]:
    if NOTIFIED_PATH.exists():
        try:
            return json.loads(NOTIFIED_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            logging.warning("通知済みファイル読み込み失敗: %s", e)
    return {}

def _save_notified(obj: Dict[str, Any]) -> None:
    try:
        NOTIFIED_PATH.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
        logging.info("notified saved: %s (bytes=%d)", NOTIFIED_PATH, NOTIFIED_PATH.stat().st_size)
    except Exception as e:
        logging.warning("通知済み保存に失敗: %s", e)

def prune_notified(store: Dict[str, Any], keep_date: str) -> Dict[str, Any]:
    pruned = {k: v for k, v in store.items() if keep_date in k}
    if len(pruned) != len(store):
        logging.info("notified pruned: %d -> %d", len(store), len(pruned))
    return pruned

def within_window(start_iso: str) -> bool:
    """
    start_iso: ISO8601文字列 "YYYY-MM-DDTHH:MM:SS+09:00" を推奨。
    "HH:MM" だけ来ても当日扱いで補完。
    """
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
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        if r.status_code != 200:
            logging.warning("LINE送信失敗 user=%s status=%s body=%s", user_id, r.status_code, r.text)
    except Exception as e:
        logging.exception("LINE送信で例外: %s", e)

def send_line_message(text: str) -> None:
    if KILL_SWITCH:
        logging.info("KILL_SWITCH有効のため送信しません。")
        return
    if DRY_RUN:
        logging.info("[DRY RUN] %s", text.replace("\n", " "))
        return

    if SHEET_AVAILABLE:
        try:
            recs = fetch_recipients()
        except Exception as e:
            logging.warning("シート読取でエラー。単一宛にフォールバック: %s", e)
            recs = []

        active = [r for r in recs if r.get("enabled") and r.get("userId")]
        if active:
            for r in active:
                _push_line(r["userId"], text)
                time.sleep(0.15)  # rate-limit 対策
            logging.info("LINE送信OK（複数宛 %d件）", len(active))
            return

    # フォールバック（単一宛）
    if LINE_USER_ID:
        _push_line(LINE_USER_ID, text)
        logging.info("LINE送信OK（単一宛）")
    else:
        logging.warning("宛先がありません（SHEETもLINE_USER_IDもなし）")


# ============ 戦略判定 ============
def _format_message(hit: Dict[str, Any]) -> str:
    """
    ヒット1件をLINE本文に整形
    必須: venue, race_no, start_at_iso, strategy, candidates(List[Dict])
    """
    venue = hit.get("venue", "")
    race_no = hit.get("race_no", "")
    start = hit.get("start_at_iso", "")
    st    = hit.get("strategy", "")
    cand  = hit.get("candidates", [])

    # 候補（最大5頭）
    lines = []
    for i, c in enumerate(cand[:5], 1):
        horse  = c.get("horse", f"ホース{i}")
        jockey = c.get("jockey", "")
        rk     = get_jrank(jockey) if jockey else "C"
        lines.append(f"{i}. {horse}（{jockey or '騎手不明'} / Rk:{rk}）")

    body = (
        f"【戦略アラート】{st}\n"
        f"{venue} {race_no}  発走 {start[11:16] if 'T' in start else start}\n\n"
        f"◎ 候補（最大５頭）\n" + "\n".join(lines)
    )
    return body

def find_strategy_matches() -> List[Dict[str, Any]]:
    """
    TODO: ここに実オッズ解析（戦略①〜④）を実装。
    今は TEST_HOOK=1 の時だけダミー1件を返す。
    """
    if not TEST_HOOK:
        return []

    sample = {
        "race_id": "TEST-ISO",
        "venue": "川崎",
        "race_no": "3R",
        "start_at_iso": (now_jst() + timedelta(minutes=7)).isoformat(),  # 7分後
        "strategy": "戦略③：3連単 軸1頭マルチ（候補≤5）",
        "candidates": [
            {"horse": "ホースA", "jockey": "矢野貴之"},
            {"horse": "ホースB", "jockey": "本田正重"},
            {"horse": "ホースC", "jockey": "笹川翼"},
        ],
    }
    return [sample]


# ============ メイン ============
def main():
    logging.info("ジョブ開始 host=%s pid=%s", socket.gethostname(), os.getpid())
    logging.info(
        "NOTIFIED_PATH=%s KILL_SWITCH=%s DRY_RUN=%s SHEET=%s TEST_HOOK=%s",
        NOTIFIED_PATH, KILL_SWITCH, DRY_RUN, SHEET_AVAILABLE, TEST_HOOK
    )

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
        race_id  = h.get("race_id", "")
        strategy = h.get("strategy", "")
        start_iso = h.get("start_at_iso", "")

        # 時間窓チェック
        if start_iso and not within_window(start_iso):
            logging.info("時間窓外のためスキップ: %s %s", race_id, start_iso)
            continue

        # デデュープキー（日付込み）
        key_date = start_iso[:10] if start_iso else today
        key = f"{race_id}|{strategy}|{key_date}"
        if notified.get(key):
            logging.info("重複通知を回避: %s", key)
            continue

        msg = _format_message(h).strip()
        if not msg:
            logging.info("message空のためスキップ: %s", key)
            continue

        send_line_message(msg)
        notified[key] = int(time.time())

    _save_notified(notified)
    logging.info("ジョブ終了")


if __name__ == "__main__":
    main()