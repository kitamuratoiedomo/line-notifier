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

# 受信者（スプレッドシート）オプション
USE_SHEET = os.getenv("USE_SHEET", "1") == "1"
SHEET_AVAILABLE = False
try:
    if USE_SHEET:
        from sheets_client import fetch_recipients  # あれば複数配信
        SHEET_AVAILABLE = True
except Exception as e:
    logging.warning("sheets_client を読み込めませんでした（単一宛先にフォールバック）: %s", e)
    SHEET_AVAILABLE = False

# オッズ取得＆戦略判定
try:
    from odds_client import list_today_raceids, fetch_tanfuku_odds
    from strategy_rules import eval_strategy
except Exception as e:
    # 起動だけは通す（DRY_RUNでの文面テストを可能にするため）
    list_today_raceids = None
    fetch_tanfuku_odds = None
    eval_strategy = None
    logging.warning("odds/strategy モジュール読み込みに失敗: %s", e)

# ====== 設定 ======
NOTIFIED_PATH = Path(os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json"))
NOTIFIED_PATH.parent.mkdir(parents=True, exist_ok=True)

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

# キルスイッチ / ドライラン
KILL_SWITCH = os.getenv("NOTIFY_ENABLED", "1") != "1"
DRY_RUN     = os.getenv("DRY_RUN_MESSAGE", "0") == "1"

# 通知許可の時間窓（発走○分前～○分後）
WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "5"))   # 既定: 5分前
WINDOW_AFTER_MIN  = int(os.getenv("WINDOW_AFTER_MIN", "2"))    # 既定: 2分後

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
    """
    start_iso: ISO8601推奨（例: 2025-08-09T14:05:00+09:00）
    設定: 発走「WINDOW_BEFORE_MIN 分前」〜「WINDOW_AFTER_MIN 分後」のみ True
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
    r = requests.post(url, headers=headers, json=payload, timeout=12)
    if r.status_code != 200:
        logging.warning("LINE送信失敗 user=%s status=%s body=%s", user_id, r.status_code, r.text)

def send_line_message(text: str) -> None:
    if KILL_SWITCH:
        logging.info("KILL_SWITCH有効のため送信しません。")
        return
    if DRY_RUN:
        logging.info("[DRY RUN] %s", text.replace("\n", " "))
        return

    # スプレッドシートが使えれば複数宛、ダメなら従来の単一宛
    if SHEET_AVAILABLE:
        try:
            recs = fetch_recipients()
        except Exception as e:
            logging.warning("シート取得失敗（単一宛にフォールバック）: %s", e)
            recs = []
        active = [r for r in recs if r.get("enabled") and r.get("userId")]
        if not active:
            if LINE_USER_ID:
                _push_line(LINE_USER_ID, text)
                logging.info("LINE送信OK（単一宛フォールバック）")
            else:
                logging.warning("宛先がありません（SHEETもLINE_USER_IDもなし）")
            return
        for r in active:
            _push_line(r["userId"], text)
            time.sleep(0.15)  # 軽いレート制御
        logging.info("LINE送信OK（複数宛 %d件）", len(active))
    else:
        if LINE_USER_ID:
            _push_line(LINE_USER_ID, text)
            logging.info("LINE送信OK（単一宛）")
        else:
            logging.warning("宛先がありません（SHEETもLINE_USER_IDもなし）")

# ============ メッセージ組み立て ============
def _build_message(r: Dict[str, Any], decision: Dict[str, Any]) -> str:
    venue = r.get("venue", "—")
    race_no = r.get("race_no", "—R")
    start_at = r.get("start_at_iso", "")
    try:
        dt = datetime.fromisoformat(start_at).astimezone(JST)
        when = dt.strftime("%m/%d %H:%M")
    except Exception:
        when = start_at or "—"

    # 人気とオッズ（上位4）
    horses = r.get("horses", [])
    head = []
    for h in horses[:4]:
        head.append(f"【{h['pop']}番人気】馬番{h['umaban']}（単勝 {h['odds']:.1f}）")
    head_txt = "\n".join(head) if head else "—"

    tickets = "\n".join(decision.get("tickets", [])) or "—"

    msg = (
f"《対象レース検出》\n"
f"{venue} {race_no}（{when} 発走想定）\n"
f"戦略: {decision.get('strategy','—')}\n"
f"\n"
f"— 人気/オッズ（上位）—\n"
f"{head_txt}\n"
f"\n"
f"— 買い目（簡易表記）—\n"
f"{tickets}\n"
f"\n"
f"{decision.get('roi','')} / {decision.get('hit','')}\n"
f"\n"
f"※オッズは締切直前まで変化します。\n"
f"※的中・回収率は保証しません。余裕資金でお願いします。"
    )
    return msg

# ============ 戦略判定 ============
def find_strategy_matches() -> List[Dict[str, Any]]:
    hits: List[Dict[str, Any]] = []

    if not (list_today_raceids and fetch_tanfuku_odds and eval_strategy):
        logging.warning("odds_client / strategy_rules が読み込めないため、本番判定をスキップします。")
        return hits

    race_ids = list_today_raceids()
    if not race_ids:
        logging.info("対象レースIDが空です（env RACEIDS を設定して試験してください）")
        return hits

    for rid in race_ids:
        data = fetch_tanfuku_odds(rid)
        if not data or not data.get("horses"):
            continue

        decision = eval_strategy(data["horses"])
        if not decision:
            continue

        msg = _build_message(data, decision)
        hits.append({
            "race_id": data["race_id"],
            "venue": data["venue"],
            "race_no": data["race_no"],
            "start_at_iso": data["start_at_iso"],
            "strategy": decision["strategy"],
            "message": msg,
        })

    return hits

# ============ メイン ============
def main():
    logging.info("ジョブ開始 host=%s pid=%s", socket.gethostname(), os.getpid())
    logging.info("NOTIFIED_PATH=%s KILL_SWITCH=%s DRY_RUN=%s", NOTIFIED_PATH, KILL_SWITCH, DRY_RUN)

    notified = _load_notified()
    today = now_jst().strftime("%Y-%m-%d")
    notified = prune_notified(notified,