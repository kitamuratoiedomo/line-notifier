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

# ===== local modules =====
from odds_client import fetch_tanfuku_odds
# どちらかが実装されていれば使う（両方無ければ自動フォールバック）
try:
    from odds_client import get_sale_close_iso  # ネット販売締切のISO（推定）
except Exception:
    get_sale_close_iso = None
try:
    from odds_client import get_race_start_iso   # 発走時刻のISO
except Exception:
    get_race_start_iso = None

# ---- Google Sheets (受信者 & 既送ログ) ----
SHEET_AVAILABLE = False
try:
    from sheets_client import (
        fetch_recipients,
        ensure_sent_log_sheet,
        already_sent,
        append_sent_log,
    )
    SHEET_AVAILABLE = True
except Exception as e:
    logging.warning("sheets_client を読み込めませんでした（単一宛にフォールバック）: %s", e)
    SHEET_AVAILABLE = False

# ====== ENV ======
NOTIFIED_PATH = Path(os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json"))
NOTIFIED_PATH.parent.mkdir(parents=True, exist_ok=True)

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

KILL_SWITCH = os.getenv("NOTIFY_ENABLED", "1") != "1"
DRY_RUN = os.getenv("DRY_RUN_MESSAGE", "0") == "1"

WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "5"))   # 5分前から
WINDOW_AFTER_MIN  = int(os.getenv("WINDOW_AFTER_MIN", "0"))    # 0で即切り推奨

TEST_HOOK = os.getenv("TEST_HOOK", "0") == "1"  # 手動テスト用

JST = timezone(timedelta(hours=9))
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def now_jst() -> datetime:
    return datetime.now(JST)

# ====== storage (フォールバック用) ======
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

def within_window(target_iso: str) -> bool:
    t = datetime.fromisoformat(target_iso)
    now = now_jst()
    return (t - timedelta(minutes=WINDOW_BEFORE_MIN)) <= now <= (t + timedelta(minutes=WINDOW_AFTER_MIN))

# ===== LINE =====
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

    # 複数宛（シート） or 単一宛
    if SHEET_AVAILABLE:
        recs = fetch_recipients()
        active = [r for r in recs if r.get("enabled") and r.get("userId")]
        if not active and LINE_USER_ID:
            _push_line(LINE_USER_ID, text)
            logging.info("LINE送信OK（単一宛 フォールバック）")
            return
        for r in active:
            _push_line(r["userId"], text)
            time.sleep(0.15)
        logging.info("LINE送信OK（複数宛 %d件）", len(active))
    else:
        if LINE_USER_ID:
            _push_line(LINE_USER_ID, text)
            logging.info("LINE送信OK（単一宛）")
        else:
            logging.warning("宛先がありません（SHEETもLINE_USER_IDもなし）")

# ===== 戦略ロジック（ここは既存のあなたの実装を使ってOK） =====
def find_strategy_matches() -> List[Dict[str, Any]]:
    """
    ここでは「単勝オッズの上位から条件判定」する関数を想定。
    戻り値の各要素には最低限つぎのキーが入っている前提:
    - race_id
    - strategy (例: '① 1〜3番人気三連単BOX')
    - candidates: [(umaban, odds), ...] 最大5
    可能なら venue / race_no / sale_close_iso / start_at_iso も含める
    """
    # あなたの既存ロジックを呼ぶ前提。ここではダミー0件にしておく。
    return []

# ===== メッセージ整形 =====
def _hm(iso: str) -> str:
    try:
        return datetime.fromisoformat(iso).strftime("%H:%M")
    except Exception:
        return "—:—"

def build_message(hit: Dict[str, Any], venue: str, race_no: str, start_iso: str, close_iso: str, odds_url: str) -> str:
    strategy = hit.get("strategy", "")
    cand = hit.get("candidates", [])
    lines = []
    lines.append(f"【戦略一致】{strategy}")
    lines.append(f"{venue} {race_no} 発走 { _hm(start_iso) }（販売締切 予定 { _hm(close_iso) }）")
    lines.append("")
    lines.append("◎ 候補（最大5頭）")
    for i, (umaban, odds) in enumerate(cand[:5], 1):
        lines.append(f"{i}. #{umaban} 単勝:{odds}")
    lines.append("")
    lines.append("※この通知は『販売締切（発走5分前想定）5分前』基準です。")
    if odds_url:
        lines.append(odds_url)
    return "\n".join(lines)

# ===== メイン =====
def main():
    logging.info("ジョブ開始 host=%s pid=%s", socket.gethostname(), os.getpid())
    logging.info("NOTIFIED_PATH=%s KILL_SWITCH=%s DRY_RUN=%s SHEET=%s TEST_HOOK=%s",
                 NOTIFIED_PATH, KILL_SWITCH, DRY_RUN, SHEET_AVAILABLE, TEST_HOOK)

    notified_local = _load_notified()

    # sent_log タブを保証
    if SHEET_AVAILABLE:
        ensure_sent_log_sheet()

    hits = find_strategy_matches()
    logging.info("HITS=%s", len(hits))
    if not hits:
        _save_notified(notified_local)
        logging.info("戦略一致なし（通知なし）")
        return

    for h in hits:
        rid = h.get("race_id", "")
        if not rid:
            continue

        # 楽天の単勝ページURL
        odds_url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"

        # 会場・R・時刻の補完
        venue = h.get("venue") or "地方"
        race_no = h.get("race_no") or "—R"

        # 締切ISO（推定）・発走ISO
        close_iso = h.get("sale_close_iso")
        if not close_iso and get_sale_close_iso:
            try:
                close_iso = get_sale_close_iso(rid)
            except Exception as e:
                logging.warning("販売締切取得失敗 rid=%s err=%s", rid, e)
        start_iso = h.get("start_at_iso")
        if not start_iso and get_race_start_iso:
            try:
                start_iso = get_race_start_iso(rid)
            except Exception:
                pass
        # どちらも無ければ10分後/5分後で仮置き
        if not start_iso:
            start_iso = (now_jst() + timedelta(minutes=10)).isoformat()
        if not close_iso:
            close_iso = (datetime.fromisoformat(start_iso) - timedelta(minutes=5)).isoformat()

        # 通知ウィンドウ判定（販売締切をターゲットに）
        if not within_window(close_iso):
            logging.info("時間窓外のためスキップ: %s %s", rid, close_iso)
            continue

        # sent_key（分単位で丸め）
        sent_key = f"{rid}|{h.get('strategy','')[:8]}|{datetime.fromisoformat(close_iso).strftime('%Y-%m-%dT%H:%M')}"
        # まずシートの sent_log を見る
        if SHEET_AVAILABLE and already_sent(sent_key):
            logging.info("既送(シート)のためスキップ: %s", sent_key)
            continue
        # ついでにローカルも見る（コンテナ内の重複抑止）
        if notified_local.get(sent_key):
            logging.info("既送(ローカル)のためスキップ: %s", sent_key)
            continue

        # venue / race_no を odds ページから補完（必要時）
        if venue == "地方" or race_no == "—R":
            try:
                info = fetch_tanfuku_odds(rid) or {}
                venue = info.get("venue", venue)
                race_no = info.get("race_no", race_no)
            except Exception:
                pass

        msg = build_message(h, venue, race_no, start_iso, close_iso, odds_url)
        send_line_message(msg)

        notified_local[sent_key] = int(time.time())
        if SHEET_AVAILABLE:
            append_sent_log(rid, h.get("strategy",""), close_iso, sent_key)

    _save_notified(notified_local)
    logging.info("ジョブ終了")

if __name__ == "__main__":
    main()