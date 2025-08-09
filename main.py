# main.py — 完全貼り替え版（DRY_RUN時はodds_clientを読み込まない）

import os
import json
import time
import logging
import socket
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone

import requests
from jockey_rank import get_jrank  # 騎手ランク（A/B/C）を使う想定

# ====== 環境変数 ======
LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

# Render Free は /var/data が権限で落ちることがあるので /tmp をデフォルトに
NOTIFIED_PATH = Path(os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json"))
NOTIFIED_PATH.parent.mkdir(parents=True, exist_ok=True)

# 送信キルスイッチ（0で停止）
KILL_SWITCH = os.getenv("NOTIFY_ENABLED", "1") != "1"

# テスト通知モード（1でDRY-RUN）
DRY_RUN = os.getenv("DRY_RUN_MESSAGE", "0") == "1"

# 通知許可の時間窓（発走○分前〜○分後）
WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "15"))
WINDOW_AFTER_MIN  = int(os.getenv("WINDOW_AFTER_MIN", "5"))

# ログ
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
JST = timezone(timedelta(hours=9))


# ====== 本番用の依存（DRY_RUNでなければ読み込む）======
_odds_available = False
if not DRY_RUN:
    try:
        # ここはあなたの odds_client 実装に合わせてください
        from odds_client import list_today_raceids, fetch_tanfuku_odds
        _odds_available = True
    except Exception as e:
        # 本番でも、odds_clientが未実装／未配置でも落とさない
        logging.warning("odds_client を読み込めませんでした（本番機能はスキップ）。detail=%s", e)
else:
    # DRY_RUN中は絶対に import しない（ここが今日の“落ちないポイント”）
    pass


# ====== ユーティリティ ======
def now_jst() -> datetime:
    return datetime.now(JST)

def _load_notified() -> Dict[str, Any]:
    if NOTIFIED_PATH.exists():
        try:
            return json.loads(NOTIFIED_PATH.read_text())
        except Exception as e:
            logging.warning("通知済みファイル読み込み失敗: %s", e)
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
    start_iso: "YYYY-MM-DDTHH:MM:SS+09:00"
    発走WINDOW_BEFORE_MIN分前〜WINDOW_AFTER_MIN分後のみ True
    """
    try:
        t = datetime.fromisoformat(start_iso)
    except Exception:
        # "HH:MM"だけ来た場合は当日扱い
        base = now_jst().strftime("%Y-%m-%d")
        t = datetime.fromisoformat(f"{base}T{start_iso}:00+09:00")
    now = now_jst()
    return (t - timedelta(minutes=WINDOW_BEFORE_MIN)) <= now <= (t + timedelta(minutes=WINDOW_AFTER_MIN))


# ====== LINE送信 ======
def send_line_message(text: str) -> None:
    if KILL_SWITCH:
        logging.info("KILL_SWITCH有効のため送信しません。")
        return
    if not LINE_ACCESS_TOKEN or not LINE_USER_ID:
        logging.warning("LINE 環境変数が未設定のため送信しません。")
        return

    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {LINE_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"to": LINE_USER_ID, "messages": [{"type": "text", "text": text}]}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        if r.status_code != 200:
            logging.warning("LINE送信失敗 status=%s body=%s", r.status_code, r.text)
        else:
            logging.info("LINE送信OK")
    except Exception as e:
        logging.exception("LINE送信で例外: %s", e)


# ====== 戦略判定（土台） ======
def _format_message_for_strategy3(venue: str, race_no: str, start_iso: str) -> str:
    # ここはテスト用の文面。実オッズに合わせた文面は後で差し替え
    lines = []
    lines.append("【戦略アラート】戦略③：3連単軸1頭マルチ（候補<=5）")
    try:
        hhmm = datetime.fromisoformat(start_iso).strftime("%H:%M")
    except Exception:
        hhmm = "時刻不明"
    lines.append(f"{venue} {race_no}  発走 {hhmm}")
    lines.append("")
    lines.append("◎ 候補（最大5頭）")
    # ダミーの見本。騎手ランクAPIの利用例を見せるために名前だけ入れる
    demo = [("ホースA", "矢野貴之"), ("ホースB", "本田正重"), ("ホースC", "笹川翼")]
    for i, (h, j) in enumerate(demo, 1):
        lines.append(f"{i}. {h}（{j} / Rk:{get_jrank(j)}）")
    if DRY_RUN:
        lines.append("")
        lines.append("注記: DRY_RUN_MESSAGE=1 による動作確認")
    return "\n".join(lines)

def find_strategy_matches() -> List[Dict[str, Any]]:
    """
    ここで『対象レース抽出＋人気/オッズ解析→戦略判定』を行い、ヒットしたレースを返す。
    """
    # --- DRY-RUN: ダミーを必ず1件返す ---
    if DRY_RUN:
        start = (now_jst() + timedelta(minutes=10)).isoformat()
        return [{
            "race_id": "DRYRUN-TEST",
            "venue": "川崎",
            "race_no": "3R",
            "start_at_iso": start,
            "strategy": "戦略③：3連単軸1頭マルチ",
            "message": _format_message_for_strategy3("川崎", "3R", start),
        }]

    # --- 本番: odds_client が使えない場合は何も返さない（落とさない）---
    if not _odds_available:
        return []

    # ここから本番処理の雛形（実装済みの odds_client に合わせて拡張してください）
    hits: List[Dict[str, Any]] = []
    try:
        race_ids = list_today_raceids()  # 例: 今日のレースID一覧
        for rid in race_ids:
            # 例: 単複オッズ＋人気データを取得
            data = fetch_tanfuku_odds(rid)  # あなたの戻り値仕様に合わせて加工
            # TODO: strategy①〜④のロジック判定をここに実装
            # if matched:
            #     hits.append({...})
            pass
    except Exception as e:
        logging.warning("本番オッズ処理で例外: %s", e)

    return hits


# ====== メイン ======
def main():
    logging.info(
        "ジョブ開始 host=%s pid=%s",
        socket.gethostname(),
        os.getpid(),
    )
    logging.info(
        "NOTIFIED_PATH=%s KILL_SWITCH=%s DRY_RUN=%s",
        NOTIFIED_PATH, KILL_SWITCH, DRY_RUN
    )

    notified = prune_notified(_load_notified(), keep_date=now_jst().strftime("%Y-%m-%d"))
    hits = find_strategy_matches()
    logging.info("HITS=%s", len(hits))

    if not hits:
        logging.info("戦略一致なし（通知なし）")
        _save_notified(notified)
        return

    for h in hits:
        race_id   = h.get("race_id", "")
        strategy  = h.get("strategy", "")
        start_iso = h.get("start_at_iso", "")

        # 時間窓チェック
        if start_iso and not within_window(start_iso):
            logging.info("時間窓外のためスキップ: %s %s", race_id, start_iso)
            continue

        # 重複回避キー（当日単位）
        key_date = start_iso[:10] if start_iso else now_jst().strftime("%Y-%m-%d")
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