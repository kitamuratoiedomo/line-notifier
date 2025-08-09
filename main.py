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
from jockey_rank import get_jrank  # 既存のまま利用

# ====== シート連携（存在しなければフォールバック） ======
USE_SHEET = os.getenv("USE_SHEET", "1") == "1"
SHEET_AVAILABLE = False
try:
    if USE_SHEET:
        from sheets_client import fetch_recipients
        SHEET_AVAILABLE = True
except Exception as e:
    logging.warning("sheets_client を読み込めませんでした（単一宛先にフォールバック）: %s", e)
    SHEET_AVAILABLE = False

# ====== オッズ・レース情報（存在しなければフォールバック） ======
ODDS_AVAILABLE = False
try:
    from odds_client import list_today_raceids, fetch_tanfuku_odds, get_race_start_iso
    ODDS_AVAILABLE = True
except Exception as e:
    logging.warning("odds_client を読み込めませんでした（本番機能はスキップ）。detail=%s", e)
    ODDS_AVAILABLE = False

# ====== 設定 ======
NOTIFIED_PATH = Path(os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json"))
NOTIFIED_PATH.parent.mkdir(parents=True, exist_ok=True)

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

# キルスイッチ / ドライラン
KILL_SWITCH = os.getenv("NOTIFY_ENABLED", "1") != "1"
DRY_RUN     = os.getenv("DRY_RUN_MESSAGE", "0") == "1"
TEST_HOOK   = os.getenv("TEST_HOOK", "0") == "1"

# 締切基準の設定（※ここが今回の主役）
# 例：CUTOFF_OFFSET_MIN=5 → 発走5分前を「販売締切」として扱う
CUTOFF_OFFSET_MIN = int(os.getenv("CUTOFF_OFFSET_MIN", "5"))

# 通知許可の時間窓（締切の何分前後で送るか）
# デフォルトはピッタリのみ（0 / 0）。取りこぼし防止で±1分などにしてもOK
WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "0"))
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

def within_cutoff_window(start_iso: str) -> bool:
    """
    start_iso: 発走の ISO8601 ('YYYY-MM-DDTHH:MM:SS+09:00')
    【販売締切 = 発走 CUTOFF_OFFSET_MIN 分前】を基準に、
    WINDOW_BEFORE_MIN 分前〜WINDOW_AFTER_MIN 分後 だけ True。
    """
    # start_iso を datetime に
    try:
        start = datetime.fromisoformat(start_iso)
    except Exception:
        # 古い互換: "HH:MM" だけ来る場合は当日扱い
        base = now_jst().strftime("%Y-%m-%d")
        start = datetime.fromisoformat(f"{base}T{start_iso}:00+09:00")

    cutoff = start - timedelta(minutes=CUTOFF_OFFSET_MIN)
    now = now_jst()
    return (cutoff - timedelta(minutes=WINDOW_BEFORE_MIN)) <= now <= (cutoff + timedelta(minutes=WINDOW_AFTER_MIN))


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

    # スプレッドシートが使えれば複数宛、ダメなら従来の単一宛
    if SHEET_AVAILABLE:
        try:
            recs = fetch_recipients()
        except Exception as e:
            logging.warning("fetch_recipients failed: %s", getattr(e, "message", str(e)))
            recs = []
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


# ============ 戦略判定（本番 or テスト） ============
def find_strategy_matches() -> List[Dict[str, Any]]:
    """
    本番：odds_client で RACEIDS を順に解析 → 条件一致なら返却
    テスト：TEST_HOOK=1 のときに販売締切基準で1件だけ返す
    返却形式：
      {
        "race_id": "...",
        "venue": "...",
        "race_no": "3R",
        "start_at_iso": "発走のISO（ここから内部で締切= -5分を計算）",
        "strategy": "戦略名",
        "message": "LINE本文"
      }
    """
    # テスト用フック（販売締切ロジックで流れを確認）
    if TEST_HOOK:
        start_iso = (now_jst() + timedelta(minutes=10)).isoformat()  # 発走10分後 → 締切5分後
        return [{
            "race_id": "TEST-ISO",
            "venue": "テスト場",
            "race_no": "1R",
            "start_at_iso": start_iso,
            "strategy": "（テスト）",
            "message": "【テスト】販売締切基準の通知テストです。\nこのメッセージが締切タイミングで届けばOK。",
        }]

    if not ODDS_AVAILABLE:
        return []

    race_ids = list_today_raceids()
    if not race_ids:
        return []

    hits: List[Dict[str, Any]] = []
    for rid in race_ids:
        # 発走（start_iso）を取る → 締切は後で -5分
        try:
            start_iso = get_race_start_iso(rid)
        except Exception as e:
            logging.warning("発走取得失敗 rid=%s err=%s", rid, e)
            continue

        # オッズ取得（単勝ページ）
        data = fetch_tanfuku_odds(rid)
        if not data:
            continue
        horses = data.get("horses") or []
        if len(horses) < 3:
            continue

        # 人気順情報
        pops = sorted(horses, key=lambda x: x["odds"])
        # 1〜4番人気の単勝オッズを拾う（足りなければ None）
        def odds_at(n: int) -> float | None:
            return pops[n-1]["odds"] if len(pops) >= n else None

        o1, o2, o3, o4 = odds_at(1), odds_at(2), odds_at(3), odds_at(4)

        # ここに戦略①〜④の判定を入れる（簡略版：条件成立でヒット）
        strategy = None
        # ① 1〜3番人気三連単BOX
        if (o1 is not None and o2 is not None and o3 is not None and o4 is not None
            and 2.0 <= o1 <= 10.0 and o2 < 10.0 and o3 < 10.0 and o4 >= 15.0):
            strategy = "① 1〜3番人気三連単BOX"

        # ② 1番人気1着固定 × 2・3番人気（三連単）
        if strategy is None and (o1 is not None and o2 is not None and o3 is not None
            and o1 < 2.0 and o2 < 10.0 and o3 < 10.0):
            strategy = "② 1番人気1着固定 × 2・3番人気（三連単）"

        # ④ 3着固定（3番人気固定）三連単
        if strategy is None and (o1 is not None and o2 is not None and o3 is not None and o4 is not None
            and o1 <= 3.0 and o2 <= 3.0 and 6.0 <= o3 <= 10.0 and o4 >= 15.0):
            strategy = "④ 3着固定（三連単）"

        # ③は単勝テーブルだけでは「相手10〜20倍の4頭」を厳密抽出できないため、
        # ここでは成立判定をスキップ（別ページ解析で拡張予定）。
        # if strategy is None and o1 is not None and o1 <= 1.5:
        #    ...

        if strategy:
            venue = data.get("venue") or "地方"
            race_no = data.get("race_no") or "—R"

            # 騎手ランクの表示（要求通りの出し分けは別途詳細化可能）
            # 単勝上位の馬番だけ拾っておく（騎手名の取得は別処理にしている想定）
            top_umaban = [h["umaban"] for h in pops[:3]]
            jinfo = []
            for _ in top_umaban:
                # 騎手名が手元に無い場合は仮（jockey_rank 側で名前→ランク変換）
                # 実運用では馬毎の騎手名を別途取得して get_jrank(name) に掛ける
                # ここでは見栄え用プレースホルダ
                name = "騎手"
                jinfo.append(f"{name}:{get_jrank(name)}")

            msg = (
                f"【戦略一致】{strategy}\n"
                f"{venue} {race_no}\n"
                f"※この通知は『販売締切（発走{CUTOFF_OFFSET_MIN}分前）』基準です。\n"
                f"上位人気 騎手ランク: {', '.join(jinfo)}"
            )

            hits.append({
                "race_id": rid,
                "venue": venue,
                "race_no": race_no,
                "start_at_iso": start_iso,  # ここは発走のISO（締切は内部で -5分）
                "strategy": strategy,
                "message": msg,
            })

    return hits


# ============ メイン ============
def main():
    logging.info(
        "ジョブ開始 host=%s pid=%s", socket.gethostname(), os.getpid()
    )
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
        logging.info("ジョブ終了")
        return

    for h in hits:
        race_id  = h.get("race_id", "")
        strategy = h.get("strategy", "")
        start_iso = h.get("start_at_iso", "")  # 発走ISO（ここから締切を算出）

        # 「販売締切」基準で時間窓チェック
        if start_iso and not within_cutoff_window(start_iso):
            logging.info("時間窓外のためスキップ: %s %s", race_id, start_iso)
            continue

        key_date = start_iso[:10] if start_iso else today
        key = f"{race_id}|{strategy}|{key_date}|cutoff-{CUTOFF_OFFSET_MIN}"
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