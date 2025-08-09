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

# ====== オプション依存（存在しなければフォールバック） ======
# Sheetsクライアント（複数宛先）
USE_SHEET = os.getenv("USE_SHEET", "1") == "1"
SHEET_AVAILABLE = False
try:
    if USE_SHEET:
        from sheets_client import fetch_recipients  # -> List[Dict]: {userId, enabled(bool), name?}
        SHEET_AVAILABLE = True
except Exception as e:
    logging.warning("sheets_client を読み込めませんでした（単一宛先にフォールバック）: %s", e)
    SHEET_AVAILABLE = False

# オッズ・レース情報
from odds_client import (
    list_today_raceids,       # -> List[str]
    fetch_tanfuku_odds,       # -> Dict{race_id, venue, race_no, start_at_iso, horses:[{pop, umaban, odds}]}
    get_race_start_iso,       # -> "YYYY-MM-DDTHH:MM:SS+09:00"
)

# 騎手ランク（戦略③は1番人気のみ表示。それ以外は1〜3番人気）
try:
    from jockey_rank import get_jrank  # -> "A" / "B" / "C" / "—"
except Exception:
    def get_jrank(_: str) -> str:
        return "—"  # 名前が取れない環境でも落ちないように


# ====== 環境変数・基本設定 ======
NOTIFIED_PATH = Path(os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json"))
NOTIFIED_PATH.parent.mkdir(parents=True, exist_ok=True)

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

# 送信制御
KILL_SWITCH = os.getenv("NOTIFY_ENABLED", "1") != "1"           # 1で有効、0で停止
DRY_RUN     = os.getenv("DRY_RUN_MESSAGE", "0") == "1"          # 1で送らずログのみ

# 通知時間窓（販売締切時刻＝発走時刻 − CUTOFF_OFFSET_MIN を基準）
# 例: 販売締切5分前に通知したい → CUTOFF_OFFSET_MIN=5, WINDOW_BEFORE_MIN=5, WINDOW_AFTER_MIN=0 など
CUTOFF_OFFSET_MIN = int(os.getenv("CUTOFF_OFFSET_MIN", "5"))    # 販売締切 = 発走 − 5分（デフォルト）
WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "5"))    # 締切のX分前から
WINDOW_AFTER_MIN  = int(os.getenv("WINDOW_AFTER_MIN", "0"))     # 締切のY分後まで

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

def within_window(cutoff_iso: str) -> bool:
    """
    cutoff_iso: 販売締切のISO時刻（= 発走時刻 - CUTOFF_OFFSET_MIN 分）
    """
    t = datetime.fromisoformat(cutoff_iso)
    now = now_jst()
    return (t - timedelta(minutes=WINDOW_BEFORE_MIN)) <= now <= (t + timedelta(minutes=WINDOW_AFTER_MIN))

def hhmm(dt_iso: str) -> str:
    try:
        return datetime.fromisoformat(dt_iso).strftime("%H:%M")
    except Exception:
        return dt_iso


# ============ LINE送信系 ============
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

    # シートに受信者がいれば複数送信、なければ単一宛
    if SHEET_AVAILABLE:
        try:
            recs = fetch_recipients()
            active = [r for r in recs if r.get("enabled") and r.get("userId")]
        except Exception as e:
            logging.warning("fetch_recipients failed: %s", e)
            active = []

        if active:
            for r in active:
                _push_line(r["userId"], text)
                time.sleep(0.15)  # rate-limit 予防
            logging.info("LINE送信OK（複数宛 %d件）", len(active))
            return

    # フォールバック（単一宛）
    if LINE_USER_ID:
        _push_line(LINE_USER_ID, text)
        logging.info("LINE送信OK（単一宛）")
    else:
        logging.warning("宛先がありません（SHEETもLINE_USER_IDもなし）")


# ============ 戦略判定ロジック ============
def pick_top(horses: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    return [h for h in horses if h.get("pop", 999) <= n]

def find_strategy_for_race(od: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    od: fetch_tanfuku_odds の返り値
        {race_id, venue, race_no, start_at_iso, horses:[{pop, umaban, odds}]}
    戻り: 一致した戦略 dict or None
    """
    horses = od.get("horses", [])
    if len(horses) < 4:
        return None

    # 人気別オッズ（存在しなければ大きめの数）
    def get_odds_by_pop(p: int) -> float:
        for h in horses:
            if h.get("pop") == p:
                return float(h.get("odds", 9999))
        return 9999.0

    o1, o2, o3, o4 = get_odds_by_pop(1), get_odds_by_pop(2), get_odds_by_pop(3), get_odds_by_pop(4)

    # ---- 戦略① 1〜3番人気 三連単BOX
    # 1番人気: 2.0〜10.0、2〜3番人気: 10.0未満、4番人気: 15.0以上
    if (2.0 <= o1 <= 10.0) and (o2 < 10.0) and (o3 < 10.0) and (o4 >= 15.0):
        nums = [h["umaban"] for h in pick_top(horses, 3)]
        detail = f"三連単BOX（{nums[0]}-{nums[1]}-{nums[2]}）6点"
        return {
            "strategy": "①",
            "buy_detail": detail,
            "nums": nums,
            "note": "1〜3番人気BOX",
        }

    # ---- 戦略② 1番人気1着固定 × 2・3番人気（三連単）
    # 1番人気: 2.0未満、2〜3番人気: 10.0未満
    if (o1 < 2.0) and (o2 < 10.0) and (o3 < 10.0):
        n1 = [h for h in horses if h["pop"] == 1][0]["umaban"]
        n2 = [h for h in horses if h["pop"] == 2][0]["umaban"]
        n3 = [h for h in horses if h["pop"] == 3][0]["umaban"]
        detail = f"【戦略一致】②\n1着固定（{n1}）- 2,3番人気（{n2},{n3}）三連単2点"
        return {
            "strategy": "②",
            "buy_detail": detail,
            "nums": [n1, n2, n3],
            "note": "1人気1着固定×2・3人気",
        }

    # ---- 戦略③ 1着固定 × 10〜20倍流し（更新版）
    # 1番人気: 1.5以下、さらに 2番人気オッズ >= 10.0 を条件追加
    # 相手は「2番人気以下」かつ「10〜20倍」の馬から最大4頭
    if (o1 <= 1.5) and (o2 >= 10.0):
        candidates = [h for h in horses if h["pop"] >= 2 and 10.0 <= float(h["odds"]) <= 20.0]
        if candidates:
            candidates = candidates[:4]
            n1 = [h for h in horses if h["pop"] == 1][0]["umaban"]
            others = [h["umaban"] for h in candidates]
            # 買い目は「1 → 相手4頭 → 相手4頭」（2・3着入れ替え）最大12点
            detail = (
                f"【戦略一致】③\n"
                f"1着固定（{n1}）- 相手流し（{','.join(map(str, others))}）\n"
                f"三連単『1→相手→相手』最大12点（相手は10〜20倍から最大4頭）"
            )
            return {
                "strategy": "③",
                "buy_detail": detail,
                "nums": [n1] + others,
                "note": "1着固定×10〜20倍流し（相手最大4頭）",
            }

    # ---- 戦略④ 3着固定（3番人気固定）三連単 2点
    # 1・2番人気: 3.0以下、3番人気: 6.0〜10.0、4番人気: 15.0以上
    if (o1 <= 3.0) and (o2 <= 3.0) and (6.0 <= o3 <= 10.0) and (o4 >= 15.0):
        n1 = [h for h in horses if h["pop"] == 1][0]["umaban"]
        n2 = [h for h in horses if h["pop"] == 2][0]["umaban"]
        n3 = [h for h in horses if h["pop"] == 3][0]["umaban"]
        # 戦略②と同じフォーマット要望
        detail = (
            f"【戦略一致】④\n"
            f"3着固定　（{n1}×{n2}）- {n3}（３着固定）、三連単2点"
        )
        return {
            "strategy": "④",
            "buy_detail": detail,
            "nums": [n1, n2, n3],
            "note": "3着固定（3番人気固定）",
        }

    return None


def build_message(od: Dict[str, Any], strat: Dict[str, Any],
                  start_iso: str, cutoff_iso: str) -> str:
    """
    通知メッセージ生成
    ※ 騎手名はデータ元未取得環境でも落ちないよう、省略安全設計
    """
    race_url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{od['race_id']}"
    venue = od.get("venue", "地方")
    race_no = od.get("race_no", "—R")
    start_hm  = hhmm(start_iso)
    cutoff_hm = hhmm(cutoff_iso)

    header = f"発走 {start_hm}（販売締切 {cutoff_hm}）"
    body   = strat["buy_detail"]
    footer = f"単勝オッズ確認: {race_url}"

    # 騎手表示ルール（省略運用。将来、馬番→騎手名のマップが入れば拡張）
    # ③は1番人気のみ表示ルールだが、現状は名前ソースがないためスキップ
    # ①②④は1〜3番人気の騎手表示ルールも同様にスキップ

    return f"{header}\n{body}\n{footer}"


# ============ 戦略判定の実行 ============
def find_strategy_matches() -> List[Dict[str, Any]]:
    """
    全レース（list_today_raceids）に対して:
      - オッズ取得
      - 発走→販売締切（発走−CUTOFF_OFFSET_MIN）計算
      - 戦略判定
      - 1件につき {race_id, start_at_iso, cutoff_at_iso, strategy, message} を返却
    """
    results: List[Dict[str, Any]] = []
    race_ids = list_today_raceids()
    if not race_ids:
        logging.info("対象レースIDがありません（RACEIDSなどの設定を確認）")
        return results

    for rid in race_ids:
        od = fetch_tanfuku_odds(rid)
        if not od:
            continue

        # 発走時刻は odds_client 既定 or 正式に取り直し
        try:
            start_iso = get_race_start_iso(rid)
        except Exception:
            # 取得失敗時はオッズ側の推定を利用
            start_iso = od.get("start_at_iso")

        if not start_iso:
            continue

        # 販売締切基準（発走−CUTOFF_OFFSET_MIN）
        cutoff_iso = (datetime.fromisoformat(start_iso) - timedelta(minutes=CUTOFF_OFFSET_MIN)).isoformat()

        strat = find_strategy_for_race(od)
        if not strat:
            continue

        msg = build_message(od, strat, start_iso=start_iso, cutoff_iso=cutoff_iso)
        results.append({
            "race_id": rid,
            "start_at_iso": start_iso,
            "cutoff_at_iso": cutoff_iso,
            "strategy": strat["strategy"],
            "message": msg,
        })

    return results


# ============ メイン ============
def main():
    logging.info("ジョブ開始 host=%s pid=%s", socket.gethostname(), os.getpid())
    logging.info("NOTIFIED_PATH=%s KILL_SWITCH=%s DRY_RUN=%s SHEET=%s",
                 NOTIFIED_PATH, KILL_SWITCH, DRY_RUN, SHEET_AVAILABLE)

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
        race_id   = h.get("race_id", "")
        strategy  = h.get("strategy", "")
        cutoff_iso = h.get("cutoff_at_iso", "")
        start_iso  = h.get("start_at_iso", "")

        # 販売締切の時間窓判定
        if cutoff_iso and not within_window(cutoff_iso):
            logging.info("（締切基準）時間窓外のためスキップ: %s %s", race_id, cutoff_iso)
            continue

        # 日付入り強デデュープキー（同一戦略・同一レース・同一日で1回のみ）
        key_date = (start_iso or cutoff_iso)[:10] if (start_iso or cutoff_iso) else today
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