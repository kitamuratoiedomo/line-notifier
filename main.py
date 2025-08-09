# main.py ーーー 全部差し替え版
import os
import json
import time
import logging
import socket
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone

import requests

# ====== 外部モジュール（存在すれば使う） ======
from odds_client import list_today_raceids, fetch_tanfuku_odds, get_race_start_iso

# （任意）スプレッドシート連携：存在しなければ単一宛てにフォールバック
USE_SHEET = os.getenv("USE_SHEET", "1") == "1"
SHEET_AVAILABLE = False
try:
    if USE_SHEET:
        from sheets_client import fetch_recipients
        SHEET_AVAILABLE = True
except Exception as e:
    logging.warning("sheets_client を読み込めませんでした（単一宛先にフォールバック）: %s", e)
    SHEET_AVAILABLE = False

# ====== 環境設定 ======
JST = timezone(timedelta(hours=9))

NOTIFIED_PATH = Path(os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json"))
NOTIFIED_PATH.parent.mkdir(parents=True, exist_ok=True)

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

# 配信のON/OFFとドライラン
KILL_SWITCH = os.getenv("NOTIFY_ENABLED", "1") != "1"
DRY_RUN     = os.getenv("DRY_RUN_MESSAGE", "0") == "1"

# 通知ウィンドウ（ネット販売5分前 ≒ 発走10分前想定）
WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "10"))
WINDOW_AFTER_MIN  = int(os.getenv("WINDOW_AFTER_MIN", "0"))

# ログ
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ====== 共通ユーティリティ ======
def now_jst() -> datetime:
    return datetime.now(JST)

def _load_notified() -> Dict[str, Any]:
    if NOTIFIED_PATH.exists():
        try:
            return json.loads(NOTIFIED_PATH.read_text())
        except Exception:
            return {}
    return {}

def _save_notified(obj: Dict[str, Any]) -> None:
    NOTIFIED_PATH.write_text(json.dumps(obj, ensure_ascii=False))
    logging.info("notified saved: %s (bytes=%d)", NOTIFIED_PATH, NOTIFIED_PATH.stat().st_size)

def prune_notified(store: Dict[str, Any], keep_date: str) -> Dict[str, Any]:
    pruned = {k: v for k, v in store.items() if keep_date in k}
    if len(pruned) != len(store):
        logging.info("notified pruned: %d -> %d", len(store), len(pruned))
    return pruned

def within_window(start_iso: str) -> bool:
    """start_iso の [ -WINDOW_BEFORE_MIN, +WINDOW_AFTER_MIN ] に now が入っているか"""
    t = datetime.fromisoformat(start_iso)
    now = now_jst()
    return (t - timedelta(minutes=WINDOW_BEFORE_MIN)) <= now <= (t + timedelta(minutes=WINDOW_AFTER_MIN))


# ====== LINE 送信 ======
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

    # 複数宛（シート） or 単一宛（環境変数）
    if SHEET_AVAILABLE:
        try:
            recs = fetch_recipients()  # [{"userId": "...", "enabled": True}, ...]
            active = [r for r in recs if r.get("enabled") and r.get("userId")]
        except Exception as e:
            logging.warning("fetch_recipients failed: %s", e)
            active = []
        if active:
            for r in active:
                _push_line(r["userId"], text)
                time.sleep(0.15)
            logging.info("LINE送信OK（複数宛 %d件）", len(active))
            return

    if LINE_USER_ID:
        _push_line(LINE_USER_ID, text)
        logging.info("LINE送信OK（単一宛）")


# ====== 戦略ロジック ======
def _favorites(horses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """人気順（pop=1,2,3,4...）の配列に整える"""
    hs = sorted(horses, key=lambda x: x["odds"])
    for i, h in enumerate(hs, 1):
        h["pop"] = i
    return hs

def _pick_opponents_10_20(horses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """2番人気以下から 10〜20倍（両端含む）を最大4頭"""
    cands = [h for h in horses if h["pop"] >= 2 and 10.0 <= h["odds"] <= 20.0]
    # 倍率の低い順に最大4頭
    cands.sort(key=lambda x: x["odds"])
    return cands[:4]

def _fmt_header(r: Dict[str, Any], strategy_title: str) -> str:
    dt = datetime.fromisoformat(r["start_at_iso"]).astimezone(JST)
    t = dt.strftime("%H:%M")
    return f"【戦略アラート】{strategy_title}\n{r['venue']} {r['race_no']} 発走 {t}"

def _fmt_favs(hs: List[Dict[str, Any]], upto: int = 3) -> str:
    lines = []
    for h in hs[:upto]:
        lines.append(f"{h['pop']}. 馬番{h['umaban']}（{h['odds']:.1f}倍）")
    return "◎ 候補\n" + "\n".join(lines)

def _bets_box(ums: List[int]) -> List[Tuple[int, int, int]]:
    """三連単BOX（3頭）= 6点"""
    a, b, c = ums
    return [
        (a, b, c), (a, c, b),
        (b, a, c), (b, c, a),
        (c, a, b), (c, b, a),
    ]

def _bets_first_fixed(first: int, second: int, third: int) -> List[Tuple[int, int, int]]:
    return [(first, second, third)]

def _bets_first_fixed_2pts(first: int, sec: int, thi: int) -> List[Tuple[int, int, int]]:
    return [(first, sec, thi), (first, thi, sec)]

def _bets_first_fixed_flow(first: int, opps: List[int]) -> List[Tuple[int, int, int]]:
    """1→相手→相手（相手は重複不可）。opps は 2〜4頭"""
    out = []
    for i in range(len(opps)):
        for j in range(len(opps)):
            if i == j:
                continue
            out.append((first, opps[i], opps[j]))
    return out

def build_messages(r: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    horses: [{umaban, odds, pop}]
    戦略①〜④の一致を調べ、送信用メッセージを返す。
    """
    hs = _favorites(r["horses"])
    if len(hs) < 3:
        return []

    p1, p2, p3 = hs[0], hs[1], hs[2]
    p4 = hs[3] if len(hs) >= 4 else None

    msgs: List[Dict[str, Any]] = []

    # ① 1〜3番人気 三連単BOX
    # 1番人気 2.0〜10.0、2〜3番人気 <10.0、4番人気 >=15.0
    if (2.0 <= p1["odds"] <= 10.0) and (p2["odds"] < 10.0) and (p3["odds"] < 10.0) and (p4 and p4["odds"] >= 15.0):
        header = _fmt_header(r, "戦略①：1〜3番人気 三連単BOX（6点）")
        body = _fmt_favs(hs, 3)
        bets = _bets_box([p1["umaban"], p2["umaban"], p3["umaban"]])
        bet_txt = "【買い目6点】\n" + "\n".join([f"{a}-{b}-{c}" for a, b, c in bets])
        msgs.append({"key": "S1", "text": f"{header}\n\n{body}\n\n{bet_txt}"})

    # ② 1番人気1着固定 × 2・3番人気（三連単 2点）
    # 1番 <2.0、2〜3番 <10.0
    if (p1["odds"] < 2.0) and (p2["odds"] < 10.0) and (p3["odds"] < 10.0):
        header = _fmt_header(r, "戦略②：1着固定×2・3番人気（三連単 2点）")
        body = _fmt_favs(hs, 3)
        bets = _bets_first_fixed_2pts(p1["umaban"], p2["umaban"], p3["umaban"])
        bet_txt = "【買い目2点】\n" + "\n".join([f"{a}-{b}-{c}" for a, b, c in bets])
        msgs.append({"key": "S2", "text": f"{header}\n\n{body}\n\n{bet_txt}"})

    # ③ 1着固定 × 10〜20倍流し（1→相手4頭→相手4頭）
    # 1番 ≤1.5、相手: 2番人気以下で10〜20倍から最大4頭（未満ならあるだけ）
    if p1["odds"] <= 1.5:
        opps = _pick_opponents_10_20(hs)
        if opps:
            header = _fmt_header(r, "戦略③：1着固定×10〜20倍流し")
            # ③は1番人気のみ表示仕様
            body = "◎ 1番人気\n" + f"1. 馬番{p1['umaban']}（{p1['odds']:.1f}倍）"
            opp_nums = [o["umaban"] for o in opps]
            bets = _bets_first_fixed_flow(p1["umaban"], opp_nums)
            bet_txt = f"【買い目{len(bets)}点】\n" + "\n".join([f"{a}-{b}-{c}" for a, b, c in bets])
            msgs.append({"key": "S3", "text": f"{header}\n\n{body}\n\n相手（{len(opps)}頭）: " +
                        ", ".join([f"馬番{n}" for n in opp_nums]) + f"\n\n{bet_txt}"})

    # ④ 3着固定（3番人気固定）三連単（2点）
    # 1・2番 ≤3.0、3番 6.0〜10.0、4番 ≥15.0
    if (p1["odds"] <= 3.0) and (p2["odds"] <= 3.0) and (6.0 <= p3["odds"] <= 10.0) and (p4 and p4["odds"] >= 15.0):
        header = _fmt_header(r, "戦略④：3着固定（3番人気固定）三連単 2点")
        body = _fmt_favs(hs, 3)
        bets = [
            (p1["umaban"], p2["umaban"], p3["umaban"]),
            (p2["umaban"], p1["umaban"], p3["umaban"]),
        ]
        bet_txt = "【買い目2点】\n" + "\n".join([f"{a}-{b}-{c}" for a, b, c in bets])
        msgs.append({"key": "S4", "text": f"{header}\n\n{body}\n\n{bet_txt}"})

    return msgs


# ====== メイン処理 ======
def main():
    logging.info("ジョブ開始 host=%s pid=%s", socket.gethostname(), os.getpid())
    logging.info("NOTIFIED_PATH=%s KILL_SWITCH=%s DRY_RUN=%s SHEET=%s TEST_HOOK=%s",
                 NOTIFIED_PATH, KILL_SWITCH, DRY_RUN, SHEET_AVAILABLE, os.getenv("TEST_HOOK","False"))

    notified = prune_notified(_load_notified(), keep_date=now_jst().strftime("%Y-%m-%d"))

    race_ids = list_today_raceids()
    hits = 0

    for rid in race_ids:
        # オッズ取得
        data = fetch_tanfuku_odds(rid)
        if not data or not data.get("horses"):
            continue

        # 正しい発走時刻に置換（取得できない場合は odds_client 側で例外）
        try:
            data["start_at_iso"] = get_race_start_iso(rid)
        except Exception as e:
            logging.warning("発走時刻取得失敗 rid=%s: %s", rid, e)
            # 失敗時はスキップ（通知窓が判断できないため）
            continue

        if not within_window(data["start_at_iso"]):
            logging.info("時間窓外のためスキップ: %s %s", rid, data["start_at_iso"])
            continue

        # 戦略判定
        messages = build_messages(data)
        if not messages:
            continue

        # 重複回避 & 送信
        for m in messages:
            key = f"{rid}|{m['key']}|{data['start_at_iso'][:10]}"
            if notified.get(key):
                continue
            send_line_message(m["text"])
            notified[key] = int(time.time())
            hits += 1

    logging.info("HITS=%s", hits)
    _save_notified(notified)
    logging.info("ジョブ終了")


if __name__ == "__main__":
    main()