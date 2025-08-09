# main.py
import os
import json
import time
import logging
import socket
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone

import requests

from odds_client import list_today_raceids, fetch_tanfuku_odds, get_race_start_iso

# ==== 既存モジュール（あなたのリポジトリにある想定） ====
# - odds_client: レース一覧、単複オッズ、発走時刻ISO
# - sheets_client: 受信者スプレッドシート
# - jockey_rank: 騎手ランク（A/B/C）
ODDS_AVAILABLE = True
SHEET_AVAILABLE = True

try:
    from odds_client import (
        list_today_raceids,         # -> List[str]
        fetch_tanfuku_odds,         # (race_id) -> Dict[str, Any]
        get_race_start_iso,         # (race_id) -> str (ISO, JST)
        get_race_title,             # (race_id) -> str  例: "佐賀2R"
        get_venue_and_rno,          # (race_id) -> Tuple[str, str] 例: ("佐賀","2R")
    )
except Exception as e:
    logging.warning("odds_client を読み込めませんでした（本番機能はスキップ）: %s", e)
    ODDS_AVAILABLE = False

try:
    from sheets_client import fetch_recipients   # -> List[Dict] {userId, enabled}
except Exception as e:
    logging.warning("sheets_client を読み込めませんでした（単一宛にフォールバック）: %s", e)
    SHEET_AVAILABLE = False

try:
    from jockey_rank import get_jrank            # (jockey_name) -> "A"|"B"|"C"
except Exception:
    # 最悪ランクが取れなくても落とさない
    def get_jrank(_name: str) -> str:
        return "C"


# ====== 設定 ======
# データ保持
NOTIFIED_PATH = Path(os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json"))
NOTIFIED_PATH.parent.mkdir(parents=True, exist_ok=True)

# LINE
LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

# スイッチ類
KILL_SWITCH = os.getenv("NOTIFY_ENABLED", "1") != "1"
DRY_RUN     = os.getenv("DRY_RUN_MESSAGE", "0") == "1"
TEST_HOOK   = os.getenv("TEST_HOOK", "0") == "1"   # テスト用ダミーヒットを流す

# 時間窓（“ネット締切5分前”補正として、発走10分前のみ通知）
WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "10"))  # ←指示どおり既定値10
WINDOW_AFTER_MIN  = int(os.getenv("WINDOW_AFTER_MIN",  "0"))   # ←指示どおり既定値0

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
    start_iso: "YYYY-MM-DDTHH:MM:SS+09:00" か "HH:MM"（当日扱い）
    通知許可: 発走10分前〜発走ちょうど（after=0）
    """
    try:
        t = datetime.fromisoformat(start_iso)
    except Exception:
        base = now_jst().strftime("%Y-%m-%d")
        t = datetime.fromisoformat(f"{base}T{start_iso}:00+09:00")
    now = now_jst()
    return (t - timedelta(minutes=WINDOW_BEFORE_MIN)) <= now <= (t + timedelta(minutes=WINDOW_AFTER_MIN))

def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s or s in {"-", "—", "―"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


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
        logging.warning("LINE送信例外: %s", e)

def send_line_message(text: str) -> None:
    if KILL_SWITCH:
        logging.info("KILL_SWITCH有効のため送信しません。")
        return
    if DRY_RUN:
        logging.info("[DRY RUN] %s", text.replace("\n", " "))
        return

    # シートが読めれば複数宛、ダメなら単一宛先
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
                time.sleep(0.15)  # rate-limit対策
            logging.info("LINE送信OK（複数宛 %d件）", len(active))
            return

    # フォールバック：単一宛
    if LINE_USER_ID:
        _push_line(LINE_USER_ID, text)
        logging.info("LINE送信OK（単一宛）")
    else:
        logging.warning("宛先がありません（SHEETもLINE_USER_IDもなし）")


# ============ オッズ→人気配列の整形 ============
def _pick_top_by_odds(odds_rows: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
    """
    odds_rows: odds_client.fetch_tanfuku_odds() が返す「出走馬行」の配列を想定
      例 要素: {"horse_no": "3", "horse_name": "...", "jockey": "○○", "odds": "2.4"}
    人気は「複勝ではなく“単勝オッズの小さい順”」で並べ替える想定（tanfukuエンドポイントの単勝を使う）
    """
    rows = []
    for r in odds_rows:
        o = _to_float(r.get("odds"))
        if o is None:
            continue
        rows.append({
            "horse_no": str(r.get("horse_no") or ""),
            "horse_name": str(r.get("horse_name") or ""),
            "jockey": str(r.get("jockey") or ""),
            "odds": o,
        })
    rows.sort(key=lambda x: x["odds"])
    if limit:
        rows = rows[:limit]
    # rank付与
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    return rows


# ============ 戦略判定 ============

def _fmt_jrank(name: str) -> str:
    rank = get_jrank(name or "")
    return f"{name}（{rank}）" if name else name

def _fmt_msg_header(race_id: str) -> str:
    title = ""
    venue = ""
    rno   = ""
    try:
        title = get_race_title(race_id)  # 例: "佐賀 2R"
    except Exception:
        try:
            venue, rno = get_venue_and_rno(race_id)
            title = f"{venue} {rno}"
        except Exception:
            title = race_id
    return title or race_id

def judge_strategy(top: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    top: 人気順（rank, odds, jockey, horse_no, horse_name） 1位〜
    返り値: { "strategy": "① …", "picks": {...}, "msg_lines": [..] } or None
    """
    if len(top) < 3:
        return None

    # 便宜上、p1..p5 を用意
    p1, p2, p3 = top[0], top[1], top[2]
    p4 = top[3] if len(top) >= 4 else None
    # p5 は戦略③で候補探索に使う可能性があるので top 全体を使う

    # ----------------
    # ① 1〜3番人気 三連単BOX
    # 条件:
    #   1番人気オッズ: 2.0〜10.0
    #   2〜3番人気オッズ: 10.0未満
    #   4番人気: 15.0以上
    if p4:
        if (2.0 <= p1["odds"] <= 10.0) and (p2["odds"] < 10.0) and (p3["odds"] < 10.0) and (p4["odds"] >= 15.0):
            lines = [
                "【戦略①】1〜3番人気 三連単BOX（6点）",
                f"人気1: {p1['horse_no']} {p1['horse_name']}  単勝{p1['odds']}",
                f"人気2: {p2['horse_no']} {p2['horse_name']}  単勝{p2['odds']}",
                f"人気3: {p3['horse_no']} {p3['horse_name']}  単勝{p3['odds']}",
                "買い目：1-2-3 BOX（6点）",
                "騎手ランク：",
                f"  1番人気: {_fmt_jrank(p1['jockey'])}",
                f"  2番人気: {_fmt_jrank(p2['jockey'])}",
                f"  3番人気: {_fmt_jrank(p3['jockey'])}",
            ]
            return {"code": "①", "msg_lines": lines}

    # ----------------
    # ② 1番人気1着固定 × 2・3番人気（三連単）2点
    # 条件:
    #   1番人気オッズ: 2.0未満
    #   2〜3番人気: 10.0未満
    if (p1["odds"] < 2.0) and (p2["odds"] < 10.0) and (p3["odds"] < 10.0):
        lines = [
            "【戦略②】1着固定（1番人気）→ 2・3番人気（三連単 2点）",
            f"人気1: {p1['horse_no']} {p1['horse_name']}  単勝{p1['odds']}",
            f"人気2: {p2['horse_no']} {p2['horse_name']}  単勝{p2['odds']}",
            f"人気3: {p3['horse_no']} {p3['horse_name']}  単勝{p3['odds']}",
            "買い目：1→(2,3)→(2,3)（2点）",
            "騎手ランク：",
            f"  1番人気: {_fmt_jrank(p1['jockey'])}",
            f"  2番人気: {_fmt_jrank(p2['jockey'])}",
            f"  3番人気: {_fmt_jrank(p3['jockey'])}",
        ]
        return {"code": "②", "msg_lines": lines}

    # ----------------
    # ③ 1着固定 × 10〜20倍流し（1→相手4頭→相手4頭）
    # 条件:
    #   1番人気オッズ: 1.5以下
    #   相手：2番人気以下で “単勝10〜20倍” の馬4頭（満たなければ、いるだけ）
    if p1["odds"] <= 1.5:
        # 2番人気以下から10〜20倍を拾う（最大4頭）
        cands = [r for r in top[1:] if 10.0 <= r["odds"] <= 20.0]
        cands = cands[:4]
        if cands:
            # 表示は候補数だけ
            cand_lines = [f"  相手{i+1}: {r['horse_no']} {r['horse_name']}  単勝{r['odds']}" for i, r in enumerate(cands)]
            lines = [
                "【戦略③】1着固定 × 10〜20倍流し（1→相手4頭→相手4頭）",
                f"人気1: {p1['horse_no']} {p1['horse_name']}  単勝{p1['odds']}",
                "相手候補（10〜20倍・最大4頭）:",
                *cand_lines,
                "買い目：1→(相手)→(相手)",
                "騎手ランク：",
                f"  1番人気: {_fmt_jrank(p1['jockey'])}",
            ]
            return {"code": "③", "msg_lines": lines}

    # ----------------
    # ④ 3着固定（3番人気固定）三連単（2点）
    # 条件:
    #   1・2番人気: 3.0以下
    #   3番人気: 6.0〜10.0
    #   4番人気: 15.0以上
    if p4:
        if (p1["odds"] <= 3.0) and (p2["odds"] <= 3.0) and (6.0 <= p3["odds"] <= 10.0) and (p4["odds"] >= 15.0):
            lines = [
                "【戦略④】3着固定（3番人気固定）三連単（2点）",
                f"人気1: {p1['horse_no']} {p1['horse_name']}  単勝{p1['odds']}",
                f"人気2: {p2['horse_no']} {p2['horse_name']}  単勝{p2['odds']}",
                f"人気3: {p3['horse_no']} {p3['horse_name']}  単勝{p3['odds']}",
                f"人気4: {p4['horse_no']} {p4['horse_name']}  単勝{p4['odds']}",
                "買い目：(1,2)→(1,2)→3（2点）",
                "騎手ランク：",
                f"  1番人気: {_fmt_jrank(p1['jockey'])}",
                f"  2番人気: {_fmt_jrank(p2['jockey'])}",
                f"  3番人気: {_fmt_jrank(p3['jockey'])}",
            ]
            return {"code": "④", "msg_lines": lines}

    return None


def build_message(race_id: str, start_iso: str, judged: Dict[str, Any]) -> str:
    header = _fmt_msg_header(race_id)
    dt = ""
    try:
        dt = datetime.fromisoformat(start_iso).strftime("%-m/%-d %H:%M")
    except Exception:
        dt = start_iso
    lines = [
        f"{judged['code']} 判定",
        f"発走: {dt}",
        *judged["msg_lines"],
        "",
        "※この通知は発走10分前基準です（ネット締切5分前相当）。",
    ]
    return "\n".join(lines)


# ============ メインの抽出 ============

def find_strategy_matches() -> List[Dict[str, Any]]:
    """
    1) 当日の対象レースIDを取得
    2) 各レースの単勝オッズを取得し人気順top10を作成
    3) 条件に一致すれば通知ペイロードを返す
    """
    results: List[Dict[str, Any]] = []

    # テストフック（動作確認用）
    if TEST_HOOK:
        start_iso = (now_jst() + timedelta(minutes=11)).isoformat()  # window=10分なので直後にかかる
        dummy = {
            "race_id": "TEST-ISO",
            "start_at_iso": start_iso,
            "payload": {
                "strategy": "TEST",
                "message": "（テスト通知）",
            },
        }
        # ダミーも judge 風にフォーマットして送る
        judged = {"code": "テスト", "msg_lines": ["テストメッセージです。"]}
        msg = build_message(dummy["race_id"], start_iso, judged)
        results.append({"race_id": dummy["race_id"], "start_at_iso": start_iso, "message": msg, "strategy": "テスト"})
        return results

    if not ODDS_AVAILABLE:
        return results

    try:
        race_ids = list_today_raceids()
    except Exception as e:
        logging.warning("list_today_raceids で失敗: %s", e)
        return results

    for rid in race_ids:
        try:
            odds_data = fetch_tanfuku_odds(rid)
            start_iso = get_race_start_iso(rid)
        except Exception as e:
            logging.warning("odds/start取得失敗 rid=%s err=%s", rid, e)
            continue

        # odds_data は { "rows": [ {horse_no, horse_name, jockey, odds}, ... ] } を想定
        rows = odds_data.get("rows") if isinstance(odds_data, dict) else None
        if not rows:
            continue

        top = _pick_top_by_odds(rows, limit=10)
        if len(top) < 3:
            continue

        judged = judge_strategy(top)
        if not judged:
            continue

        msg = build_message(rid, start_iso, judged)
        results.append({
            "race_id": rid,
            "start_at_iso": start_iso,
            "message": msg,
            "strategy": judged["code"],
        })

    return results


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
        rid = h.get("race_id", "")
        strategy = h.get("strategy", "")
        start_iso = h.get("start_at_iso", "")

        # 時間窓チェック（発走10分前のみ）
        if start_iso and not within_window(start_iso):
            logging.info("時間窓外のためスキップ: %s %s", rid, start_iso)
            continue

        # デデュープキー（日付込み）
        key_date = start_iso[:10] if start_iso else today
        key = f"{rid}|{strategy}|{key_date}"
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