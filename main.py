# main.py
import os
import re
import json
import time
import logging
import socket
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone

import requests

# 既存ならそのまま使う（無ければ後でフォールバック）
try:
    from jockey_rank import get_jrank  # 任意
except Exception:
    def get_jrank(*args, **kwargs):
        return None

# Rakuten 単勝オッズ取得など（あなたの odds_client.py の関数を利用）
# - fetch_tanfuku_odds(race_id) -> {race_id, venue, race_no, start_at_iso, horses:[{umaban, odds, pop}]}
# - get_sale_close_iso(race_id)  -> 'YYYY-MM-DDTHH:MM:SS+09:00'
try:
    from odds_client import fetch_tanfuku_odds, get_sale_close_iso
except Exception as e:
    fetch_tanfuku_odds = None
    get_sale_close_iso = None
    logging.warning("odds_client を読み込めませんでした（本番機能はスキップ）: %s", e)

# ===== ログ & 共通 =====
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
JST = timezone(timedelta(hours=9))

def now_jst() -> datetime:
    return datetime.now(JST)

# ===== 通知済み管理 =====
NOTIFIED_PATH = Path(os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json"))
NOTIFIED_PATH.parent.mkdir(parents=True, exist_ok=True)

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
        logging.info("notified saved: %s (bytes=%d)", NOTIFIED_PATH, NOTIFIED_PATH.stat().st_size)
    except Exception as e:
        logging.warning("通知済み保存に失敗: %s", e)

def prune_notified(store: Dict[str, Any], keep_date: str) -> Dict[str, Any]:
    pruned = {k: v for k, v in store.items() if keep_date in k}
    if len(pruned) != len(store):
        logging.info("notified pruned: %d -> %d", len(store), len(pruned))
    return pruned

# ===== LINE 送信（安全弁つき） =====
LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

KILL_SWITCH = os.getenv("NOTIFY_ENABLED", "1") != "1"   # 1 以外なら送らない
DRY_RUN     = os.getenv("DRY_RUN_MESSAGE", "0") == "1" # 1 なら送らない（ログだけ）

# HTMLタグ除去の“安全弁”
TAG_RE = re.compile(r"<[^>]+>")
def _strip_html(s: str) -> str:
    if not s:
        return s
    return TAG_RE.sub("", s)

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

# 受信者：Google シート連携（あれば複数宛）／なければ単一宛
SHEET_AVAILABLE = False
try:
    from sheets_client import fetch_recipients
    SHEET_AVAILABLE = True
except Exception:
    SHEET_AVAILABLE = False

def send_line_message(text: str) -> None:
    text = _strip_html(text)  # ★安全弁（タグが混じっても常に除去）

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
            logging.warning("fetch_recipients failed: %s", e)
            recs = []
        active = [r for r in recs if r.get("enabled") and r.get("userId")]
        if active:
            for r in active:
                _push_line(r["userId"], text)
                time.sleep(0.15)
            logging.info("LINE送信OK（複数宛 %d件）", len(active))
            return
        logging.info("シートに有効な受信者なし。単一宛フォールバック。")

    if LINE_USER_ID:
        _push_line(LINE_USER_ID, text)
        logging.info("LINE送信OK（単一宛）")
    else:
        logging.warning("宛先がありません（SHEETもLINE_USER_IDもなし）")

# ===== RACE_ID 自動収集（失敗時は RACEIDS 環境変数）=====
def list_raceids_from_env() -> List[str]:
    env = os.getenv("RACEIDS", "").strip()
    return [x.strip() for x in env.split(",") if x.strip()]

def list_today_raceids_auto() -> List[str]:
    """
    楽天のトップ/当日ページから RACEID=18桁 を拾う簡易版。
    失敗したら [] を返す（呼び出し側でフォールバック）。
    """
    urls = [
        "https://keiba.rakuten.co.jp/",  # トップ
        "https://keiba.rakuten.co.jp/today",  # 当日（存在しなくても無視）
    ]
    rx = re.compile(r"/RACEID/(\d{18})")
    found: List[str] = []
    for url in urls:
        try:
            resp = requests.get(url, timeout=8)
            if resp.status_code != 200:
                continue
            ids = rx.findall(resp.text)
            for rid in ids:
                if rid not in found:
                    found.append(rid)
        except Exception:
            continue
    return found

def resolve_raceids() -> List[str]:
    ids = list_today_raceids_auto()
    if ids:
        return ids
    ids = list_raceids_from_env()
    if ids:
        logging.info("RACEIDS 環境変数から %d 件取得", len(ids))
    else:
        logging.info("RACE_ID を取得できませんでした。処理スキップ。")
    return ids

# ===== 販売締切 5 分前ウィンドウ =====
def _parse_iso(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def within_sale_minus5_window(sale_close_iso: str) -> bool:
    """
    販売締切の 5 分前ちょうど ～ その 1 分後の間に入ったら True
    （cron 1分間隔でちょうど1回ヒットさせる狙い）
    """
    sale = _parse_iso(sale_close_iso)
    if not sale:
        return False
    target = sale - timedelta(minutes=5)
    now = now_jst()
    return target <= now < (target + timedelta(minutes=1))

# ===== 戦略判定（①〜④） =====
def eval_strategies(horses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    horses は odds 昇順・pop 付与済み想定 [{umaban, odds, pop}, ...]
    条件に合った戦略を返す
    """
    res: List[Dict[str, Any]] = []
    if len(horses) < 3:
        return res

    # 上位を取りやすく
    top = sorted(horses, key=lambda x: x["odds"])[:10]  # 10頭あれば十分
    # 便利アクセス
    def get_pop(n: int) -> Optional[Dict[str, Any]]:
        return top[n-1] if 0 < n <= len(top) else None

    h1, h2, h3, h4 = get_pop(1), get_pop(2), get_pop(3), get_pop(4)

    # ① 1〜3番人気BOX
    # 1番 2.0〜10.0 / 2〜3番 <10.0 / 4番 >=15.0
    if h1 and h2 and h3 and h4:
        if (2.0 <= h1["odds"] <= 10.0) and (h2["odds"] < 10.0) and (h3["odds"] < 10.0) and (h4["odds"] >= 15.0):
            res.append({"code": "①", "type": "3連単BOX(1-3人気)", "picks": [h1, h2, h3]})

    # ② 1番人気1着固定 × 2・3番人気
    # 1番 <2.0 / 2〜3番 <10.0
    if h1 and h2 and h3:
        if (h1["odds"] < 2.0) and (h2["odds"] < 10.0) and (h3["odds"] < 10.0):
            res.append({"code": "②", "type": "1着固定(1人気)→2,3人気", "picks": [h1, h2, h3]})

    # ③ 1着固定 × 10〜20倍流し（相手 最大4頭）
    # 1番 <=1.5 / 相手: 2番人気以下で 10〜20
    if h1:
        if h1["odds"] <= 1.5:
            pool = [x for x in top[1:] if 10.0 <= x["odds"] <= 20.0][:4]
            if pool:
                res.append({"code": "③", "type": "1→相手4頭→相手4頭", "picks": [h1] + pool})

    # ④ 3着固定（3番人気固定）
    # 1・2番 <=3.0 / 3番 6.0〜10.0 / 4番 >=15.0
    if h1 and h2 and h3 and h4:
        if (h1["odds"] <= 3.0) and (h2["odds"] <= 3.0) and (6.0 <= h3["odds"] <= 10.0) and (h4["odds"] >= 15.0):
            res.append({"code": "④", "type": "3着固定(3人気)", "picks": [h1, h2, h3]})

    return res

def build_message(rinfo: Dict[str, Any], strat: Dict[str, Any], sale_close_iso: str) -> str:
    venue = rinfo.get("venue") or "—"
    race_no = rinfo.get("race_no") or "—R"
    code = strat["code"]
    stype = strat["type"]
    horses = strat["picks"]

    lines = []
    lines.append(f"【戦略一致】{code} {stype}")
    lines.append(f"{venue} {race_no}")
    lines.append("")
    lines.append("◎ 候補（最大5頭）" if code in ("①", "③") else "◎ 候補")
    # 表示頭数は最大5（③は1+相手最大4）
    show = horses[:5]
    for i, h in enumerate(show, 1):
        rk = get_jrank(h.get("jockey", "")) or "—"  # jockey 名が無ければ "—"
        lines.append(f"{i}. #{h['umaban']} 単勝:{h['odds']:.1f}")
    lines.append("")
    lines.append("※この通知は『販売締切（発走5分前想定）5分前』基準です。")
    # 追跡用URL（任意・プレーンなURLだけ）
    if rinfo.get("race_id"):
        lines.append(f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rinfo['race_id']}")
    return "\n".join(lines)

# ===== メイン =====
def main():
    logging.info("ジョブ開始 host=%s pid=%s", socket.gethostname(), os.getpid())
    logging.info("NOTIFIED_PATH=%s KILL_SWITCH=%s DRY_RUN=%s SHEET=%s TEST_HOOK=%s",
                 NOTIFIED_PATH, KILL_SWITCH, DRY_RUN, SHEET_AVAILABLE, os.getenv("TEST_HOOK","False"))

    notified = prune_notified(_load_notified(), keep_date=now_jst().strftime("%Y-%m-%d"))

    # 1) RACE_ID を集める
    race_ids = resolve_raceids()
    if not race_ids or not fetch_tanfuku_odds:
        logging.info("HITS=0")
        _save_notified(notified)
        return

    hits = 0
    for rid in race_ids:
        # 2) オッズ取得
        info = fetch_tanfuku_odds(rid)
        if not info or not info.get("horses"):
            continue

        # 3) 戦略判定
        matches = eval_strategies(info["horses"])
        if not matches:
            continue

        # 4) 販売締切（なければ発走-5分を仮置き）
        sale_iso = None
        if get_sale_close_iso:
            try:
                sale_iso = get_sale_close_iso(rid)
            except Exception as e:
                logging.warning("販売締切取得失敗 rid=%s err=%s", rid, e)
        if not sale_iso:
            # odds_client.fetch_tanfuku_odds の start_at_iso が入っていれば -5分を仮置き
            base = _parse_iso(info.get("start_at_iso", "")) or (now_jst() + timedelta(minutes=10))
            sale_iso = (base - timedelta(minutes=5)).isoformat()

        # 5) ちょうど「販売締切5分前」に入ったレースだけ通知
        if not within_sale_minus5_window(sale_iso):
            continue

        for m in matches:
            key = f"{rid}|{m['code']}|{sale_iso[:10]}"
            if notified.get(key):
                continue
            msg = build_message(info, m, sale_iso)
            send_line_message(msg)
            notified[key] = int(time.time())
            hits += 1

    logging.info("HITS=%d", hits)
    if hits == 0:
        logging.info("戦略一致なし（通知なし）")
    _save_notified(notified)
    logging.info("ジョブ終了")

if __name__ == "__main__":
    main()