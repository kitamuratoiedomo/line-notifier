# main.py
import os
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

from odds_client import (
    fetch_tanfuku_odds,
    list_today_raceids,
    get_race_start_iso,
    get_sale_close_iso,
)

from notify_line import send_line  # 既存の LINE 送信ユーティリティを想定

# 騎手ランク（存在しなければ C 固定）
try:
    from jockey_rank import get_jockey_rank  # type: ignore
except Exception:
    def get_jockey_rank(_rid: str, _umaban: int) -> str:  # ダミー
        return "C"

# ------------------------------
# 環境
# ------------------------------
JST = timezone(timedelta(hours=9))

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "")
LINE_USER_ID = os.getenv("LINE_USER_ID", "")

NOTIFIED_PATH = Path(os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json"))
NOTIFY_ENABLED = os.getenv("NOTIFY_ENABLED", "1") == "1"
KILL_SWITCH = os.getenv("KILL_SWITCH", "False") == "True"

# ウィンドウ: 販売締切（＝発走5分前）基準の「さらに何分前で通知するか」
# 例) 5 -> 発走10分前に通知
CUTOFF_OFFSET_MIN = int(os.getenv("CUTOFF_OFFSET_MIN", "5"))

WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "5"))  # 通知許容の前側余裕
WINDOW_AFTER_MIN = int(os.getenv("WINDOW_AFTER_MIN", "0"))    # 後側余裕（基本0で良い）

# ログ
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ------------------------------
# 永続データ（重複送信防止）
# ------------------------------
def load_notified() -> Dict[str, Any]:
    if NOTIFIED_PATH.exists():
        try:
            return json.loads(NOTIFIED_PATH.read_text())
        except Exception:
            pass
    return {"sent": []}


def save_notified(data: Dict[str, Any]) -> None:
    NOTIFIED_PATH.parent.mkdir(parents=True, exist_ok=True)
    NOTIFIED_PATH.write_text(json.dumps(data, ensure_ascii=False))


def was_already_sent(key: str) -> bool:
    data = load_notified()
    return key in data.get("sent", [])


def mark_sent(key: str) -> None:
    data = load_notified()
    sent = set(data.get("sent", []))
    sent.add(key)
    data["sent"] = list(sent)
    save_notified(data)


# ------------------------------
# 戦略ロジック
# ------------------------------
def fmt_top(h: Dict[str, Any]) -> str:
    return f"#{h['pop']} 単勝:{h['odds']:.1f}"

def select_top(horses: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    return horses[:min(n, len(horses))]

def find_by_pop(horses: List[Dict[str, Any]], pop: int) -> Optional[Dict[str, Any]]:
    for h in horses:
        if h["pop"] == pop:
            return h
    return None


def match_strategy(odds: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    4パターンのどれかに一致すれば dict を返す
    """
    hs = odds["horses"]
    if len(hs) < 3:
        return None

    p1, p2, p3 = hs[0], hs[1], hs[2]
    p4 = hs[3] if len(hs) >= 4 else None

    # ① 1〜3番人気三連単BOX
    if 2.0 <= p1["odds"] <= 10.0 and p2["odds"] < 10.0 and p3["odds"] < 10.0 and (p4 is None or p4["odds"] >= 15.0):
        return {
            "code": "01",
            "title": "① 1〜3番人気 三連単BOX（6点）",
            "picks": select_top(hs, 3),
            "jockey_mode": "top3",
        }

    # ② 1番人気1着固定 × 2・3番人気（三連単 2点）
    if p1["odds"] < 2.0 and p2["odds"] < 10.0 and p3["odds"] < 10.0:
        return {
            "code": "02",
            "title": "② 1番人気1着固定 × 2・3番人気（三連単 2点）",
            "picks": [p1, p2, p3],
            "jockey_mode": "top3",
        }

    # ③ 1着固定 × 10〜20倍流し（1→相手4頭→相手4頭）
    #   条件: 1番人気 <=1.5、相手=2番人気以下でオッズ10〜20の馬 最大4頭（4頭未満はいるだけ）
    if p1["odds"] <= 1.5:
        cands = [h for h in hs[1:] if 10.0 <= h["odds"] <= 20.0][:4]
        if cands:
            return {
                "code": "03",
                "title": "③ 1→相手4頭→相手4頭（三連単マルチ）",
                "picks": [p1] + cands,
                "jockey_mode": "top1",
            }

    # ④ 3着固定（3番人気固定）三連単 2点
    #   条件: 1,2人気<=3.0、3番人気 6.0〜10.0、4番人気>=15.0
    if p1["odds"] <= 3.0 and p2["odds"] <= 3.0 and 6.0 <= p3["odds"] <= 10.0 and (p4 is None or p4["odds"] >= 15.0):
        return {
            "code": "04",
            "title": "④ 3着固定（1番人気×2番人気）-3番人気（三連単 2点）",
            "picks": [p1, p2, p3],
            "jockey_mode": "top3",
        }

    return None


def jockey_lines(race_id: str, picks: List[Dict[str, Any]], mode: str) -> str:
    if mode == "top1":
        h = picks[0]
        return f"上位人気 騎手ランク: #{h['pop']} 騎手:{get_jockey_rank(race_id, h['umaban'])}"
    # top3
    out = []
    for h in picks[:3]:
        out.append(f"#{h['pop']} 騎手:{get_jockey_rank(race_id, h['umaban'])}")
    return "上位人気 騎手ランク: " + " / ".join(out)


def build_message(odds: Dict[str, Any], match: Dict[str, Any], sale_close_iso: str, notify_base: str) -> str:
    dt_post = datetime.fromisoformat(get_race_start_iso(odds["race_id"]))
    dt_sale = datetime.fromisoformat(sale_close_iso)
    dt_base = datetime.fromisoformat(notify_base)

    venue = odds["venue"]
    rno = odds["race_no"]
    rid = odds["race_id"]

    # 候補（③は最大5頭、他は上位3頭）
    if match["code"] == "03":
        cand_lines = [f"{i+1}. #{h['umaban']} 単勝:{h['odds']:.1f}" for i, h in enumerate(match["picks"][:5])]
    else:
        cand_lines = [f"{i+1}. #{h['umaban']} 単勝:{h['odds']:.1f}" for i, h in enumerate(match["picks"][:3])]

    url = odds["odds_url"]

    if match["code"] == "02":
        header = "【戦略一致】② 1番人気1着固定 × 2・3番人気（三連単 2点）"
    elif match["code"] == "04":
        header = "【戦略一致】④ 3着固定（1番人気×2番人気）-3番人気（三連単 2点）"
    else:
        header = f"【戦略一致】{match['title']}"

    msg = [
        header,
        f"{venue} {rno}（発走 {dt_post.strftime('%H:%M')}）",
        f"※この通知は『販売締切（発走5分前想定）{CUTOFF_OFFSET_MIN}分前』基準です。",
        f"オッズ参照: {url}",
        "",
        "◎ 候補",
        *cand_lines,
        "",
        jockey_lines(odds["race_id"], match["picks"], match["jockey_mode"]),
    ]
    return "\n".join(msg)


# ------------------------------
# 実行本体
# ------------------------------
def main() -> None:
    if KILL_SWITCH:
        logging.info("KILL_SWITCH=True のため中止")
        return

    race_ids = list_today_raceids()
    if not race_ids:
        logging.info("RACEIDS が未設定のため、レース列挙をスキップ")
        return

    hits = 0
    for rid in race_ids:
        try:
            sale_close_iso = get_sale_close_iso(rid)
        except Exception as e:
            logging.warning("発走/販売締切取得失敗 rid=%s err=%s", rid, e)
            continue

        # 通知ターゲット: 販売締切のさらに CUTOFF_OFFSET_MIN 分前
        notify_target = (datetime.fromisoformat(sale_close_iso) - timedelta(minutes=CUTOFF_OFFSET_MIN)).astimezone(JST)

        now = datetime.now(JST)
        if not (notify_target - timedelta(minutes=WINDOW_BEFORE_MIN) <= now <= notify_target + timedelta(minutes=WINDOW_AFTER_MIN)):
            # まだ（または過ぎて）いない
            continue

        # オッズ取得 & 戦略判定
        odds = fetch_tanfuku_odds(rid)
        if not odds:
            continue

        m = match_strategy(odds)
        if not m:
            continue

        # 重複送信防止キー（レースID＋戦略コード）
        dedupe_key = f"{rid}:{m['code']}"
        if was_already_sent(dedupe_key):
            continue

        msg = build_message(odds, m, sale_close_iso, notify_target.isoformat())

        if NOTIFY_ENABLED and LINE_ACCESS_TOKEN and LINE_USER_ID:
            ok = send_line(LINE_ACCESS_TOKEN, LINE_USER_ID, msg)
            if ok:
                mark_sent(dedupe_key)
                hits += 1
        else:
            # ドライランやトークン不備でも重複防止だけは尊重
            logging.info("DRY-RUN/LINE未設定: %s", dedupe_key)
            mark_sent(dedupe_key)
            hits += 1

    logging.info("HITS=%d", hits)


if __name__ == "__main__":
    main()