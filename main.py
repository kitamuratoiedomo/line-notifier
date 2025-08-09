# main.py
import os
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta, timezone

from odds_client import (
    fetch_tanfuku_odds,   # 単勝オッズ {horses:[{umaban,odds,pop}], venue, race_no, start_at_iso, ...}
    list_today_raceids,   # 監視レースID列挙（env RACEIDS 推奨）
)
from jockey_rank import get_jockey_map
from notify_line import send_line

JST = timezone(timedelta(hours=9))

# ===== env =====
NOTIFIED_PATH = os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json")
WINDOW_BEFORE_MIN = int(os.getenv("WINDOW_BEFORE_MIN", "5"))  # 販売締切5分前“想定”
WINDOW_AFTER_MIN  = int(os.getenv("WINDOW_AFTER_MIN", "0"))
KILL_SWITCH = os.getenv("KILL_SWITCH", "False").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"
TEST_HOOK = os.getenv("TEST_HOOK", "0") in ("1", "true", "True")
SHEET_ENABLED = os.getenv("SHEET", "True").lower() == "true"
LINE_FALLBACK_USER = os.getenv("LINE_USER_ID", "").strip()

# ===== logging =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ===== util =====
def _now() -> datetime:
    return datetime.now(JST)

def load_notified() -> Dict[str, str]:
    p = Path(NOTIFIED_PATH)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}

def save_notified(d: Dict[str, str]) -> None:
    p = Path(NOTIFIED_PATH)
    p.write_text(json.dumps(d, ensure_ascii=False))

def within_window(sale_close_iso: str) -> bool:
    """
    「販売締切（想定）」のISO時刻を基準に、[ -WINDOW_BEFORE, +WINDOW_AFTER ] に
    現在時刻が入っているかどうか。
    取得できない場合は False。
    """
    try:
        tgt = datetime.fromisoformat(sale_close_iso)
    except Exception:
        return False
    now = _now()
    return (tgt - timedelta(minutes=WINDOW_BEFORE_MIN)) <= now <= (tgt + timedelta(minutes=WINDOW_AFTER_MIN))

# ===== strategy judgement =====
def _sort_horses(horses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    hs = [dict(h) for h in horses]
    hs.sort(key=lambda x: x["odds"])
    for i, h in enumerate(hs, 1):
        h["pop"] = i
    return hs

def judge_strategies(horses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    4戦略の一致を返す。
    return: [{code:'S1'|'S2'|'S3'|'S4', picks: {...}} ...]
    """
    h = _sort_horses(horses)
    if len(h) < 3:
        return []

    res = []

    # ① 1〜3人気 三連単BOX（6点）
    # 1番人気:2.0〜10.0, 2〜3番人気<10.0, 4番人気>=15.0
    if (2.0 <= h[0]["odds"] <= 10.0) and (h[1]["odds"] < 10.0) and (h[2]["odds"] < 10.0) and (len(h) >= 4 and h[3]["odds"] >= 15.0):
        res.append({"code": "S1", "picks": {"box": [h[0], h[1], h[2]]}})

    # ② 1番人気1着固定 × 2・3番人気（三連単2点）
    # 1番人気 < 2.0, 2〜3番人気 < 10.0
    if (h[0]["odds"] < 2.0) and (h[1]["odds"] < 10.0) and (h[2]["odds"] < 10.0):
        res.append({"code": "S2", "picks": {"first": h[0], "pair": [h[1], h[2]]}})

    # ③ 1着固定 × 10〜20倍流し（相手4頭）
    # 1番人気 <= 1.5, 相手: 2番人気以下で 10〜20 の馬 から最大4頭
    cand = [x for x in h[1:] if 10.0 <= x["odds"] <= 20.0]
    if h[0]["odds"] <= 1.5 and cand:
        res.append({"code": "S3", "picks": {"first": h[0], "targets": cand[:4]}})

    # ④ 3着固定（3番人気固定）三連単2点
    # 1・2人気 <= 3.0, 3番人気 6.0〜10.0, 4番人気 >= 15.0
    if (h[0]["odds"] <= 3.0) and (h[1]["odds"] <= 3.0) and (6.0 <= h[2]["odds"] <= 10.0) and (len(h) >= 4 and h[3]["odds"] >= 15.0):
        res.append({"code": "S4", "picks": {"third_fixed": h[2], "pair12": [h[0], h[1]]}})

    return res

# ===== format =====
def _fmt_odds_tag(h: Dict[str, Any]) -> str:
    return f"#{h['pop']} 単勝:{h['odds']:.1f}"

def _fmt_jockey(h: Dict[str, Any]) -> str:
    jn = h.get("jockey") or "—"
    rk = h.get("jrank") or "-"
    return f"{jn} / Rk:{rk}"

def format_message(hit: Dict[str, Any], meta: Dict[str, str], jockey_policy: str) -> str:
    """
    jockey_policy: 'top3'|'top1'
    """
    venue = meta.get("venue", "")
    race_no = meta.get("race_no", "")
    sale_close = meta.get("sale_close_hhmm", "")
    rid = meta.get("race_id", "")

    lines = []
    title = ""
    if hit["code"] == "S1":
        title = "【戦略一致】① 1〜3番人気 三連単BOX（6点）"
        picks = hit["picks"]["box"]
        lines += [f"設定方法  {race_no}  {venue}（販売締切 {sale_close}）", ""]
        # 騎手は上位3人気
        if jockey_policy == "top3":
            for i, ph in enumerate(picks, 1):
                lines.append(f"{i}. {_fmt_odds_tag(ph)}  { _fmt_jockey(ph)}")
    elif hit["code"] == "S2":
        title = "【戦略一致】② 1番人気1着固定 × 2・3番人気（三連単2点）"
        f1 = hit["picks"]["first"]
        a  = hit["picks"]["pair"]
        lines += [f"設定方法  {race_no}  {venue}（販売締切 {sale_close}）", ""]
        lines.append(f"1着固定：{_fmt_odds_tag(f1)}  {_fmt_jockey(f1)}")
        for i, ph in enumerate(a, 1):
            lines.append(f"相手{i}：{_fmt_odds_tag(ph)}  {_fmt_jockey(ph)}")
    elif hit["code"] == "S3":
        title = "【戦略一致】③ 1→相手4頭→相手4頭（三連単）"
        f1 = hit["picks"]["first"]
        ts = hit["picks"]["targets"]
        lines += [f"設定方法  {race_no}  {venue}（販売締切 {sale_close}）", ""]
        lines.append(f"◎ 1頭固定：{_fmt_odds_tag(f1)}  {_fmt_jockey(f1)}")
        lines.append("○ 候補（最大4頭）")
        for i, ph in enumerate(ts, 1):
            lines.append(f"{i}. {_fmt_odds_tag(ph)}")
    elif hit["code"] == "S4":
        title = "【戦略一致】④ 3着固定（1番人気×2番人気）-3番人気（三連単2点）"
        a, b = hit["picks"]["pair12"]
        c = hit["picks"]["third_fixed"]
        lines += [f"設定方法  {race_no}  {venue}（販売締切 {sale_close}）", ""]
        # 騎手は上位3人気
        lines.append(f"1番人気：{_fmt_odds_tag(a)}  {_fmt_jockey(a)}")
        lines.append(f"2番人気：{_fmt_odds_tag(b)}  {_fmt_jockey(b)}")
        lines.append(f"3着固定：{_fmt_odds_tag(c)}  {_fmt_jockey(c)}")
    else:
        title = "【戦略】"
    body = "\n".join(lines)
    note = "※この通知は『販売締切（発走5分前想定）5分前』基準です。"
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"
    return f"{title}\n{body}\n{note}\n{url}"

# ===== recipients =====
def fetch_recipients() -> List[str]:
    """
    スプレッドシートから LINE userId の配列を返す。
    取得失敗時は環境変数 LINE_USER_ID（単一宛）にフォールバック。
    """
    try:
        from sheets_client import fetch_recipients as _fr  # 既存の関数名想定
        ids = _fr()
        ids = [x for x in ids if x]
        if ids:
            return ids
    except Exception as e:
        logging.warning("fetch_recipients failed: %s", e)
    if LINE_FALLBACK_USER:
        logging.info("フォールバックで単一宛: %s", LINE_FALLBACK_USER[:6] + "…")
        return [LINE_FALLBACK_USER]
    return []

# ===== main flow =====
def enrich_with_jockey(horses: List[Dict[str, Any]], jmap: Dict[int, Tuple[str, str]]) -> None:
    for h in horses:
        umaban = h.get("umaban")
        if umaban in jmap:
            jn, rk = jmap[umaban]
            h["jockey"] = jn
            h["jrank"] = rk
        else:
            h["jockey"] = "—"
            h["jrank"] = "-"

def main():
    logging.info("ジョブ開始 host=%s pid=%s", os.getenv("RENDER_INSTANCE_ID","local"), os.getpid())
    logging.info("NOTIFIED_PATH=%s KILL_SWITCH=%s DRY_RUN=%s SHEET=%s TEST_HOOK=%s",
                 NOTIFIED_PATH, KILL_SWITCH, DRY_RUN, SHEET_ENABLED, TEST_HOOK)

    if KILL_SWITCH:
        logging.info("KILL_SWITCH=True のため終了")
        return

    race_ids = list_today_raceids()
    if not race_ids:
        logging.info("RACEIDS が未設定のため、レース列挙をスキップ")
        return

    notified = load_notified()
    recipients = fetch_recipients()

    for rid in race_ids:
        info = fetch_tanfuku_odds(rid)
        if not info or not info.get("horses"):
            logging.warning("オッズ取得失敗/空 rid=%s", rid)
            continue

        # 騎手取得（常に試みる）
        jmap = get_jockey_map(rid)
        enrich_with_jockey(info["horses"], jmap)

        # 販売締切（想定）時刻（発走時刻しか取れない場合は発走-5分を仮の販売締切とする）
        sale_close_iso = None
        try:
            # 既存 odds_client に sale-close 取得があれば利用（ない場合 except）
            from odds_client import get_sale_close_iso  # type: ignore
            sale_close_iso = get_sale_close_iso(rid)
        except Exception:
            pass
        if not sale_close_iso:
            try:
                start_iso = info.get("start_at_iso")
                if start_iso:
                    sale_close_iso = (datetime.fromisoformat(start_iso) - timedelta(minutes=5)).isoformat()
            except Exception:
                sale_close_iso = None

        if not sale_close_iso or not within_window(sale_close_iso):
            logging.info("時間窓外のためスキップ: %s %s", rid, sale_close_iso or "N/A")
            continue

        # 戦略判定
        hits = judge_strategies(info["horses"])
        if not hits:
            logging.info("戦略一致なし（通知なし）")
            continue

        # 二重送信防止キー：rid + sale_close 'YYYY-MM-DDTHH:MM'
        key_base = f"{rid}:{(sale_close_iso or '')[:16]}"
        sent_any = False

        for hit in hits:
            k = f"{key_base}:{hit['code']}"
            if notified.get(k):
                continue

            meta = {
                "venue": info.get("venue",""),
                "race_no": info.get("race_no",""),
                "race_id": rid,
                "sale_close_hhmm": (sale_close_iso or "")[11:16],
            }
            # 騎手表示ポリシー
            policy = "top3" if hit["code"] in ("S1","S4") else ("top1" if hit["code"]=="S3" else "top3")
            msg = format_message(hit, meta, policy)

            if DRY_RUN or not recipients:
                logging.info("[DRY] would send to %d users: %s", len(recipients), hit["code"])
            else:
                try:
                    send_line(msg, recipients)
                    sent_any = True
                    logging.info("LINE送信OK（%d宛）", len(recipients))
                except Exception as e:
                    logging.warning("LINE送信失敗: %s", e)
                    continue

            notified[k] = _now().isoformat()

        if sent_any:
            save_notified(notified)

    logging.info("ジョブ終了")

if __name__ == "__main__":
    main()