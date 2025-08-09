# main.py  --- 全部差し替え版
import os
import json
import time
import logging
import socket
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

# 既存モジュール（このプロジェクトですでに配置済みの想定）
from odds_client import fetch_tanfuku_odds, list_today_raceids, get_race_start_iso
from jockey_rank import get_jrank

# --- ログ設定 ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
JST = timezone(timedelta(hours=9))


# ========== 環境変数 ==========
NOTIFIED_PATH = Path(os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json"))
# 5分前に通知したい → 「締切＝発走-10分」 を仮定し、そこからさらに CUTOFF_OFFSET_MIN 前で通知
# つまり 実質 発走-(10 + CUTOFF_OFFSET_MIN) 分が通知目標時刻（デフォ 15分前）
CUTOFF_OFFSET_MIN = int(os.getenv("CUTOFF_OFFSET_MIN", "5"))

# 許容ゆらぎ（cronは1分毎なので±1分の幅で“いまが通知すべき時刻”とみなす）
TOLERANCE_MIN = int(os.getenv("TOLERANCE_MIN", "1"))

# LINE
LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID      = os.getenv("LINE_USER_ID", "").strip()

# KILLスイッチ（=0 で送る）
KILL_SWITCH = os.getenv("NOTIFY_ENABLED", "1") != "1"
# ドライラン（本文はログだけ・送信しない）
DRY_RUN = os.getenv("DRY_RUN_MESSAGE", "0") == "1"

# シート複数宛先（任意）
USE_SHEET = os.getenv("USE_SHEET", "1") == "1"
SHEET_AVAILABLE = False
try:
    if USE_SHEET:
        from sheets_client import fetch_recipients
        SHEET_AVAILABLE = True
except Exception as e:
    logging.warning("sheets_client を読み込めませんでした（単一宛にフォールバック）: %s", e)
    SHEET_AVAILABLE = False


# ========== ユーティリティ ==========
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
        NOTIFIED_PATH.parent.mkdir(parents=True, exist_ok=True)
        NOTIFIED_PATH.write_text(json.dumps(obj, ensure_ascii=False))
    except Exception as e:
        logging.warning("通知済み保存に失敗: %s", e)

def prune_notified(store: Dict[str, Any], keep_date: str) -> Dict[str, Any]:
    pruned = {k: v for k, v in store.items() if keep_date in k}
    if len(pruned) != len(store):
        logging.info("notified pruned: %d -> %d", len(store), len(pruned))
    return pruned

def minutes_until(dt: datetime) -> float:
    return (dt - now_jst()).total_seconds() / 60.0

def within_cutoff_window(post_iso: str) -> bool:
    """
    ネット販売締切 = 発走10分前（楽天前提）。
    通知目標 = 締切の CUTOFF_OFFSET_MIN 分前。
    “±TOLERANCE_MIN 分” を通知OKウィンドウとする（cron走査の揺れ吸収）。
    """
    try:
        post = datetime.fromisoformat(post_iso)
    except Exception:
        return False
    cutoff = post - timedelta(minutes=10)
    target = cutoff - timedelta(minutes=CUTOFF_OFFSET_MIN)
    delta_min = abs(minutes_until(target))
    return delta_min <= TOLERANCE_MIN


# ========== 受信者宛先 ==========
def _push_line(user_id: str, text: str) -> None:
    if not LINE_ACCESS_TOKEN or not user_id:
        logging.warning("LINEトークン/宛先なし: 送信スキップ user=%s", user_id)
        return
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {LINE_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": text}]}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        if r.status_code != 200:
            logging.warning("LINE送信失敗 user=%s status=%s body=%s", user_id, r.status_code, r.text)
    except Exception as e:
        logging.warning("LINE送信例外 user=%s err=%s", user_id, e)

def send_line_message(text: str) -> None:
    if KILL_SWITCH:
        logging.info("KILL_SWITCH=ON のため送信停止")
        return
    if DRY_RUN:
        logging.info("[DRY RUN] %s", text.replace("\n", " "))
        return

    if SHEET_AVAILABLE:
        try:
            recs = fetch_recipients()
            active = [r for r in recs if r.get("enabled") and r.get("userId")]
        except Exception as e:
            logging.warning("シート読取でエラー。単一宛にフォールバック: %s", e)
            active = []

        if active:
            for r in active:
                _push_line(r["userId"], text)
                time.sleep(0.15)
            return

    if LINE_USER_ID:
        _push_line(LINE_USER_ID, text)
    else:
        logging.warning("宛先がありません（SHEETもLINE_USER_IDもなし）")


# ========== 補助: レース情報（場名/レース番号/騎手名） ==========
def _fetch_race_meta_and_jockeys(race_id: str) -> Tuple[str, str, Dict[int, str]]:
    """
    楽天レースカードから 場名, R, {馬番: 騎手名} をできる範囲で取得（失敗時は適当補完）
    """
    venue, race_no = "地方", "—R"
    jockey_map: Dict[int, str] = {}

    urls = [
        f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{race_id}",
        f"https://keiba.rakuten.co.jp/race/top/RACEID/{race_id}",
    ]
    for url in urls:
        try:
            resp = requests.get(url, timeout=8)
            if resp.status_code != 200:
                continue
            html = resp.text
            soup = BeautifulSoup(html, "html.parser")

            # 場名 / R
            if venue == "地方":
                t = soup.get_text(" ", strip=True)
                # 例: 「佐賀 8R 発走 18:40」などが本文に含まれることが多い
                # ゆるく場名とRを取る
                m1 = None
                # R
                m_r = None
                try:
                    m1 = next((w for w in t.split() if len(w) <= 4 and ("競馬" in w or w in t[:30])), None)
                except Exception:
                    pass
                m_r = None
                import re
                m = re.search(r"(\d{1,2})R", t)
                if m:
                    race_no = f"{m.group(1)}R"
                # venue はtitle等から拾う方が精度高い
                if soup.title and soup.title.string:
                    title = soup.title.string
                    # 「佐賀8R」など
                    m2 = re.search(r"(大井|川崎|船橋|浦和|門別|盛岡|水沢|園田|姫路|高知|佐賀|金沢|名古屋|笠松|帯広|ばんえい)", title)
                    if m2:
                        venue = m2.group(1)

            # 騎手
            # 出馬表の行を総なめにして「馬番」「騎手」らしき列を拾う
            for tr in soup.find_all("tr"):
                tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                if len(tds) < 3:
                    continue
                # 馬番は先頭付近に数字1〜2桁のセルがあることが多い
                umaban = None
                for cell in tds[:2]:
                    if cell.isdigit() and 1 <= int(cell) <= 18:
                        umaban = int(cell)
                        break
                if umaban is None:
                    continue
                # 騎手名らしきセルを拾う（「騎手」「kg」等に続く短い日本語）
                name = None
                for cell in tds:
                    if 1 <= len(cell) <= 6 and all('\u3040' <= ch <= '\u30ff' or '\u4e00' <= ch <= '\u9fff' for ch in cell):
                        # ざっくり日本語だけの短い文字列を候補に
                        name = cell
                if umaban is not None and name:
                    jockey_map[umaban] = name
        except Exception:
            continue

    return venue, race_no, jockey_map


# ========== 戦略ロジック ==========
def _pick_top_by_pop(horses: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    return sorted(horses, key=lambda x: x["pop"])[:n]

def _find_10_20_candidates(horses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # 2番人気以下の中で 10〜20倍（含む）を拾う
    out = []
    for h in horses:
        if h["pop"] >= 2 and 10.0 <= h["odds"] <= 20.0:
            out.append(h)
    # 人気の低い順ではなく「人気順」（=pop昇順）で揃える
    out.sort(key=lambda x: x["pop"])
    return out

def judge_strategies(odds: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    入力: fetch_tanfuku_odds() の戻り
      { race_id, venue, race_no, start_at_iso, horses: [{umaban, odds, pop}, ...] }
    戻り: ヒット配列 [{strategy_id, buy, picks, ...}]
    """
    horses = odds["horses"]
    top5 = _pick_top_by_pop(horses, 5)
    if len(top5) < 3:
        return []

    h1, h2, h3 = top5[0], top5[1], top5[2]
    h4 = top5[3] if len(top5) >= 4 else None

    hits: List[Dict[str, Any]] = []

    # ① 1〜3番人気三連単BOX
    # 1番人気2.0〜10.0, 2〜3番人気<10.0, 4番人気>=15.0
    cond1 = (
        2.0 <= h1["odds"] <= 10.0 and
        h2["odds"] < 10.0 and
        h3["odds"] < 10.0 and
        (h4 and h4["odds"] >= 15.0)
    )
    if cond1:
        hits.append({
            "strategy": "①",
            "title": "1〜3番人気 三連単BOX（6点）",
            "buy_desc": f"三連単BOX：{h1['pop']}-{h2['pop']}-{h3['pop']}（6点）",
            "triples": [],  # 通知は文面のみで十分
            "need_jockies": [h1["umaban"], h2["umaban"], h3["umaban"]],
        })

    # ② 1番人気1着固定 × 2・3番人気（三連単2点）
    # 1番人気<2.0, 2〜3番人気<10.0
    cond2 = (
        h1["odds"] < 2.0 and
        h2["odds"] < 10.0 and
        h3["odds"] < 10.0
    )
    if cond2:
        hits.append({
            "strategy": "②",
            "title": "1着固定（1番人気）× 2・3番人気（三連単2点）",
            "buy_desc": f"1→{h2['pop']},{h3['pop']} → {h2['pop']},{h3['pop']}（2点）",
            "triples": [],
            "need_jockies": [h1["umaban"], h2["umaban"], h3["umaban"]],
        })

    # ③ 1着固定 × 10〜20倍流し
    # 1番人気<=1.5, 相手: 2番人気以下で10〜20倍の馬から最大4頭
    cond3 = (h1["odds"] <= 1.5)
    if cond3:
        cands = _find_10_20_candidates(horses)
        if cands:
            # 4頭まで
            cands = cands[:4]
            # 買い目は「1→相手n頭→相手n頭（同一不可）」→ 点数は n*(n-1)
            n = len(cands)
            hits.append({
                "strategy": "③",
                "title": f"1着固定（1番人気）→相手{n}頭→相手{n}頭（三連単{n*(n-1)}点）",
                "buy_desc": f"1→{','.join(str(x['pop']) for x in cands)} → 同（3着も相手内）",
                "triples": [],
                "need_jockies": [h1["umaban"]],  # 要件: ③は1番人気の騎手のみ表示
                "cand_pop": [x["pop"] for x in cands],
            })

    # ④ 3着固定（3番人気固定）三連単（2点）
    # 1・2番人気<=3.0, 3番人気=6.0〜10.0, 4番人気>=15.0
    cond4 = (
        h1["odds"] <= 3.0 and
        h2["odds"] <= 3.0 and
        6.0 <= h3["odds"] <= 10.0 and
        (h4 and h4["odds"] >= 15.0)
    )
    if cond4:
        hits.append({
            "strategy": "④",
            "title": "3着固定（1番人気×2番人気）- 3番人気（三連単2点）",
            "buy_desc": f"{h1['pop']}→{h2['pop']}→{h3['pop']} / {h2['pop']}→{h1['pop']}→{h3['pop']}（2点）",
            "triples": [],
            "need_jockies": [h1["umaban"], h2["umaban"], h3["umaban"]],
        })

    return hits


# ========== メッセージ生成 ==========
def _format_time_jp(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso)
        return dt.strftime("%H:%M")
    except Exception:
        return "時刻未取得"

def build_message(odds: Dict[str, Any], hit: Dict[str, Any],
                  venue: str, race_no: str, start_iso: str,
                  jockey_map: Dict[int, str]) -> str:
    race_id = odds["race_id"]
    line = []
    line.append("【戦略一致】" + hit["strategy"])
    line.append(hit["title"])

    # レース情報
    line.append(f"対象 : {venue} {race_no}（発走 { _format_time_jp(start_iso) }）")
    line.append(hit["buy_desc"])

    # 騎手ランク表示（要件: ①②④は1〜3番人気まで3人表示、③は1番人気のみ）
    if hit["strategy"] in ("①", "②", "④"):
        # 人気上位3頭の馬番を odds から取得
        tops = sorted(odds["horses"], key=lambda x: x["pop"])[:3]
        rows = []
        for x in tops:
            name = jockey_map.get(x["umaban"], "-")
            jr = get_jrank(name) if name and name != "-" else "-"
            rows.append(f"{x['pop']}番人気 騎手:{name}（J:{jr}）")
        line.append("騎手ランク: " + " / ".join(rows))
    else:
        # ③
        top1 = sorted(odds["horses"], key=lambda x: x["pop"])[0]
        name = jockey_map.get(top1["umaban"], "-")
        jr = get_jrank(name) if name and name != "-" else "-"
        line.append(f"騎手ランク: 1番人気 騎手:{name}（J:{jr}）")

    # 参照URL（必要最低限に）
    line.append(f"単勝オッズ: https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}")

    return "\n".join(line)


# ========== メイン処理 ==========
def process_race(race_id: str, notified: Dict[str, Any]) -> None:
    # オッズ取得
    odds = fetch_tanfuku_odds(race_id)
    if not odds or not odds.get("horses"):
        logging.info("odds取得できず skip race_id=%s", race_id)
        return

    # 発走時刻（ISO）
    try:
        start_iso = get_race_start_iso(race_id)
    except Exception:
        start_iso = odds.get("start_at_iso")  # フォールバック

    # 締切ベースの通知タイミングか？
    if not start_iso or not within_cutoff_window(start_iso):
        return

    # レース情報（場名/レース番号/騎手）
    venue, race_no, jockey_map = _fetch_race_meta_and_jockeys(race_id)

    # 戦略判定
    hits = judge_strategies(odds)
    if not hits:
        return

    # 重複防止のキー日付
    key_date = (start_iso[:10] if start_iso else now_jst().strftime("%Y-%m-%d"))

    # 各戦略で通知
    for h in hits:
        key = f"{race_id}|{h['strategy']}|{key_date}"
        if notified.get(key):
            continue
        msg = build_message(odds, h, venue, race_no, start_iso, jockey_map)
        send_line_message(msg)
        notified[key] = int(time.time())


def main():
    logging.info("ジョブ開始 host=%s pid=%s", socket.gethostname(), os.getpid())
    logging.info("NOTIFIED_PATH=%s KILL_SWITCH=%s DRY_RUN=%s SHEET=%s", NOTIFIED_PATH, KILL_SWITCH, DRY_RUN, SHEET_AVAILABLE)

    notified = _load_notified()
    today = now_jst().strftime("%Y-%m-%d")
    notified = prune_notified(notified, keep_date=today)

    # レース列挙：RACEIDS（env）→ list_today_raceids()
    race_ids = []
    env_ids = os.getenv("RACEIDS", "").strip()
    if env_ids:
        race_ids = [x.strip() for x in env_ids.split(",") if x.strip()]
    else:
        try:
            race_ids = list_today_raceids() or []
        except Exception as e:
            logging.warning("レース列挙に失敗: %s", e)
            race_ids = []

    if not race_ids:
        logging.info("対象レースなし（RACEIDS 未設定/列挙不能）")
        _save_notified(notified)
        logging.info("ジョブ終了")
        return

    for rid in race_ids:
        try:
            process_race(rid, notified)
        except Exception as e:
            logging.warning("process_race 例外 rid=%s err=%s", rid, e)

    _save_notified(notified)
    logging.info("ジョブ終了")


if __name__ == "__main__":
    main()