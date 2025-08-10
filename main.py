# main.py
import os, re, json, time, logging, datetime as dt
from typing import List, Set
import requests

# ========== 設定 ==========
BASE = "https://keiba.rakuten.co.jp"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA, "Accept-Language": "ja,en-US;q=0.9"})

# 環境変数
NOTIFIED_PATH = os.getenv("NOTIFIED_PATH", "/tmp/notified_races.json")
LINE_TOKEN = os.getenv("LINE_NOTIFY_TOKEN", "").strip()
KILL_SWITCH = os.getenv("KILL_SWITCH", "False") == "True"
DRY_RUN = os.getenv("DRY_RUN", "False") == "True"
SHEET = os.getenv("SHEET", "True") == "True"  # 互換のため残すだけ（未使用でもOK）

# ログ
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ========== ユーティリティ ==========
def _get(url: str, max_retry=3, timeout=15) -> str:
    last = None
    for i in range(max_retry):
        r = SESSION.get(url, timeout=timeout)
        if r.ok and r.text:
            return r.text
        last = r
        time.sleep(1.2 * (i + 1))
    raise RuntimeError(f"GET失敗: {url} status={getattr(last,'status_code',None)}")

def jst_now() -> dt.datetime:
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

# ========== RACEID 抽出ロジック ==========
def _extract_course_list_ids(top_html: str) -> List[str]:
    """
    当日の出馬表トップから『各競馬場の“レース一覧”ページ』のRACEID（…400）を取る
    """
    ids: Set[str] = set()

    # race_track 部分のリンク
    for m in re.finditer(r"/race_card/list/RACEID/(\d{18})", top_html):
        rid = m.group(1)
        if rid.endswith("400"):
            ids.add(rid)

    # 「本日の発売情報」内の保険リンク
    for m in re.finditer(
        r'href="https://keiba\.rakuten\.co\.jp/race_card/list/RACEID/(\d{18})"', top_html
    ):
        rid = m.group(1)
        if rid.endswith("400"):
            ids.add(rid)

    return sorted(ids)

def _extract_race_ids_from_course(course_html: str) -> List[str]:
    """
    各場ページから「各RのRACEID（…01〜12）」を抜く
    links: /race_card/list/RACEID/XXXXXXXXXXXXXX01 など
           /odds/tanfuku/RACEID/XXXXXXXXXXXXXX01 なども拾ってOK
    """
    ids: Set[str] = set()
    for m in re.finditer(r"/(?:race_card/list|odds/[a-z]+)/RACEID/(\d{18})", course_html):
        rid = m.group(1)
        # 末尾が01〜12（各レース本体）だけ採用
        if re.search(r"\d{16}(0[1-9]|1[0-2])$", rid):
            ids.add(rid)
    return sorted(ids)

def list_today_raceids(now: dt.datetime | None = None) -> List[str]:
    if now is None:
        now = jst_now()
    ymd = now.strftime("%Y%m%d")

    # 当日の出馬表トップ
    top_url = f"{BASE}/race_card/list/RACEID/{ymd}000000000000"
    top_html = _get(top_url)
    course_list_ids = _extract_course_list_ids(top_html)

    if not course_list_ids:
        log.info("当日トップからコース一覧が見つかりませんでした（構造変更の可能性）")
        return []

    race_ids: List[str] = []
    for cid in course_list_ids:
        url = f"{BASE}/race_card/list/RACEID/{cid}"
        ch = _get(url)
        race_ids.extend(_extract_race_ids_from_course(ch))

    race_ids = sorted(set(race_ids))
    return race_ids

# ========== 発売中判定 ==========
ODDS_HINT_MUST = [
    "投票締切時刻",
    "投票する",           # ボタンラベル
]
ODDS_HINT_NEG = [
    "発売前", "未発売", "開催なし", "データがありません"
]

def is_selling_open(race_id: str) -> bool:
    """
    単勝/複勝ページで発売中をざっくり判定。
    """
    url = f"{BASE}/odds/tanfuku/RACEID/{race_id}"
    html = _get(url)

    # NGワードがあるなら未発売扱い
    if any(x in html for x in ODDS_HINT_NEG):
        return False

    # 必須ワードが揃っていれば発売中とみなす
    if all(x in html for x in ODDS_HINT_MUST):
        return True

    # 予備：見出しなど
    if re.search(r"オッズ.*?単勝", html) and "みんなの予想まとめ" in html:
        return True

    return False

# ========== 通知（LINE or ログ） ==========
def send_line(text: str):
    msg = f"[地方競馬] {text}"
    if not LINE_TOKEN:
        log.info("LINE_TOKEN未設定: %s", msg)
        return
    try:
        r = SESSION.post(
            "https://notify-api.line.me/api/notify",
            headers={"Authorization": f"Bearer {LINE_TOKEN}"},
            data={"message": msg},
            timeout=15,
        )
        if r.status_code != 200:
            log.warning("LINE通知失敗 status=%s body=%s", r.status_code, r.text[:200])
    except Exception as e:
        log.exception("LINE通知例外: %s", e)

# ========== 既通知の保存 ==========
def load_notified(path: str) -> Set[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except FileNotFoundError:
        return set()
    except Exception as e:
        log.warning("既通知JSON読込失敗: %s", e)
        return set()

def save_notified(path: str, ids: Set[str]):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(sorted(ids), f, ensure_ascii=False)
        log.info("notified saved: %s (bytes=%d)", path, os.path.getsize(path))
    except Exception as e:
        log.warning("notified保存失敗: %s", e)

# ========== メイン ==========
def main():
    log.info(
        "ジョブ開始 host=%s pid=%s", os.environ.get("RENDER_INSTANCE_ID", "?"), os.getpid()
    )
    log.info(
        "NOTIFIED_PATH=%s KILL_SWITCH=%s DRY_RUN=%s SHEET=%s",
        NOTIFIED_PATH, KILL_SWITCH, DRY_RUN, SHEET
    )

    if KILL_SWITCH:
        log.info("KILL_SWITCH=True のため何もしません。")
        return

    # 1) 今日の全RACEIDを収集
    race_ids = list_today_raceids()
    if not race_ids:
        log.info("本日のRACEIDが見つかりませんでした。終了。")
        return
    log.info("今日のRACEID件数: %d 例: %s ...", len(race_ids), ", ".join(race_ids[:5]))

    # 2) 既通知をロード
    notified = load_notified(NOTIFIED_PATH)

    # 3) 発売中をチェックして通知
    hits = []
    for rid in race_ids:
        if rid in notified:
            continue
        try:
            if is_selling_open(rid):
                hits.append(rid)
                if not DRY_RUN:
                    send_line(f"発売中: RACEID={rid}  オッズ: {BASE}/odds/tanfuku/RACEID/{rid}")
                else:
                    log.info("[DRY_RUN] 通知予定: %s", rid)
        except Exception as e:
            log.warning("判定失敗 RACEID=%s err=%s", rid, e)

    # 4) 保存
    if hits:
        notified.update(hits)
        save_notified(NOTIFIED_PATH, notified)

    log.info("HITS=%d", len(hits))
    log.info("ジョブ終了")

if __name__ == "__main__":
    main()