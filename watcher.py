# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視・通知バッチ（発走-12分ターゲット、15〜10分前通知 v2025-09-02A）
- 発走時刻 = race_card/list or detail ページから抽出（強化版フォールバック付き）
- 通知基準 = 発走時刻 - CUTOFF_OFFSET_MIN
- 窓判定   = target±(WINDOW_BEFORE/AFTER_MIN)
- 通知: 窓内1回のみ（TTL管理）/ Google Sheets 永続化 / 日次サマリ
"""

import os, re, json, time, random, logging, socket
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Set, Any

import requests
from bs4 import BeautifulSoup, Tag
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from strategy_rules import eval_strategy

# ===== JSTユーティリティ =====
JST = timezone(timedelta(hours=9))
def jst_now() -> datetime: return datetime.now(JST)
def jst_today() -> str: return jst_now().strftime("%Y%m%d")

# ===== ENV =====
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "ja,en-US;q=0.9",
})
TIMEOUT = (10, 25); RETRY = 3; SLEEP_BETWEEN = (0.6, 1.2)

LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"

START_HOUR = int(os.getenv("START_HOUR", "10"))
END_HOUR   = int(os.getenv("END_HOUR",   "22"))
DRY_RUN    = os.getenv("DRY_RUN", "False").lower() == "true"
FORCE_RUN  = os.getenv("FORCE_RUN", "0") == "1"

NOTIFY_ENABLED      = os.getenv("NOTIFY_ENABLED", "1") == "1"
NOTIFY_TTL_SEC      = int(os.getenv("NOTIFY_TTL_SEC", "3600"))
NOTIFY_COOLDOWN_SEC = int(os.getenv("NOTIFY_COOLDOWN_SEC", "1800"))

CUTOFF_OFFSET_MIN   = int(os.getenv("CUTOFF_OFFSET_MIN", "12"))
WINDOW_BEFORE_MIN   = int(os.getenv("WINDOW_BEFORE_MIN", "3"))
WINDOW_AFTER_MIN    = int(os.getenv("WINDOW_AFTER_MIN", "2"))
GRACE_SECONDS       = int(os.getenv("GRACE_SECONDS", "0"))

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "")
LINE_USER_ID      = os.getenv("LINE_USER_ID", "")
LINE_USER_IDS     = [s.strip() for s in os.getenv("LINE_USER_IDS","").split(",") if s.strip()]

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_SHEET_ID   = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_SHEET_TAB  = os.getenv("GOOGLE_SHEET_TAB", "notified")
USERS_SHEET_NAME  = os.getenv("USERS_SHEET_NAME", "1")
USERS_USERID_COL  = os.getenv("USERS_USERID_COL", "H")
BETS_SHEET_TAB    = os.getenv("BETS_SHEET_TAB", "bets")

DAILY_SUMMARY_HHMM = os.getenv("DAILY_SUMMARY_HHMM", "21:02")
ALWAYS_NOTIFY_DAILY_SUMMARY = os.getenv("ALWAYS_NOTIFY_DAILY_SUMMARY", "1") == "1"

UNIT_STAKE_YEN = int(os.getenv("UNIT_STAKE_YEN", "100"))
DEBUG_RACEIDS  = [s.strip() for s in os.getenv("DEBUG_RACEIDS","").split(",") if s.strip()]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ===== Google Sheets 基本 =====
def _sheet_service():
    if not GOOGLE_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        raise RuntimeError("Google Sheets 環境変数不足")
    info  = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets","v4",credentials=creds, cache_discovery=False)

def _resolve_sheet_title(svc, tab_or_gid: str) -> str:
    meta = svc.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    sheets = meta.get("sheets", [])
    if tab_or_gid.isdigit() and len(tab_or_gid)>3:
        gid = int(tab_or_gid)
        for s in sheets:
            if s["properties"]["sheetId"] == gid:
                return s["properties"]["title"]
    for s in sheets:
        if s["properties"]["title"] == tab_or_gid:
            return tab_or_gid
    svc.spreadsheets().batchUpdate(
        spreadsheetId=GOOGLE_SHEET_ID,
        body={"requests":[{"addSheet":{"properties":{"title": tab_or_gid}}}]}
    ).execute()
    return tab_or_gid

def _sheet_get(svc, title: str, a1: str) -> List[List[str]]:
    res = svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"'{title}'!{a1}").execute()
    return res.get("values", [])

def _sheet_put(svc, title: str, a1: str, values: List[List[str]]):
    svc.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID, range=f"'{title}'!{a1}",
        valueInputOption="RAW", body={"values": values}
    ).execute()

# ===== 発走時刻抽出強化版 =====
def _extract_start_hhmm_near_rid_from_daylist(html: str, rid: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    a = soup.find("a", href=re.compile(re.escape(rid)))
    if not a: return None

    ancestors = []
    node = a
    for _ in range(6):  # 深めに探索
        ancestors.append(node)
        node = getattr(node, "parent", None)
        if not node: break

    def _scan_container(el) -> Optional[str]:
        for t in el.find_all("time"):
            for attr in ("datetime","data-time","title","aria-label"):
                v = t.get(attr)
                if v:
                    m = re.search(r'([0-2]?\d)[:：]([0-5]\d)', str(v))
                    if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
            txt_t = t.get_text(" ", strip=True)
            m = re.search(r'([0-2]?\d)[:：]([0-5]\d)', txt_t)
            if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"

        txt = el.get_text(" ", strip=True)
        m = re.search(r'(?:発走|予定)\s*([0-2]?\d)[:：]([0-5]\d)', txt)
        if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
        m = re.search(r'([0-2]?\d)\s*時\s*([0-5]\d)\s*分', txt)
        if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
        m = re.search(r'([0-2]?\d)[:：]([0-5]\d)', txt)
        if m: return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
        return None

    for host in ancestors:
        hhmm = _scan_container(host)
        if hhmm: return hhmm

    sibs = list(ancestors[0].parent.children) if ancestors and ancestors[0].parent else []
    if sibs:
        try: idx = sibs.index(ancestors[0])
        except ValueError: idx = -1
        rng = sibs[max(0, idx-2): idx] + sibs[idx+1: idx+3] if idx != -1 else []
        for s in rng:
            if not hasattr(s, "get_text"): continue
            hhmm = _scan_container(s)
            if hhmm: return hhmm
    return None
    
# ===== 通知窓判定 =====
def is_within_window(target_dt: datetime) -> bool:
    now = jst_now()
    start = target_dt - timedelta(minutes=WINDOW_BEFORE_MIN)
    end   = target_dt + timedelta(minutes=WINDOW_AFTER_MIN)
    return (start - timedelta(seconds=GRACE_SECONDS)) <= now <= (end + timedelta(seconds=GRACE_SECONDS))

# ===== main =====
def main():
    logging.info("[BOOT] host=%s pid=%s", socket.gethostname(), os.getpid())
    logging.info("[BOOT] now(JST)=%s CUT=%s", jst_now().strftime("%Y-%m-%d %H:%M:%S %z"), CUTOFF_OFFSET_MIN)

    # RIDS収集と発走判定 → 通知処理（省略、既存の eval_strategy & push_line_text 流用）
    # ...
    logging.info("[INFO] ジョブ終了")

def run_watcher_forever(sleep_sec: int = 60):
    logging.info("[INFO] watcher.start (sleep=%ss)", sleep_sec)
    while True:
        try:
            main()
        except Exception as e:
            logging.exception("[FATAL] loop error: %s", e)
        time.sleep(max(10, sleep_sec))

if __name__ == "__main__":
    main()