# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視・通知バッチ（完全差し替え版 v2025-08-15E）
- 締切時刻：単複オッズ/一覧ページから“締切”を直接抽出（最優先）
- 発走時刻：一覧ページ優先＋オッズ詳細のフォールバック
- 窓判定：ターゲット時刻（締切 or 発走）基準、±GRACE_SECONDS の許容
- 通知：窓内1回 / 429時はクールダウン / Google SheetでTTL永続
- 送信先：Googleシート(タブA=名称「1」)のH列から userId を収集
- 戦略③：専用フォーマット（1軸・相手10〜20倍・候補最大4頭・点数表示）
- 騎手ランク：内蔵200位＋表記ゆれ耐性（強化クレンジング＋前方一致＋姓一致フォールバック）
- ★通知本文：買い目を「馬番＋オッズ＋騎手ランク」で表示
  例：3番（1人気／1.7倍／A）- 5番（3人気／6.0倍／B）
  ※戦略1/2/4の表示は『人気優先→馬番へ変換』に統一（11人気混入対策）
- 未一致の騎手名は [RANKMISS] ログに記録（重複抑止）＋ [RANKDBG] で突合過程を出力
- betsシート：馬番ベースで記録（仕様は従来通り）
- 日次サマリ：JST 21:02 に当日1回だけ送信（0件でも送信） ← FIX
- 券種は STRATEGY_BET_KIND_JSON で設定（既定: ①馬連, ②馬単, ③三連単, ④三連複）

★締切基準で運用する場合：
  - 環境変数 CUTOFF_OFFSET_MIN を 5（推奨）に設定
  - 本版は “締切そのもの” を抽出できたらそれを採用。取れない場合のみ「発走-5分」を代用。

★本版の変更点（E）：
  - 日次サマリ実行を 21:02 で確実にトリガ（>= 判定・重複抑止はシートの summary フラグ）
  - サマリ作成中の払戻ページ404等はレース単位で捕捉してスキップ（処理全体を落とさない）
  - サマリ開始・結果・スキップ理由などログを強化
"""

import os, re, json, time, random, logging, pathlib, hashlib, unicodedata
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Set

import requests
from bs4 import BeautifulSoup, Tag
from strategy_rules import eval_strategy

# --- 通知ログ append のフォールバック付き import ---
try:
    from utils_notify_log import append_notify_log
except ModuleNotFoundError:
    import logging as _logging
    def append_notify_log(*args, **kwargs):
        _logging.warning("[WARN] utils_notify_log が見つからないため、通知ログの追記をスキップします。")

# 日付ユーティリティ
from utils_summary import jst_today_str, jst_now

# ===== Google Sheets =====
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ========= 基本設定 =========
JST = timezone(timedelta(hours=9))
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
})
TIMEOUT = (10, 25)
RETRY = 3
SLEEP_BETWEEN = (0.6, 1.2)

LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"

# ========= 環境変数 =========
START_HOUR          = int(os.getenv("START_HOUR", "10"))
END_HOUR            = int(os.getenv("END_HOUR",   "22"))
DRY_RUN             = os.getenv("DRY_RUN", "False").lower() == "true"
KILL_SWITCH         = os.getenv("KILL_SWITCH", "False").lower() == "true"
NOTIFY_ENABLED      = os.getenv("NOTIFY_ENABLED", "1") == "1"
DEBUG_RACEIDS       = [s.strip() for s in os.getenv("DEBUG_RACEIDS", "").split(",") if s.strip()]

NOTIFY_TTL_SEC      = int(os.getenv("NOTIFY_TTL_SEC", "3600"))
NOTIFY_COOLDOWN_SEC = int(os.getenv("NOTIFY_COOLDOWN_SEC", "1800"))

WINDOW_BEFORE_MIN   = int(os.getenv("WINDOW_BEFORE_MIN", "15"))
WINDOW_AFTER_MIN    = int(os.getenv("WINDOW_AFTER_MIN", "0"))   # 締切運用なら 0 推奨
CUTOFF_OFFSET_MIN   = int(os.getenv("CUTOFF_OFFSET_MIN", "0"))  # 例: 5（締切直接取得できない場合の代用）
FORCE_RUN           = os.getenv("FORCE_RUN", "0") == "1"
GRACE_SECONDS       = int(os.getenv("GRACE_SECONDS", "60"))     # 境界許容（秒）

LINE_ACCESS_TOKEN   = os.getenv("LINE_ACCESS_TOKEN", "")
LINE_USER_ID        = os.getenv("LINE_USER_ID", "")
LINE_USER_IDS       = [s.strip() for s in os.getenv("LINE_USER_IDS", "").split(",") if s.strip()]

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_SHEET_ID         = os.getenv("GOOGLE_SHEET_ID", "")

# TTL管理タブ（名前 or gid）
GOOGLE_SHEET_TAB        = os.getenv("GOOGLE_SHEET_TAB", "notified")

# 送信先ユーザーを読むタブA（=「1」）と列（=H）
USERS_SHEET_NAME        = os.getenv("USERS_SHEET_NAME", "1")
USERS_USERID_COL        = os.getenv("USERS_USERID_COL", "H")

# ベット記録タブ
BETS_SHEET_TAB          = os.getenv("BETS_SHEET_TAB", "bets")

# 券種（戦略→券種）
_DEFAULT_BET_KIND = {"1":"馬連", "2":"馬単", "3":"三連単", "4":"三連複"}
try:
    STRATEGY_BET_KIND = json.loads(os.getenv("STRATEGY_BET_KIND_JSON","")) or _DEFAULT_BET_KIND
except Exception:
    STRATEGY_BET_KIND = _DEFAULT_BET_KIND

UNIT_STAKE_YEN = int(os.getenv("UNIT_STAKE_YEN", "100"))  # 1点100円

# === 日次サマリ ===
DAILY_SUMMARY_HHMM = os.getenv("DAILY_SUMMARY_HHMM", "21:02")  # 決まった時刻に1回送る（JST）
ALWAYS_NOTIFY_DAILY_SUMMARY = os.getenv("ALWAYS_NOTIFY_DAILY_SUMMARY", "1") == "1"  # 0件でも送る

RACEID_RE   = re.compile(r"/RACEID/(\d{18})")
TIME_PATS = [
    re.compile(r"\b(\d{1,2}):(\d{2})\b"),
    re.compile(r"\b(\d{1,2})：(\d{2})\b"),
    re.compile(r"\b(\d{1,2})\s*時\s*(\d{1,2})\s*分\b"),
]
PLACEHOLDER = re.compile(r"\d{8}0000000000$")

# ラベル類
IGNORE_NEAR_PAT   = re.compile(r"(現在|更新|発売|確定|払戻|実況)")
POST_LABEL_PAT    = re.compile(r"(発走|発走予定|発走時刻|発送|出走)")
CUTOFF_LABEL_PAT  = re.compile(r"(投票締切|発売締切|締切)")

# ========= 騎手ランク（1〜200位を内蔵） =========
_RANKMISS_SEEN: Set[str] = set()

def _log_rank_miss(orig: str, norm: str):
    key = f"{orig}|{norm}"
    if key not in _RANKMISS_SEEN:
        _RANKMISS_SEEN.add(key)
        logging.info("[RANKMISS] name_raw=%s name_norm=%s", orig, norm)

def _normalize_name(s: str) -> str:
    """全半角正規化・空白除去・旧字体/異体字の代表表記化"""
    if not s: return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace(" ", "").replace("\u3000", "")
    replace_map = {
        "𠮷": "吉", "栁": "柳", "髙": "高", "濵": "浜", "﨑": "崎", "嶋": "島", "峯": "峰",
        "齋": "斎", "齊": "斉", "內": "内", "冨": "富", "國": "国", "體": "体", "眞": "真",
        "廣": "広", "邊": "辺", "邉": "辺", "渡邊": "渡辺", "渡邉": "渡辺",
    }
    for k, v in replace_map.items():
        s = s.replace(k, v)
    return s

def _clean_jockey_name(s: str) -> str:
    """括弧/斤量/印/接尾辞(J/Ｊ/騎手)を除去し素の氏名へ"""
    if not s: return ""
    s = re.sub(r"[（(].*?[）)]", "", s)                           # 括弧内
    s = re.sub(r"[▲△☆★◇◆⊙◎○◯◉⚪︎＋+＊*]", "", s)                # 印
    s = re.sub(r"\d+(?:\.\d+)?\s*(?:kg|斤)?", "", s)               # 斤量
    s = s.replace("斤量", "")
    s = s.replace("騎手", "").replace("J", "").replace("Ｊ", "")   # 接尾辞
    s = re.sub(r"\s+", "", s)
    return s

# 1〜200位ランク表（抜粋先頭。※野畑凌=ランク6 → A）
JOCKEY_RANK_TABLE_RAW: Dict[int, str] = json.loads(os.getenv("JOCKEY_RANKS_JSON", "{}"))
JOCKEY_RANK_TABLE_RAW: Dict[int, str] = {
_JOCKEY_NAME_TO_RANK: Dict[str, int] = { _normalize_name(v): k for k, v in JOCKEY_RANK_TABLE_RAW.items() }

def _split_family_given(n: str) -> Tuple[str, str]:
    """姓・名（名は連結）を返す。空白が無ければ全体を姓として扱う。"""
    if not n: return "", ""
    parts = re.split(r"[\s\u3000]", n)
    if len(parts) >= 2:
        return parts[0], "".join(parts[1:])
    return n, ""

def _best_match_rank(name_norm: str) -> Optional[int]:
    """
    直接一致がない場合のフォールバック：
      1) 前方一致/逆前方一致
      2) 姓完全一致＋名頭文字一致
      3) 姓完全一致
      → tie はランク上位を優先
    """
    cands=[]
    fam, given = _split_family_given(name_norm)
    for n2, rank in _JOCKEY_NAME_TO_RANK.items():
        if n2.startswith(name_norm) or name_norm.startswith(n2):
            cands.append((0, rank)); continue
        f2, g2 = _split_family_given(n2)
        if fam and fam == f2:
            if given and g2 and given[0] == g2[0]:
                cands.append((1, rank))
            else:
                cands.append((2, rank))
    if not cands: return None
    cands.sort(key=lambda x:(x[0], x[1]))
    return cands[0][1]

def jockey_rank_letter_by_name(name: Optional[str]) -> str:
    """表示ランク: A=1〜70 / B=71〜200 / C=その他 / —=名前なし"""
    if not name: return "—"
    base_raw = _clean_jockey_name(name)
    base = _normalize_name(base_raw)
    rank = _JOCKEY_NAME_TO_RANK.get(base)
    if rank is None and base:
        rank = _best_match_rank(base)
    if rank is None:
        _log_rank_miss(base_raw, base)
        return "C"
    return "A" if 1<=rank<=70 else ("B" if 71<=rank<=200 else "C")

# ========= 共通 =========
def now_jst() -> datetime: return datetime.now(JST)

def within_operating_hours() -> bool:
    if FORCE_RUN: return True
    return START_HOUR <= now_jst().hour < END_HOUR

def fetch(url: str) -> str:
    last_err=None
    for i in range(1, RETRY+1):
        try:
            r=SESSION.get(url, timeout=TIMEOUT); r.raise_for_status()
            r.encoding="utf-8"; return r.text
        except Exception as e:
            last_err=e; wait=random.uniform(*SLEEP_BETWEEN)
            logging.warning(f"[WARN] fetch失敗({i}/{RETRY}) {e} -> {wait:.1f}s待機: {url}")
            time.sleep(wait)
    raise last_err

# ========= Google Sheets =========
def _sheet_service():
    if not GOOGLE_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        raise RuntimeError("Google Sheets の環境変数不足")
    info=json.loads(GOOGLE_CREDENTIALS_JSON)
    creds=Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets","v4",credentials=creds, cache_discovery=False)

def _resolve_sheet_title(svc, tab_or_gid: str) -> str:
    meta=svc.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    sheets=meta.get("sheets", [])
    if tab_or_gid.isdigit() and len(tab_or_gid)>3:
        gid=int(tab_or_gid)
        for s in sheets:
            if s["properties"]["sheetId"]==gid:
                return s["properties"]["title"]
        raise RuntimeError(f"指定gidのシートが見つかりません: {gid}")
    for s in sheets:
        if s["properties"]["title"]==tab_or_gid:
            return tab_or_gid
    body={"requests":[{"addSheet":{"properties":{"title":tab_or_gid}}}]}
    svc.spreadsheets().batchUpdate(spreadsheetId=GOOGLE_SHEET_ID, body=body).execute()
    return tab_or_gid

def _sheet_get_range_values(svc, title: str, a1: str) -> List[List[str]]:
    res=svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"'{title}'!{a1}").execute()
    return res.get("values", [])

def _sheet_update_range_values(svc, title: str, a1: str, values: List[List[str]]):
    svc.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID, range=f"'{title}'!{a1}",
        valueInputOption="RAW", body={"values": values}).execute()

def sheet_load_notified() -> Dict[str, float]:
    svc=_sheet_service(); title=_resolve_sheet_title(svc, GOOGLE_SHEET_TAB)
    values=_sheet_get_range_values(svc, title, "A:C")
    start = 1 if values and values[0] and str(values[0][0]).upper() in ("KEY","RACEID","RID","ID") else 0
    d={}
    for row in values[start:]:
        if not row or len(row)<2: continue
        key=str(row[0]).strip()
        try: d[key]=float(row[1])
        except: pass
    return d

def sheet_upsert_notified(key: str, ts: float, note: str = "") -> None:
    svc=_sheet_service(); title=_resolve_sheet_title(svc, GOOGLE_SHEET_TAB)
    values=_sheet_get_range_values(svc, title, "A:C")
    header=["KEY","TS_EPOCH","NOTE"]
    if not values:
        _sheet_update_range_values(svc, title, "A:C", [header, [key, ts, note]]); return
    start_row=1 if values and values[0] and values[0][0] in header else 0
    found=None
    for i,row in enumerate(values[start_row:], start=start_row):
        if row and str(row[0]).strip()==key: found=i; break
    if found is None: values.append([key, ts, note])
    else: values[found]=[key, ts, note]
    _sheet_update_range_values(svc, title, "A:C", values)

# ========= 送信先ユーザー =========
def load_user_ids_from_simple_col() -> List[str]:
    svc=_sheet_service(); title=USERS_SHEET_NAME; col=USERS_USERID_COL.upper()
    values=_sheet_get_range_values(svc, title, f"{col}:{col}")
    user_ids=[]
    for i,row in enumerate(values):
        v=(row[0].strip() if row and row[0] is not None else "")
        if not v: continue
        low=v.replace(" ","").lower()
        if i==0 and ("userid" in low or "user id" in low or "line" in low): continue
        if not v.startswith("U"): continue
        if v not in user_ids: user_ids.append(v)
    logging.info("[INFO] usersシート読込: %d件 from tab=%s", len(user_ids), title)
    return user_ids

# ========= HTMLユーティリティ =========
def _extract_raceids_from_soup(soup: BeautifulSoup) -> List[str]:
    rids=[]
    for a in soup.find_all("a", href=True):
        m=RACEID_RE.search(a["href"])
        if m:
            rid=m.group(1)
            if not PLACEHOLDER.search(rid): rids.append(rid)
    return sorted(set(rids))

def _rid_date_parts(rid: str) -> Tuple[int,int,int]:
    return int(rid[0:4]), int(rid[4:6]), int(rid[6:8])

def _norm_hhmm_from_text(text: str) -> Optional[Tuple[int,int,str]]:
    if not text: return None
    s=str(text)
    for pat, tag in zip(TIME_PATS, ("half","full","kanji")):
        m=pat.search(s)
        if m:
            hh=int(m.group(1)); mm=int(m.group(2))
            if 0<=hh<=23 and 0<=mm<=59: return hh,mm,tag
    return None

def _make_dt_from_hhmm(rid: str, hh: int, mm: int) -> Optional[datetime]:
    try:
        y,mon,d=_rid_date_parts(rid)
        return datetime(y,mon,d,hh,mm,tzinfo=JST)
    except: return None

def _find_time_nearby(el: Tag) -> Tuple[Optional[str], str]:
    t=el.find("time")
    if t:
        for attr in ("datetime","data-time","title","aria-label"):
            v=t.get(attr)
            if v:
                got=_norm_hhmm_from_text(v)
                if got: hh,mm,why=got; return f"{hh:02d}:{mm:02d}", f"time@{attr}/{why}"
        got=_norm_hhmm_from_text(t.get_text(" ", strip=True))
        if got: hh,mm,why=got; return f"{hh:02d}:{mm:02d}", f"time@text/{why}"
    for node in el.find_all(True, recursive=True):
        for attr in ("data-starttime","data-start-time","data-time","title","aria-label"):
            v=node.get(attr)
            if not v: continue
            got=_norm_hhmm_from_text(v)
            if got: hh,mm,why=got; return f"{hh:02d}:{mm:02d}", f"data:{attr}/{why}"
    for sel in [".startTime",".cellStartTime",".raceTime",".time",".start-time"]:
        node=el.select_one(sel)
        if node:
            got=_norm_hhmm_from_text(node.get_text(" ", strip=True))
            if got: hh,mm,why=got; return f"{hh:02d}:{mm:02d}", f"sel:{sel}/{why}"
    got=_norm_hhmm_from_text(el.get_text(" ", strip=True))
    if got: hh,mm,why=got; return f"{hh:02d}:{mm:02d}", f"row:text/{why}"
    return None, "-"

# ========= 発走時刻（一覧ページ）解析 =========
def parse_post_times_from_table_like(root: Tag) -> Dict[str, datetime]:
    post_map={}
    # テーブル
    for table in root.find_all("table"):
        thead=table.find("thead")
        if thead:
            head_text="".join(thead.stripped_strings)
            if not any(k in head_text for k in ("発走","発走時刻","レース")): continue
        body=table.find("tbody") or table
        for tr in body.find_all("tr"):
            rid=None; link=tr.find("a", href=True)
            if link:
                m=RACEID_RE.search(link["href"]); 
                if m: rid=m.group(1)
            if not rid or PLACEHOLDER.search(rid): continue
            hhmm,_=_find_time_nearby(tr)
            if not hhmm: continue
            hh,mm=map(int, hhmm.split(":"))
            dt=_make_dt_from_hhmm(rid, hh, mm)
            if dt: post_map[rid]=dt
    # カード型
    for a in root.find_all("a", href=True):
        m=RACEID_RE.search(a["href"])
        if not m: continue
        rid=m.group(1)
        if PLACEHOLDER.search(rid) or rid in post_map: continue
        host=None; depth=0
        for parent in a.parents:
            if isinstance(parent, Tag) and parent.name in ("tr","li","div","section","article"):
                host=parent; break
            depth+=1
            if depth>=6: break
        host=host or a
        hhmm,_=_find_time_nearby(host)
        if not hhmm:
            sib_text=" ".join([x.get_text(" ", strip=True) for x in a.find_all_next(limit=4) if isinstance(x, Tag)])
            got=_norm_hhmm_from_text(sib_text)
            if got:
                hh,mm,_=got; hhmm=f"{hh:02d}:{mm:02d}"
        if not hhmm: continue
        hh,mm=map(int, hhmm.split(":"))
        dt=_make_dt_from_hhmm(rid, hh, mm)
        if dt: post_map[rid]=dt
    return post_map

def collect_post_time_map(ymd: str, ymd_next: str) -> Dict[str, datetime]:
    post_map={}
    def _merge_from(url: str):
        try:
            soup=BeautifulSoup(fetch(url), "lxml")
            post_map.update(parse_post_times_from_table_like(soup))
        except Exception as e:
            logging.warning(f"[WARN] 発走一覧読み込み失敗: {e} ({url})")
    _merge_from(f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000")
    _merge_from(f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd_next}0000000000")
    logging.info(f"[INFO] 発走時刻取得: {len(post_map)}件")
    return post_map

# ========= 締切時刻（最優先で抽出） =========
def _extract_cutoff_hhmm_from_soup(soup: BeautifulSoup) -> Optional[str]:
    # セレクタ優先
    for sel in ["time[data-type='cutoff']", ".cutoff time", ".deadline time", ".time.-deadline"]:
        t=soup.select_one(sel)
        if t:
            got=_norm_hhmm_from_text(t.get_text(" ", strip=True) or t.get("datetime",""))
            if got:
                hh,mm,_=got; return f"{hh:02d}:{mm:02d}"
    # ラベル近傍
    for node in soup.find_all(string=CUTOFF_LABEL_PAT):
        container=getattr(node, "parent", None) or soup
        host=container
        for p in container.parents:
            if isinstance(p, Tag) and p.name in ("div","section","article","li"): host=p; break
        text=" ".join(host.get_text(" ", strip=True).split())
        got=_norm_hhmm_from_text(text)
        if got:
            hh,mm,_=got; return f"{hh:02d}:{mm:02d}"
    # 全文フォールバック
    txt=" ".join(soup.stripped_strings)
    if CUTOFF_LABEL_PAT.search(txt):
        got=_norm_hhmm_from_text(txt)
        if got:
            hh,mm,_=got; return f"{hh:02d}:{mm:02d}"
    return None

def resolve_cutoff_dt(rid: str) -> Optional[Tuple[datetime, str]]:
    try:
        soup=BeautifulSoup(fetch(f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"), "lxml")
        hhmm=_extract_cutoff_hhmm_from_soup(soup)
        if hhmm:
            hh,mm=map(int, hhmm.split(":"))
            dt=_make_dt_from_hhmm(rid, hh, mm)
            if dt: return dt, "tanfuku"
    except Exception as e:
        logging.warning("[WARN] 締切抽出(tanfuku)失敗 rid=%s: %s", rid, e)
    try:
        soup=BeautifulSoup(fetch(f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{rid}"), "lxml")
        hhmm=_extract_cutoff_hhmm_from_soup(soup)
        if hhmm:
            hh,mm=map(int, hhmm.split(":"))
            dt=_make_dt_from_hhmm(rid, hh, mm)
            if dt: return dt, "list"
    except Exception as e:
        logging.warning("[WARN] 締切抽出(list)失敗 rid=%s: %s", rid, e)
    return None

# ========= オッズ解析（単複ページ） =========
def _clean(s: str) -> str: return re.sub(r"\s+","", s or "")
def _as_float(text: str) -> Optional[float]:
    if not text: return None
    t=text.replace(",","").strip()
    if "%" in t or "-" in t or "～" in t or "~" in t: return None
    m=re.search(r"\d+(?:\.\d+)?", t); return float(m.group(0)) if m else None
def _as_int(text: str) -> Optional[int]:
    if not text: return None
    m=re.search(r"\d+", text); return int(m.group(0)) if m else None

def _find_popular_odds_table(soup: BeautifulSoup) -> Tuple[Optional[BeautifulSoup], Dict[str,int]]:
    for table in soup.find_all("table"):
        thead=table.find("thead"); 
        if not thead: continue
        headers=[_clean(th.get_text()) for th in thead.find_all(["th","td"])]
        if not headers: continue
        pop_idx=win_idx=num_idx=jockey_idx=None
        for i,h in enumerate(headers):
            if h in ("人気","順位") or ("人気" in h and "順" not in h): pop_idx=i; break
        win_c=[]
        for i,h in enumerate(headers):
            if ("複" in h) or ("率" in h) or ("%" in h): continue
            if   h=="単勝": win_c.append((0,i))
            elif "単勝" in h: win_c.append((1,i))
            elif "オッズ" in h: win_c.append((2,i))
        win_idx=sorted(win_c,key=lambda x:x[0])[0][1] if win_c else None
        for i,h in enumerate(headers):
            if "馬番" in h: num_idx=i; break
        if num_idx is None:
            for i,h in enumerate(headers):
                if ("馬" in h) and ("馬名" not in h) and (i!=pop_idx): num_idx=i; break
        for i,h in enumerate(headers):
            if any(k in h for k in ("騎手","騎手名")): jockey_idx=i; break
        if pop_idx is None or win_idx is None: continue
        body=table.find("tbody") or table
        rows=body.find_all("tr")
        seq_ok,last=0,0
        for tr in rows[:6]:
            tds=tr.find_all(["td","th"])
            if len(tds)<=max(pop_idx,win_idx): continue
            s=tds[pop_idx].get_text(strip=True)
            if not s.isdigit(): break
            v=int(s)
            if v<=last: break
            last=v; seq_ok+=1
        if seq_ok>=2:
            return table, {"pop":pop_idx,"win":win_idx,"num":num_idx if num_idx is not None else -1,"jockey":jockey_idx if jockey_idx is not None else -1}
    return None, {}

def parse_odds_table(soup: BeautifulSoup) -> Tuple[List[Dict[str,float]], Optional[str], Optional[str]]:
    venue_race=(soup.find("h1").get_text(strip=True) if soup.find("h1") else None)
    nowtime=soup.select_one(".withUpdate .nowTime") or soup.select_one(".nowTime")
    now_label=nowtime.get_text(strip=True) if nowtime else None
    table, idx=_find_popular_odds_table(soup)
    if not table: return [], venue_race, now_label
    pop_idx=idx["pop"]; win_idx=idx["win"]; num_idx=idx.get("num",-1); jockey_idx=idx.get("jockey",-1)
    horses=[]; body=table.find("tbody") or table
    for tr in body.find_all("tr"):
        tds=tr.find_all(["td","th"])
        if len(tds)<=max(pop_idx,win_idx): continue
        pop_txt=tds[pop_idx].get_text(strip=True)
        if not pop_txt.isdigit(): continue
        pop=int(pop_txt)
        if not (1<=pop<=30): continue
        odds=_as_float(tds[win_idx].get_text(" ", strip=True))
        if odds is None: continue
        num=None
        if 0<=num_idx<len(tds): num=_as_int(tds[num_idx].get_text(" ", strip=True))
        jockey=None
        if 0<=jockey_idx<len(tds):
            jt=tds[jockey_idx].get_text(" ", strip=True)
            jraw=re.split(r"[（( ]", jt)[0].strip() if jt else None
            jclean=_clean_jockey_name(jraw) if jraw else None
            jockey=jclean if jclean else None
        rec={"pop":pop,"odds":float(odds)}
        if num is not None: rec["num"]=num
        if jockey: rec["jockey"]=jockey
        horses.append(rec)
    # 人気重複の排除
    uniq={}
    for h in sorted(horses, key=lambda x:x["pop"]): uniq[h["pop"]]=h
    horses=[uniq[k] for k in sorted(uniq.keys())]
    return horses, venue_race, now_label

# === 出馬表からの騎手補完（＋補完後の再正規化） ===
def fetch_jockey_map_from_card(race_id: str) -> Dict[int, str]:
    urls=[f"https://keiba.rakuten.co.jp/race_card/RACEID/{race_id}",
          f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{race_id}"]
    result={}
    for url in urls:
        try: soup=BeautifulSoup(fetch(url),"lxml")
        except Exception: continue
        for table in soup.find_all("table"):
            thead=table.find("thead")
            if not thead: continue
            headers=[_clean(th.get_text()) for th in thead.find_all(["th","td"])]
            if not headers: continue
            num_idx=next((i for i,h in enumerate(headers) if "馬番" in h), -1)
            jockey_idx=next((i for i,h in enumerate(headers) if any(k in h for k in ("騎手","騎手名"))), -1)
            if num_idx<0 or jockey_idx<0: continue
            body=table.find("tbody") or table
            for tr in body.find_all("tr"):
                tds=tr.find_all(["td","th"])
                if len(tds)<=max(num_idx, jockey_idx): continue
                num=_as_int(tds[num_idx].get_text(" ", strip=True))
                jtx=tds[jockey_idx].get_text(" ", strip=True)
                if num is None or not jtx: continue
                name=_clean_jockey_name(re.split(r"[（(]", jtx)[0])
                if name: result[num]=name
            if result: return result
    return result

def _enrich_horses_with_jockeys(horses: List[Dict[str,float]], race_id: str) -> None:
    need=any((h.get("jockey") is None) and isinstance(h.get("num"), int) for h in horses)
    num2jockey=fetch_jockey_map_from_card(race_id) if need else {}
    for h in horses:
        if (not h.get("jockey")) and isinstance(h.get("num"), int):
            name=num2jockey.get(h["num"])
            if name: h["jockey"]=_clean_jockey_name(name)
        if h.get("jockey"):
            h["jockey"]=_clean_jockey_name(h["jockey"])  # 再正規化

def _debug_jockey_match(horses: List[Dict[str,float]]):
    for h in sorted(horses, key=lambda x:int(x.get("pop",999)))[:5]:
        raw=h.get("jockey") or ""
        norm=_normalize_name(_clean_jockey_name(raw))
        r=jockey_rank_letter_by_name(raw)
        logging.debug("[RANKDBG] pop=%s uma=%s jockey_raw=%s norm=%s rank=%s",
                      h.get("pop"), h.get("num"), raw, norm, r)

def check_tanfuku_page(race_id: str) -> Optional[Dict]:
    url=f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{race_id}"
    html=fetch(url); soup=BeautifulSoup(html,"lxml")
    horses, venue_race, now_label = parse_odds_table(soup)
    if not horses: return None
    if not venue_race: venue_race="地方競馬"
    _enrich_horses_with_jockeys(horses, race_id)
    _debug_jockey_match(horses)
    return {"race_id": race_id, "url": url, "horses": horses, "venue_race": venue_race, "now": now_label or ""}

# ========= 発走時刻フォールバック =========
def fallback_post_time_for_rid(rid: str) -> Optional[Tuple[datetime, str, str]]:
    def _from_list_page():
        url=f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{rid}"
        soup=BeautifulSoup(fetch(url),"lxml")
        a=soup.find("a", href=re.compile(rf"/RACEID/{rid}"))
        if not a: return None
        host=None
        for parent in a.parents:
            if isinstance(parent, Tag) and parent.name in ("tr","li","div","section","article"):
                host=parent; break
        host=host or a
        hhmm,reason=_find_time_nearby(host)
        if not hhmm:
            sibs=[n for n in host.find_all_next(limit=6) if isinstance(n, Tag)]
            text=" ".join([n.get_text(" ", strip=True) for n in sibs])
            if POST_LABEL_PAT.search(text):
                got=_norm_hhmm_from_text(text)
                if got:
                    hh,mm,why=got; hhmm,reason=f"{hh:02d}:{mm:02d}", f"sibling:label-first/{why}"
            else:
                if not IGNORE_NEAR_PAT.search(text):
                    got=_norm_hhmm_from_text(text)
                    if got:
                        hh,mm,why=got; hhmm,reason=f"{hh:02d}:{mm:02d}", f"sibling:text/{why}"
        if not hhmm: return None
        hh,mm=map(int, hhmm.split(":")); dt=_make_dt_from_hhmm(rid, hh, mm)
        return (dt, f"list-anchor/{reason}", url) if dt else None

    def _from_tanfuku_page():
        url=f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{rid}"
        soup=BeautifulSoup(fetch(url),"lxml")
        for key in ("発走","発走時刻","発走予定","発送","出走"):
            for node in soup.find_all(string=re.compile(key)):
                el=getattr(node,"parent",None) or soup
                container=el
                for parent in el.parents:
                    if isinstance(parent, Tag) and parent.name in ("div","section","article","li"):
                        container=parent; break
                near=" ".join((container.get_text(" ", strip=True) or "").split())
                if IGNORE_NEAR_PAT.search(near) and not POST_LABEL_PAT.search(near):
                    continue
                got=_norm_hhmm_from_text(near)
                if got:
                    hh,mm,why=got; dt=_make_dt_from_hhmm(rid, hh, mm)
                    if dt: return dt, f"tanfuku-label/{key}/{why}", url
        return None

    try:
        got=_from_list_page()
        if got: return got
    except Exception as e:
        logging.warning("[WARN] fallback(list)失敗 rid=%s: %s", e)
    try:
        got=_from_tanfuku_page()
        if got: return got
    except Exception as e:
        logging.warning("[WARN] fallback(tanfuku)失敗 rid=%s: %s", e)
    return None

# ========= RACEID 列挙 =========
def list_raceids_today_ticket(ymd: str) -> List[str]:
    url=f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000"
    soup=BeautifulSoup(fetch(url),"lxml")
    ids=_extract_raceids_from_soup(soup)
    logging.info(f"[INFO] Rakuten#1 本日の発売情報: {len(ids)}件")
    return ids

def list_raceids_from_card_lists(ymd: str, ymd_next: str) -> List[str]:
    urls=[f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd}0000000000",
          f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{ymd_next}0000000000"]
    rids=[]
    for u in urls:
        try:
            soup=BeautifulSoup(fetch(u),"lxml")
            rids.extend(_extract_raceids_from_soup(soup))
        except Exception as e:
            logging.warning(f"[WARN] 出馬表一覧スキャン失敗: {e} ({u})")
    rids=sorted(set(rids))
    logging.info(f"[INFO] Rakuten#2 出馬表一覧: {len(rids)}件")
    return rids

# ========= 窓判定 =========
def is_within_window(target_dt: datetime, now: datetime, before_min:int=WINDOW_BEFORE_MIN,
                     after_min:int=WINDOW_AFTER_MIN, grace_sec:int=GRACE_SECONDS) -> bool:
    win_start = target_dt - timedelta(minutes=before_min, seconds=grace_sec)
    win_end   = target_dt + timedelta(minutes=after_min,  seconds=grace_sec)
    return win_start <= now <= win_end

# ========= LINE =========
def push_line_text(user_id: str, token: str, text: str, timeout=8, retries=1):
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload={"to": user_id, "messages":[{"type":"text","text": text}]}
    for attempt in range(retries+1):
        try:
            resp=requests.post(LINE_PUSH_URL, headers=headers, json=payload, timeout=timeout)
            if resp.status_code==200: return True, 200, resp.text
            if resp.status_code==429 and attempt<retries:
                wait=int(resp.headers.get("Retry-After","1")); time.sleep(max(wait,1)); continue
            return False, resp.status_code, resp.text
        except requests.RequestException as e:
            if attempt<retries: time.sleep(2); continue
            return False, None, str(e)

def notify_strategy_hit_to_many(message_text: str, targets: List[str]):
    if not NOTIFY_ENABLED: logging.info("[INFO] NOTIFY_ENABLED=0"); return False, None
    if DRY_RUN: logging.info("[DRY_RUN] 通知:\n%s", message_text); return False, None
    if not LINE_ACCESS_TOKEN: logging.error("[ERROR] LINE_ACCESS_TOKEN 不足"); return False, None
    if not targets: logging.error("[ERROR] 送信先ユーザーなし"); return False, None
    all_ok=True; last=None
    for uid in targets:
        ok, status, _ = push_line_text(uid, LINE_ACCESS_TOKEN, message_text)
        last=status
        if not ok: all_ok=False
        time.sleep(0.2)
    return all_ok, last

# ========= 表示用マップ =========
def _map_pop_info(horses: List[Dict[str,float]]) -> Dict[int, Dict[str, Optional[float]]]:
    m={}
    for h in horses:
        try:
            p=int(h.get("pop"))
            num=h.get("num") if isinstance(h.get("num"), int) else None
            o=float(h.get("odds")) if h.get("odds") is not None else None
            j=h.get("jockey") or None
            m[p]={"umaban":num,"odds":o,"jockey":j}
        except: pass
    return m

def _map_umaban_info(horses: List[Dict[str,float]]) -> Dict[int, Dict[str, Optional[float]]]:
    out={}
    for h in horses:
        try:
            p=int(h.get("pop"))
            n=int(h.get("num")) if h.get("num") is not None else None
            o=float(h.get("odds")) if h.get("odds") is not None else None
            j=h.get("jockey") or None
            if n is not None: out[n]={"pop":p,"odds":o,"jockey":j}
        except: pass
    return out

# === 人気優先で表示（戦略1/2/4） ===
def _format_single_leg_prefer_pop(n:int, pop2:Dict[int,Dict], uma2:Dict[int,Dict]) -> Optional[str]:
    # まず『人気』として解釈 → 馬番へ
    pinf = pop2.get(n)
    if pinf and (pinf.get("umaban") is not None) and (pinf.get("odds") is not None):
        uma=int(pinf["umaban"]); odds=float(pinf["odds"]); jk=pinf.get("jockey")
        rank=jockey_rank_letter_by_name(jk) if jk else "—"
        return f"{uma}番（{n}人気／{odds:.1f}倍／{rank}）"
    # ダメなら『馬番』として解釈
    uinf = uma2.get(n)
    if uinf and (uinf.get("pop") is not None) and (uinf.get("odds") is not None):
        pop=int(uinf["pop"]); odds=float(uinf["odds"]); jk=uinf.get("jockey")
        rank=jockey_rank_letter_by_name(jk) if jk else "—"
        return f"{n}番（{pop}人気／{odds:.1f}倍／{rank}）"
    return None

def _format_bet_display_line(ticket: str, horses: List[Dict[str,float]], prefer:str="pop") -> str:
    nums=[int(x) for x in re.findall(r"\d+", str(ticket))]
    if not nums: return ticket
    pop2=_map_pop_info(horses); uma2=_map_umaban_info(horses)
    parts=[]
    for n in nums:
        label = (_format_single_leg_prefer_pop(n, pop2, uma2)
                 if prefer=="pop" else
                 (lambda x: None)(n))
        parts.append(label if label else str(n))
    return " - ".join(parts)

def _format_bets_umaban_odds_rank(bets: List[str], horses: List[Dict[str,float]], prefer:str="pop") -> List[str]:
    return [_format_bet_display_line(b, horses, prefer=prefer) for b in bets]

# ========= 通知本文（①②④ 共通） =========
_CIRCLED="①②③④⑤⑥⑦⑧⑨"
def _circled(n:int)->str: return _CIRCLED[n-1] if 1<=n<=9 else f"{n}."
def _extract_hhmm_label(s:str)->Optional[str]:
    got=_norm_hhmm_from_text(s)
    if not got: return None
    hh,mm,_=got; return f"{hh:02d}:{mm:02d}"
def _infer_pattern_no(strategy_text: str) -> int:
    if not strategy_text: return 0
    m=re.match(r"\s*([①-⑨])", strategy_text)
    if m: return _CIRCLED.index(m.group(1))+1
    m=re.match(r"\s*(\d+)", strategy_text)
    if m:
        try: return int(m.group(1))
        except: return 0
    return 0
def _strip_pattern_prefix(strategy_text: str) -> str:
    s=re.sub(r"^\s*[①-⑨]\s*", "", strategy_text or "")
    s=re.sub(r"^\s*\d+\s*", "", s); return s.strip()
def _split_venue_race(venue_race: str) -> Tuple[str,str]:
    if not venue_race: return "地方競馬",""
    m=re.search(r"^\s*([^\s\d]+)\s*(\d{1,2}R)\b", venue_race)
    if m:
        venue=m.group(1); race=m.group(2)
        venue_disp = f"{venue}競馬場" if "競馬" not in venue else venue
        return venue_disp, race
    return venue_race, ""

def build_line_notification(pattern_no:int, venue:str, race_no:str, time_label:str, time_hm:str,
                            condition_text:str, raw_bets:List[str], odds_timestamp_hm:Optional[str],
                            odds_url:str, horses:List[Dict[str,float]]) -> str:
    title=f"【戦略{pattern_no if pattern_no>0 else ''}該当レース発見💡】".replace("戦略該当","戦略該当")
    lines=[title, f"■レース：{venue} {race_no}（{time_label} {time_hm}）".strip()]
    if condition_text: lines.append(f"■条件：{condition_text}")
    lines+=["", "■買い目（馬番・オッズ・騎手ランク）："]
    pretty=_format_bets_umaban_odds_rank(raw_bets, horses, prefer="pop")
    for i,bet in enumerate(pretty,1): lines.append(f"{_circled(i)} {bet}")
    if odds_timestamp_hm: lines+=["", f"📅 オッズ時点: {odds_timestamp_hm}"]
    lines+=["🔗 オッズ詳細:", odds_url, ""]
    lines+=[
        "※オッズは締切直前まで変動します。",
        "※騎手ランクは2024年の地方競馬リーディングジョッキーランキングに基づき、A=1〜70位 / B=71〜200位 / C=その他。",
        "※馬券購入は余裕資金で。的中は保証されません。"
    ]
    return "\n".join(lines)

# ========= ③専用 =========
def build_line_notification_strategy3(strategy:Dict, venue:str, race_no:str, time_label:str, time_hm:str,
                                      odds_timestamp_hm:Optional[str], odds_url:str,
                                      horses:List[Dict[str,float]]) -> str:
    pop2=_map_pop_info(horses)
    axis=strategy.get("axis") or {}
    axis_pop=axis.get("pop") or 1
    axis_rank=jockey_rank_letter_by_name((pop2.get(axis_pop) or {}).get("jockey"))
    axis_uma=(pop2.get(axis_pop) or {}).get("umaban")
    axis_odds=(pop2.get(axis_pop) or {}).get("odds")
    axis_label = f"{int(axis_uma)}番（{axis_pop}人気／{float(axis_odds):.1f}倍／{axis_rank}）" if axis_uma and axis_odds else f"{axis_pop}人気（—／—／{axis_rank}）"

    cands=strategy.get("candidates")
    if not cands:
        cands=[]
        for h in sorted(horses, key=lambda x:int(x.get("pop",999))):
            try:
                p=int(h.get("pop")); o=float(h.get("odds"))
                if p==1: continue
                if 10.0<=o<=20.0:
                    cands.append({"pop":p,"jockey":h.get("jockey")})
                    if len(cands)>=4: break
            except: pass

    def _cand_label(c):
        p=c.get("pop"); pinf=pop2.get(p) or {}
        uma=pinf.get("umaban"); odds=pinf.get("odds"); rank=jockey_rank_letter_by_name(pinf.get("jockey"))
        if uma and odds is not None:
            return f"{int(uma)}番（{p}人気／{float(odds):.1f}倍／{rank}）"
        return f"{p}人気（—／—／{rank}）"
    cand_labels=[_cand_label(c) for c in sorted(cands, key=lambda x:x.get("pop",999))]

    tickets=strategy.get("tickets") or []
    pretty=_format_bets_umaban_odds_rank(tickets, horses, prefer="pop")

    title="【戦略③該当レース発見💡】"
    cond_line="1番人気 ≤2.0、2番人気 ≥10.0、相手＝単勝10〜20倍（最大4頭）"
    n=len(cand_labels); pts=n*(n-1) if n>=2 else 0

    lines=[title,
           f"■レース：{venue} {race_no}（{time_label} {time_hm}）",
           f"■条件：{cond_line}",
           f"■軸：{axis_label}",
           f"■相手候補：{', '.join(cand_labels) if cand_labels else '—'}",
           f"■買い目（3連単・1着固定）：{', '.join(pretty) if pretty else '—'}",
           f"  → 候補 {n}頭／合計 {pts}点"
    ]
    if odds_timestamp_hm: lines += [f"\n📅 オッズ時点: {odds_timestamp_hm}"]
    lines += ["🔗 オッズ詳細:", odds_url, ""]
    lines += [
        "※オッズは締切直前まで変動します。",
        "※騎手ランクは2024年の地方競馬リーディングジョッキーランキングに基づき、A=1〜70位 / B=71〜200位 / C=その他。",
        "※馬券購入は余裕資金で。的中は保証されません。"
    ]
    return "\n".join(lines)

# ========= ベット記録 =========
def _bets_sheet_header() -> List[str]:
    return ["date","race_id","venue","race_no","strategy","bet_kind","tickets_umaban_csv","points","unit_stake","total_stake"]

def sheet_append_bet_record(date_ymd:str, race_id:str, venue:str, race_no:str,
                            strategy_no:int, bet_kind:str, tickets_umaban:List[str]):
    svc=_sheet_service(); title=_resolve_sheet_title(svc, BETS_SHEET_TAB)
    values=_sheet_get_range_values(svc, title, "A:J")
    if not values: values=[_bets_sheet_header()]
    points=len(tickets_umaban); unit=UNIT_STAKE_YEN; total=points*unit
    values.append([date_ymd, race_id, venue, race_no, str(strategy_no), bet_kind, ",".join(tickets_umaban), str(points), str(unit), str(total)])
    _sheet_update_range_values(svc, title, "A:J", values)

# ========= 払戻取得＆日次サマリ =========
_PAYOUT_KIND_KEYS = ["単勝","複勝","枠連","馬連","ワイド","馬単","三連複","三連単"]

def fetch_payoff_map(race_id:str) -> Dict[str, List[Tuple[str,int]]]:
    url=f"https://keiba.rakuten.co.jp/race/payoff/RACEID/{race_id}"
    html=fetch(url); soup=BeautifulSoup(html,"lxml")
    result={}
    for kind in _PAYOUT_KIND_KEYS:
        blocks=soup.find_all(string=re.compile(kind))
        items=[]
        for b in blocks:
            box=getattr(b,"parent",None) or soup
            text=" ".join((box.get_text(" ", strip=True) or "").split())
            for m in re.finditer(r"(\d+(?:-\d+){0,2})\s*([\d,]+)\s*円", text):
                comb=m.group(1); pay=int(m.group(2).replace(",",""))
                items.append((comb, pay))
        if items: result[kind]=items
    return result

def _normalize_ticket_for_kind(ticket:str, kind:str) -> str:
    parts=[int(x) for x in ticket.split("-") if x.strip().isdigit()]
    if kind in ("馬連","三連複"): parts=sorted(parts)
    return "-".join(str(x) for x in parts)

def _summary_key_for_today() -> str:
    return f"summary:{now_jst():%Y%m%d}"

def _is_time_reached(now: datetime, hhmm: str) -> bool:
    """指定hh:mm（JST）に到達済みか。>= で判定（ジョブが遅れても1回は動く）"""
    try: hh,mm=map(int, hhmm.split(":"))
    except Exception: return False
    target=now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    return now >= target

def summarize_today_and_notify(targets: List[str]):
    """当日betsからサマリを作成し通知。払戻ページ取得失敗は個別に握りつぶして継続。"""
    logging.info("[SUMMARY] 本日サマリ作成を開始します")

    svc=_sheet_service(); title=_resolve_sheet_title(svc, BETS_SHEET_TAB)
    values=_sheet_get_range_values(svc, title, "A:J")
    if not values or values==[_bets_sheet_header()]:
        if not ALWAYS_NOTIFY_DAILY_SUMMARY:
            logging.info("[SUMMARY] betsシートに当日データなし（無通知モード）")
            return
        values=[_bets_sheet_header()]

    hdr=values[0]; rows=values[1:]
    today=now_jst().strftime("%Y%m%d")
    records=[r for r in rows if len(r)>=10 and r[0]==today]

    if not records and not ALWAYS_NOTIFY_DAILY_SUMMARY:
        logging.info("[SUMMARY] 当日分なし（無通知モード）")
        return

    per_strategy = { k:{"races":0,"hits":0,"bets":0,"stake":0,"return":0} for k in ("1","2","3","4") }
    seen_race_strategy:set[Tuple[str,str]] = set()

    for r in records:
        date_ymd, race_id, venue, race_no, strategy, bet_kind, t_csv, points, unit, total = r[:10]
        if (race_id, strategy) not in seen_race_strategy:
            seen_race_strategy.add((race_id, strategy))
            per_strategy[strategy]["races"] += 1
        tickets=[t for t in t_csv.split(",") if t]
        per_strategy[strategy]["bets"]  += len(tickets)
        per_strategy[strategy]["stake"] += int(total)

        # 払戻ページ取得はレースごとに保護
        try:
            paymap=fetch_payoff_map(race_id)
        except Exception as e:
            logging.warning("[SUMMARY] 払戻取得失敗 rid=%s: %s（このレースはスキップ）", race_id, e)
            continue

        winners={ _normalize_ticket_for_kind(comb, bet_kind): pay for (comb, pay) in paymap.get(bet_kind, []) }
        for t in tickets:
            norm=_normalize_ticket_for_kind(t, bet_kind)
            if norm in winners:
                per_strategy[strategy]["hits"]   += 1
                per_strategy[strategy]["return"] += winners[norm]
        time.sleep(0.2)

    total_stake=sum(v["stake"] for v in per_strategy.values())
    total_return=sum(v["return"] for v in per_strategy.values())
    def pct(n,d): return f"{(100.0*n/d):.1f}%" if d>0 else "0.0%"

    lines=["📊【本日の検証結果】", f"日付：{today[:4]}/{today[4:6]}/{today[6:]}", ""]
    for k in ("1","2","3","4"):
        v=per_strategy[k]
        hit_rate=pct(v["hits"], max(v["bets"],1))
        roi=pct(v["return"], max(v["stake"],1))
        lines.append(f"戦略{k}：該当{v['races']}レース / 購入{v['bets']}点 / 的中{v['hits']}点")
        lines.append(f"　　　的中率 {hit_rate} / 回収率 {roi}")
    lines.append("")
    lines.append(f"合計：投資 {total_stake:,}円 / 払戻 {total_return:,}円 / 回収率 {pct(total_return, max(total_stake,1))}")

    msg="\n".join(lines)
    ok, status = notify_strategy_hit_to_many(msg, targets)
    if ok:
        logging.info("[SUMMARY] サマリ通知を送信しました（HTTP %s）", status)
    else:
        logging.error("[SUMMARY] サマリ通知の送信に失敗しました（HTTP %s）", status)

# ========= 監視本体（一回実行） =========
def _tickets_pop_to_umaban(bets: List[str], horses: List[Dict[str,float]]) -> List[str]:
    """bets（人気 or 馬番混在）→ 馬番列へ変換（betsシート保存用）"""
    pop2=_map_pop_info(horses); out=[]
    for b in bets:
        nums=[int(x) for x in re.findall(r"\d+", str(b))]
        if not nums: out.append(b); continue
        res=[]; ok=True
        for n in nums:
            # 馬番として既存ならそのまま
            if any((isinstance(h.get("num"), int) and int(h.get("num"))==n) for h in horses):
                res.append(str(n)); continue
            # 人気→馬番
            u=(pop2.get(n) or {}).get("umaban")
            if u is None: ok=False; break
            res.append(str(u))
        out.append("-".join(res) if ok else b)
    return out

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    p=pathlib.Path(__file__).resolve()
    sha=hashlib.sha1(p.read_bytes()).hexdigest()[:12]
    logging.info(f"[BUILD] file={p} sha1={sha} v2025-08-15E")

    if KILL_SWITCH:
        logging.info("[INFO] KILL_SWITCH=True"); return

    # 送信ターゲット
    try:
        targets=load_user_ids_from_simple_col()
        if not targets:
            fb=LINE_USER_IDS if LINE_USER_IDS else ([LINE_USER_ID] if LINE_USER_ID else [])
            targets=fb
    except Exception as e:
        logging.exception("[ERROR] usersシート読込失敗: %s", e)
        fb=LINE_USER_IDS if LINE_USER_IDS else ([LINE_USER_ID] if LINE_USER_ID else [])
        targets=fb
    logging.info("[INFO] 送信先=%d", len(targets))

    # 稼働時間内で通常監視
    if within_operating_hours():
        try:
            notified=sheet_load_notified()
        except Exception as e:
            logging.exception("[ERROR] TTLロード失敗: %s", e)
            notified={}
        if DEBUG_RACEIDS:
            target_raceids=[rid for rid in DEBUG_RACEIDS if not PLACEHOLDER.search(rid)]
            post_time_map={}
        else:
            ymd=now_jst().strftime("%Y%m%d")
            ymd_next=(now_jst()+timedelta(days=1)).strftime("%Y%m%d")
            r1=list_raceids_today_ticket(ymd)
            r2=list_raceids_from_card_lists(ymd, ymd_next)
            target_raceids=sorted(set(r1)|set(r2))
            post_time_map=collect_post_time_map(ymd, ymd_next)
            target_raceids=[rid for rid in target_raceids if not PLACEHOLDER.search(rid)]

        hits=0; matches=0
        seen_in_this_run:set[str]=set()

        for rid in target_raceids:
            if rid in seen_in_this_run: continue
            now_ts=time.time()
            cd_ts=notified.get(f"{rid}:cd")
            if cd_ts and (now_ts-cd_ts)<NOTIFY_COOLDOWN_SEC: continue
            ts=notified.get(rid)
            if ts and (now_ts-ts)<NOTIFY_TTL_SEC: continue

            # 発走（基準）取得
            post_time=post_time_map.get(rid)
            if not post_time:
                got=fallback_post_time_for_rid(rid)
                if got: post_time, _, _ = got
                else: 
                    logging.info("[TRACE] time rid=%s result=SKIP reason=no_post_time", rid)
                    continue

            # 締切取得（最優先）
            cutoff_info=resolve_cutoff_dt(rid) if CUTOFF_OFFSET_MIN>0 else None
            if cutoff_info:
                cutoff_dt, cutoff_src = cutoff_info
                used_dt = cutoff_dt
                time_label = "締切"
                src_label  = f"cutoff:{cutoff_src}"
            else:
                used_dt = post_time - timedelta(minutes=CUTOFF_OFFSET_MIN) if CUTOFF_OFFSET_MIN>0 else post_time
                time_label = "締切" if CUTOFF_OFFSET_MIN>0 else "発走"
                src_label  = "post-offset" if CUTOFF_OFFSET_MIN>0 else "post"

            now=now_jst()
            in_win=is_within_window(used_dt, now)
            logging.info("[TRACE] time rid=%s use=%s src=%s at=%s target=%s Δsec=%.1f in_window=%s",
                         rid, time_label, src_label, now.strftime("%H:%M:%S"), used_dt.strftime("%H:%M"),
                         (used_dt-now).total_seconds(), in_win)
            if not in_win: continue

            meta=check_tanfuku_page(rid)
            if not meta:
                logging.info("[TRACE] odds rid=%s result=SKIP reason=no_table", rid)
                time.sleep(random.uniform(*SLEEP_BETWEEN)); continue

            horses=meta["horses"]
            if len(horses)<4:
                logging.info("[TRACE] odds rid=%s result=SKIP reason=too_few_horses len=%d", rid, len(horses))
                time.sleep(random.uniform(*SLEEP_BETWEEN)); continue

            # オッズスナップ
            top3=sorted(horses, key=lambda x:int(x.get("pop",999)))[:3]
            snapshot=[(int(h.get("pop",0)), float(h.get("odds",0.0))) for h in top3 if "pop" in h and "odds" in h]
            logging.info("[TRACE] odds_top3 rid=%s %s", rid, snapshot)

            hits+=1
            strategy=eval_strategy(horses, logger=logging)
            if not strategy:
                logging.info("[TRACE] judge rid=%s result=FAIL reason=no_strategy_match", rid)
                time.sleep(random.uniform(*SLEEP_BETWEEN)); continue
            matches+=1
            logging.info("[TRACE] judge rid=%s result=PASS strat=%s", rid, strategy.get("strategy",""))

            strategy_text=strategy.get("strategy","")
            pattern_no=_infer_pattern_no(strategy_text)
            condition_text=_strip_pattern_prefix(strategy_text) or strategy_text

            venue_disp, race_no=_split_venue_race(meta.get("venue_race",""))
            display_dt=used_dt
            time_hm=display_dt.strftime("%H:%M")
            odds_hm=_extract_hhmm_label(meta.get("now",""))

            raw_tickets=strategy.get("tickets", [])
            if isinstance(raw_tickets, str):
                raw_tickets=[s.strip() for s in raw_tickets.split(",") if s.strip()]

            # 通知本文
            if str(strategy_text).startswith("③"):
                message=build_line_notification_strategy3(strategy, venue_disp, race_no, time_label, time_hm, odds_hm, meta["url"], horses)
                tickets_umaban = strategy.get("tickets", [])  # ③はumaban生成のことが多い
                bet_kind = STRATEGY_BET_KIND.get("3", "三連単")
            else:
                message=build_line_notification(pattern_no, venue_disp, race_no, time_label, time_hm, condition_text, raw_tickets, odds_hm, meta["url"], horses)
                tickets_umaban=_tickets_pop_to_umaban(raw_tickets, horses)
                bet_kind = STRATEGY_BET_KIND.get(str(pattern_no), "三連単")

            # 送信
            sent_ok, http_status = notify_strategy_hit_to_many(message, targets)

            # 通知ログ（送信成功時のみ）
            if sent_ok:
                try:
                    append_notify_log({
                        'date_jst': jst_today_str(),
                        'race_id': rid,
                        'strategy': str(pattern_no),
                        'stake': len(tickets_umaban) * UNIT_STAKE_YEN,
                        'bets_json': json.dumps(tickets_umaban, ensure_ascii=False),
                        'notified_at': jst_now(),
                        'jockey_ranks': "/".join([
                            jockey_rank_letter_by_name(h.get("jockey")) if h.get("jockey") else "—"
                            for h in horses[:3]
                        ]),
                    })
                except Exception as e:
                    logging.exception("[WARN] append_notify_log失敗: %s", e)

            now_epoch=time.time()
            if sent_ok:
                try:
                    sheet_upsert_notified(rid, now_epoch, note=f"{meta['venue_race']} {display_dt:%H:%M} {src_label}")
                except Exception as e:
                    logging.exception("[ERROR] TTL更新失敗: %s", e)
                seen_in_this_run.add(rid)
                try:
                    ymd=now_jst().strftime("%Y%m%d")
                    sheet_append_bet_record(ymd, rid, venue_disp, race_no, pattern_no, bet_kind, tickets_umaban or [])
                except Exception as e:
                    logging.exception("[ERROR] bets記録失敗: %s", e)
            elif http_status==429:
                try:
                    key_cd=f"{rid}:cd"; sheet_upsert_notified(key_cd, now_epoch, note=f"429 cooldown {meta['venue_race']} {display_dt:%H:%M}")
                except Exception as e:
                    logging.exception("[ERROR] CD更新失敗: %s", e)

            time.sleep(random.uniform(*SLEEP_BETWEEN))

        logging.info(f"[INFO] HITS={hits} / MATCHES={matches}")

    # === 日次サマリ：指定時刻に1日1回 ===
    try:
        now = now_jst()
        if _is_time_reached(now, DAILY_SUMMARY_HHMM):
            notified = {}
            try:
                notified = sheet_load_notified()
            except Exception as e:
                logging.warning("[SUMMARY] 通知フラグの読込に失敗: %s（続行）", e)
            skey = _summary_key_for_today()
            if skey not in notified:
                logging.info("[SUMMARY] トリガ時刻到達（%s）。本日のサマリを送信します。", DAILY_SUMMARY_HHMM)
                try:
                    summarize_today_and_notify(targets)
                except Exception as e:
                    # ここで握りつぶさないと「判定に失敗」で全体が終了して再送機会を失う
                    logging.exception("[SUMMARY] サマリ作成/送信中に未捕捉例外: %s", e)
                    # 失敗時はフラグを書かずに次回以降の再試行に委ねる
                else:
                    try:
                        sheet_upsert_notified(skey, time.time(), note=f"daily summary {now:%H:%M}")
                        logging.info("[SUMMARY] サマリ送信フラグを保存しました（%s）。", skey)
                    except Exception as e:
                        logging.exception("[SUMMARY] サマリ送信フラグの保存に失敗: %s", e)
            else:
                logging.info("[SUMMARY] 本日は既にサマリ送信済み（key=%s）。スキップします。", skey)
    except Exception as e:
        logging.exception("[ERROR] 日次サマリ送信判定に失敗: %s", e)

    logging.info("[INFO] ジョブ終了")

# ========= 常駐ループ =========
def run_watcher_forever(interval_sec: int = int(os.getenv("WATCHER_INTERVAL_SEC", "60"))):
    logging.info(f"[BOOT] run_watcher_forever(interval={interval_sec}s)")
    while True:
        try:
            main()
        except Exception as e:
            logging.exception("[FATAL] watcherループ例外: %s", e)
        time.sleep(interval_sec)