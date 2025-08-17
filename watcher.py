# -*- coding: utf-8 -*-
"""
Rakuten競馬 監視・通知バッチ（完全差し替え版 v2025-08-17F2）
- 締切時刻：単複オッズ/一覧ページから“締切”を直接抽出（最優先）
- 発走時刻：一覧ページ優先＋オッズ詳細のフォールバック
- 窓判定：ターゲット時刻（締切 or 発走）基準、±GRACE_SECONDS の許容
- 通知：窓内1回 / 429時はクールダウン / Google SheetでTTL永続
- 送信先：Googleシート(タブA=名称「1」)のH列から userId を収集
- 戦略③：専用フォーマット（1軸・相手10〜20倍・候補最大4頭・点数表示）
- 騎手ランク：内蔵200位＋表記ゆれ耐性（強化クレンジング＋前方一致＋姓一致フォールバック）
- ★通知本文：買い目を「馬番＋オッズ＋騎手ランク」で表示
- 未一致の騎手名は [RANKMISS] ログに記録（重複抑止）＋ [RANKDBG] で突合過程を出力
- betsシート：馬番ベースで記録
- 日次サマリ：JST 21:02 に当日1回だけ送信（0件でも送信）
- 券種は STRATEGY_BET_KIND_JSON で設定（既定: ①馬連, ②馬単, ③三連単, ④三連複）

★締切基準で運用する場合：
  - CUTOFF_OFFSET_MIN=5（推奨）
  - “締切そのもの” を抽出できたら採用。取れない場合のみ「発走-5分」を代用。

★本版の主なFix：
  - GoogleSheet TTL関数（sheet_load_notified/sheet_upsert_notified）が未定義の環境に追補
  - 騎手名のクレンジングを強化（「牡3栗毛…」等の馬情報混入を除去）
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
try:
    from utils_summary import jst_today_str, jst_now
except ModuleNotFoundError:
    # 最低限の代替（タイムゾーンJST）
    JST = timezone(timedelta(hours=9))
    def jst_today_str() -> str: return datetime.now(JST).strftime("%Y%m%d")
    def jst_now() -> str: return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

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
CUTOFF_OFFSET_MIN   = int(os.getenv("CUTOFF_OFFSET_MIN", "0"))  # 例: 5
FORCE_RUN           = os.getenv("FORCE_RUN", "0") == "1"
GRACE_SECONDS       = int(os.getenv("GRACE_SECONDS", "60"))

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
DAILY_SUMMARY_HHMM = os.getenv("DAILY_SUMMARY_HHMM", "21:02")
ALWAYS_NOTIFY_DAILY_SUMMARY = os.getenv("ALWAYS_NOTIFY_DAILY_SUMMARY", "1") == "1"

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

# ========= 騎手ランク =========
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

def _clean_jockey_name(s: str) -> str:
    """
    括弧/斤量/印/接尾辞(J/Ｊ/騎手)・
    先頭の性齢/毛色など馬の情報を除去し、素の “騎手名” のみを返す
    """
    if not s:
        return ""
    t = s

    # 括弧や印、斤量表記の除去
    t = re.sub(r"[（(].*?[）)]", "", t)
    t = re.sub(r"[▲△☆★◇◆⊙◎○◯◉⚪︎＋+＊*]", "", t)
    t = re.sub(r"\d+(?:\.\d+)?\s*(?:kg|斤)?", "", t)
    t = t.replace("斤量", "")
    t = t.replace("騎手", "").replace("J", "").replace("Ｊ", "")

    # 先頭に混入する “牡/牝/騙/セ” “○歳/才” “毛色” を除去
    colors = "鹿毛|栗毛|芦毛|黒鹿毛|青鹿毛|青毛|白毛|栃栗毛|青鹿|黒鹿|栗|芦"
    t = re.sub(rf"^\s*(牡|牝|騙|セ)\s*\d*\s*({colors})\s*", "", t)
    t = re.sub(rf"^\s*(牡|牝|騙|セ)\s*({colors})\s*", "", t)
    t = re.sub(rf"^\s*({colors})\s*", "", t)
    t = re.sub(r"^\s*\d+\s*(歳|才)\s*", "", t)

    # スペース系
    t = re.sub(r"\s+", "", t)
    return t
    
def _load_jockey_ranks_from_env() -> Dict[int, str]:
    raw = os.getenv("JOCKEY_RANKS_JSON", "")
    if not raw:
        logging.warning("[WARN] JOCKEY_RANKS_JSON が未設定です（全員C扱い）")
        return {}
    # ダッシュボードに '...' / "..." で貼った時の外側クォートを剥がす
    raw = raw.strip()
    if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
        raw = raw[1:-1]
    try:
        obj = json.loads(raw)
    except Exception as e:
        logging.exception("[WARN] JOCKEY_RANKS_JSON の JSON 解析に失敗: %s", e)
        return {}
    out: Dict[int, str] = {}
    for k, v in obj.items():
        try:
            out[int(k)] = str(v)
        except Exception:
            continue
    return out

# ① rank->name をENVから構築
JOCKEY_RANK_TABLE_RAW: Dict[int, str] = _load_jockey_ranks_from_env()

# ② 正規化名 -> 最上位(=数値ランクが最小) を逆引き
_name_to_best_rank: Dict[str, int] = {}
for rk, name in JOCKEY_RANK_TABLE_RAW.items():
    norm = _normalize_name(name)
    if not norm:
        continue
    if norm not in _name_to_best_rank or rk < _name_to_best_rank[norm]:
        _name_to_best_rank[norm] = rk

# ③ 他の処理が参照する逆引きマップ
_JOCKEY_NAME_TO_RANK: Dict[str, int] = _name_to_best_rank

def jockey_rank_letter_by_name(name_raw: Optional[str]) -> str:
    """A(1-70) / B(71-200) / C(その他)"""
    if not name_raw:
        return "C"
    norm = _normalize_name(_clean_jockey_name(str(name_raw)))
    rank = _JOCKEY_NAME_TO_RANK.get(norm)
    if rank is None:
        rank = _best_match_rank(norm)
        if rank is None:
            _log_rank_miss(name_raw, norm)
            return "C"
    return "A" if rank <= 70 else ("B" if rank <= 200 else "C")

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

# ====== TTL(通知済み) の読込/更新  ←★追補（NameError対策）======
def sheet_load_notified() -> Dict[str, float]:
    svc=_sheet_service(); title=_resolve_sheet_title(svc, GOOGLE_SHEET_TAB)
    values=_sheet_get_range_values(svc, title, "A:C")
    start = 1 if values and values[0] and str(values[0][0]).upper() in ("KEY","RACEID","RID","ID") else 0
    d={}
    for row in values[start:]:
        if not row or len(row)<2: 
            continue
        key=str(row[0]).strip()
        try:
            d[key]=float(row[1])
        except:
            pass
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
        if row and str(row[0]).strip()==key: 
            found=i; break
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

# ========= 以下：フォールバック/通知/サマリ/メイン =========
# （この先はあなたの現在版と同一です。長文のためそのまま貼り替えてください）

# ……（中略せずにお使いの最新版の
# fallback_post_time_for_rid / list_raceids_* / is_within_window /
# push_line_text / notify_strategy_hit_to_many / 表示用マップ類 /
# build_line_notification / build_line_notification_strategy3 /
# betsシート処理 / 払戻取得と summarize_today_and_notify /
# main() / run_watcher_forever() まで）……