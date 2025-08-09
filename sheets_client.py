# sheets_client.py
import os, json, logging
from typing import List, Dict, Any

# google client
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def _load_sa_info() -> Dict[str, Any]:
    """
    サービスアカウントJSONを環境変数から読み込む。
    GOOGLE_CREDENTIALS_JSON か GOOGLE_APPLICATION_CREDENTIALS_JSON のどちらでもOK。
    """
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON") or ""
    raw = raw.strip()
    if not raw:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON env is empty")
    try:
        # RenderのUIで改行を \n として入れている前提
        return json.loads(raw)
    except Exception as e:
        # もし誤って実ファイルパスを入れてしまった時も拾えるように
        if os.path.exists(raw):
            with open(raw, "r", encoding="utf-8") as f:
                return json.load(f)
        raise RuntimeError(f"Invalid SA JSON in env: {e!r}")

def _build_service():
    info = _load_sa_info()
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    # cache_discovery=False で App Engine / serverless の不具合を回避
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def _pick(value: str) -> str:
    return (value or "").strip()

def _to_bool(s: str) -> bool:
    s = (s or "").strip().lower()
    return s in ("1","true","on","yes","有効","通知on","通知オン","○")

def fetch_recipients() -> List[Dict[str, Any]]:
    """
    受信者一覧をスプレッドシートから取得して標準化して返す。
    想定するヘッダはいずれか（列名は大小/全角半角ゆるめ判定）:

    A) シンプル表:
       name | userId | enabled
    B) Googleフォーム出力っぽい表:
       タイムスタンプ | 表示名 | LINEユーザーID | 通知ON

    返却: [{ "name": "...", "userId": "...", "enabled": True/False }, ...]
    """
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    tab = os.getenv("GOOGLE_SHEET_TAB", "").strip()
    if not sheet_id or not tab:
        raise RuntimeError("GOOGLE_SHEET_ID or GOOGLE_SHEET_TAB missing")

    service = _build_service()
    rng = f"'{tab}'!A:Z"
    resp = service.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=rng
    ).execute()

    values = resp.get("values", [])
    if not values:
        return []

    header = [str(h).strip() for h in values[0]]
    body = values[1:]

    # 列インデックス探索（ゆるふわマッチ）
    def idx(keys):
        for i, h in enumerate(header):
            hs = h.replace(" ", "").lower()
            for k in keys:
                if k in hs:
                    return i
        return -1

    # A) シンプル系
    i_name   = idx(["name","表示名"])
    i_user   = idx(["userid","user_id","lineユーザーid","lineユーザid","lineid"])
    i_enable = idx(["enabled","有効","通知on","通知オン"])

    # どれか足りなければ、フォームっぽい想定で再探査
    if i_user < 0:
        # よくあるフォーム列名
        i_user = idx(["lineユーザーid","lineユーザid","userid","lineid"])

    out = []
    for row in body:
        # 配列長が足りないセルは空に
        name   = _pick(row[i_name])   if 0 <= i_name   < len(row) else ""
        userId = _pick(row[i_user])   if 0 <= i_user   < len(row) else ""
        enabled_s = _pick(row[i_enable]) if 0 <= i_enable < len(row) else ""

        # enabled 列が存在しなければ既定 True（フォームでスイッチ置かない運用向け）
        enabled = _to_bool(enabled_s) if i_enable >= 0 else True

        if not userId:
            continue
        out.append({"name": name or userId, "userId": userId, "enabled": enabled})

    logging.info("シートから有効受信者=%d: %s",
                 sum(1 for r in out if r["enabled"]),
                 [r["userId"] for r in out if r["enabled"]])
    return out

if __name__ == "__main__":
    # 手元確認用
    try:
        rs = fetch_recipients()
        print(rs)
    except Exception as e:
        logging.exception("fetch_recipients failed: %r", e)
        raise