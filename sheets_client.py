# sheets_client.py
import json
import os
import time
import logging
from typing import List, Dict, Any, Optional

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
RECIPIENTS_TAB = os.getenv("GOOGLE_SHEET_TAB", "フォームの回答1")
SENT_LOG_TAB = os.getenv("SENT_LOG_TAB", "sent_log")

def _build_service():
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "").strip()
    if not creds_json:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS_JSON env is empty")
    info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def fetch_recipients() -> List[Dict[str, Any]]:
    """
    受信者シート（フォーム出力想定）から有効な userId を取り出す。
    想定列例:
      - 有効フラグ: enabled（'1' または TRUE）/ 無しは無効
      - LINEのユーザーID: userId
    既存シート列名に合わせて調整してください。
    """
    try:
        service = _build_service()
        rng = f"'{RECIPIENTS_TAB}'!A1:Z1000"
        res = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=rng).execute()
        values = res.get("values", [])
        if not values:
            return []
        header = [c.strip() for c in values[0]]
        out = []
        for row in values[1:]:
            rec = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
            enabled = str(rec.get("enabled", "")).strip().lower() in ("1","true","yes","on")
            uid = str(rec.get("userId", "")).strip()
            out.append({"enabled": enabled, "userId": uid})
        return out
    except Exception as e:
        logging.warning("fetch_recipients failed: %s", e)
        return []

def ensure_sent_log_sheet() -> None:
    """ sent_log タブが無ければ作成し、ヘッダを書く """
    try:
        service = _build_service()
        meta = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        sheets = [s["properties"]["title"] for s in meta.get("sheets", [])]
        if SENT_LOG_TAB not in sheets:
            # 追加
            body = {
                "requests": [{
                    "addSheet": {"properties": {"title": SENT_LOG_TAB}}
                }]
            }
            service.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body=body).execute()
            # ヘッダ
            header = [["sent_key", "race_id", "strategy", "sale_close_iso", "created_at"]]
            service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"'{SENT_LOG_TAB}'!A1:E1",
                valueInputOption="RAW",
                body={"values": header}
            ).execute()
    except HttpError as he:
        logging.warning("ensure_sent_log_sheet http error: %s", he)
    except Exception as e:
        logging.warning("ensure_sent_log_sheet failed: %s", e)

def already_sent(sent_key: str) -> bool:
    """ sent_log に同じ sent_key があるか """
    try:
        service = _build_service()
        rng = f"'{SENT_LOG_TAB}'!A2:A100000"
        res = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=rng).execute()
        vals = res.get("values", [])
        for row in vals:
            if row and row[0].strip() == sent_key:
                return True
        return False
    except Exception as e:
        logging.warning("already_sent failed (treat as not sent): %s", e)
        return False

def append_sent_log(race_id: str, strategy: str, sale_close_iso: str, sent_key: str) -> None:
    """ 送信後に1行追加 """
    try:
        service = _build_service()
        now = time.strftime("%Y-%m-%dT%H:%M:%S")
        row = [[sent_key, race_id, strategy, sale_close_iso, now]]
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"'{SENT_LOG_TAB}'!A:E",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": row}
        ).execute()
    except Exception as e:
        logging.warning("append_sent_log failed: %s", e)