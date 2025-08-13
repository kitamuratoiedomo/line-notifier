# -*- coding: utf-8 -*-
import os
import json
import logging
from typing import Dict, List
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
CRED_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
TAB_NAME = os.getenv("NOTIFY_LOG_SHEET_TAB", "notify_log")
_HEADER = ["date_jst", "race_id", "strategy", "stake", "bets_json", "notified_at", "jockey_ranks"]

def _sheet_service():
    if not SHEET_ID or not CRED_JSON:
        raise RuntimeError("notify_log: 環境変数不足")
    info = json.loads(CRED_JSON)
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def _resolve_sheet_title(svc, title: str) -> str:
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == title:
            return title
    svc.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body={
        "requests": [{"addSheet": {"properties": {"title": title}}}]
    }).execute()
    return title

def _get_values(svc, title: str, a1: str) -> List[List[str]]:
    res = svc.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=f"'{title}'!{a1}").execute()
    return res.get("values", [])

def _put_values(svc, title: str, a1: str, values: List[List[str]]):
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID, range=f"'{title}'!{a1}",
        valueInputOption="RAW", body={"values": values}
    ).execute()

def append_notify_log(payload: Dict) -> None:
    try:
        svc = _sheet_service()
    except Exception as e:
        logging.warning(f"[notify_log] Sheets初期化失敗: {e}")
        return
    title = _resolve_sheet_title(svc, TAB_NAME)
    values = _get_values(svc, title, "A:G")
    if not values:
        values = [_HEADER]
    row = [str(payload.get(k, "")) for k in _HEADER]
    values.append(row)
    try:
        _put_values(svc, title, "A:G", values)
        logging.info("[notify_log] 追記OK")
    except Exception as e:
        logging.exception("[notify_log] 書込失敗: %s", e)