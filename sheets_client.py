# sheets_client.py
import os
import json
import logging
from typing import List, Dict, Any

import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

def _build_creds_from_env() -> Credentials:
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not raw:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON env is empty")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        # 1行圧縮して貼った/改行ありなどどちらでもOK。JSONとして不正ならここで落とす
        raise RuntimeError(f"Invalid GOOGLE_CREDENTIALS_JSON: {e}")

    return Credentials.from_service_account_info(data, scopes=SCOPES)

def get_worksheet():
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    tab_name = os.getenv("GOOGLE_SHEET_TAB", "").strip() or "フォームの回答 1"
    if not sheet_id:
        raise RuntimeError("GOOGLE_SHEET_ID env is empty")

    creds = _build_creds_from_env()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    return ws

def fetch_recipients() -> List[Dict[str, Any]]:
    """
    スプレッドシートから受信者リストを取得。
    期待する列例：
      userId / 氏名 / LINEアイコン名 / 有効(はい/いいえ) / プラン / 夜レポ(はい/いいえ)
    ※列名は実シートに合わせて柔軟に読む（ヘッダー行をそのままキーに）
    """
    try:
        ws = get_worksheet()
        rows = ws.get_all_records()  # 1行目をヘッダーとして辞書化
    except Exception as e:
        logging.warning("fetch_recipients failed: %s", e)
        return []

    recipients = []
    for r in rows:
        # userId が空ならスキップ
        uid = str(r.get("userId") or r.get("ユーザーID") or "").strip()
        if not uid:
            continue

        # 有効/無効フラグ（列名はあなたのシートに合わせて変更）
        enabled_val = str(r.get("有効") or r.get("Enabled") or "はい").strip()
        enabled = enabled_val in ("はい", "true", "True", "TRUE", "有効", "1")

        recipients.append({
            "userId": uid,
            "name": r.get("氏名") or r.get("名前") or "",
            "icon_name": r.get("LINEアイコン名") or "",
            "enabled": enabled,
            "plan": r.get("プラン") or r.get("Plan") or "",
            "night_report": str(r.get("夜レポ") or "").strip() in ("はい", "true", "True", "1"),
            # 必要に応じて項目を増やす
            "_raw": r,
        })
    return recipients