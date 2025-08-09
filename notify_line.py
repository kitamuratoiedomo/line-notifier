# notify_line.py
import os
import re
import time
import json
import logging
from typing import Iterable, Tuple, Dict, Any, List

import requests

LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"
LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()

# ---- サニタイズ -------------------------------------------------------------
_TAG_RE = re.compile(r"<[^>]+>")  # 簡易にHTMLタグを除去

def _sanitize_text(text: str, max_len: int = 4900) -> str:
    """
    LINEのテキストメッセージに安全なプレーンテキストへ整形。
    - HTMLタグ/アンカー等を除去
    - 制御文字を最低限クリーニング
    - 長すぎる本文は末尾を省略（LINEの上限は5000文字）
    """
    if text is None:
        return ""
    # 改行の正規化
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    # HTMLタグ除去（<a href=...> などを殺す）
    t = _TAG_RE.sub("", t)
    # 余計な連続空白の整理（見た目を整える）
    t = re.sub(r"[ \t]{2,}", " ", t)
    # 先頭末尾の空白
    t = t.strip()
    # 長さ制限（安全マージン）
    if len(t) > max_len:
        t = t[:max_len] + " …"
    return t

# ---- 送信本体 ----------------------------------------------------------------
def _build_headers() -> Dict[str, str]:
    if not LINE_ACCESS_TOKEN:
        raise RuntimeError("LINE_ACCESS_TOKEN env is empty")
    return {
        "Authorization": f"Bearer {LINE_ACCESS_TOKEN}",
        "Content-Type": "application/json; charset=utf-8",
    }

def _push_once(user_id: str, text: str) -> Tuple[bool, str]:
    """
    単一ユーザに1通送信。成功/失敗とメッセージを返す。
    """
    payload = {
        "to": user_id,
        "messages": [{"type": "text", "text": text}],
    }
    try:
        r = requests.post(LINE_PUSH_URL, headers=_build_headers(),
                          data=json.dumps(payload), timeout=10)
        if r.status_code == 200:
            return True, "ok"
        return False, f"{r.status_code} {r.text}"
    except Exception as e:
        return False, f"exc:{e}"

def _send_with_retry(user_id: str, text: str,
                     retries: int = 2, backoff: float = 0.8) -> Tuple[bool, str]:
    """
    429/5xx を想定して軽いリトライ。指数バックオフ。
    """
    ok, msg = _push_once(user_id, text)
    attempt = 0
    while (not ok) and attempt < retries:
        attempt += 1
        # レート制限や一時故障らしき場合のみ待って再送
        if any(k in msg for k in ("429", "500", "502", "503", "504")):
            time.sleep(backoff * (2 ** (attempt - 1)))
            ok, msg = _push_once(user_id, text)
        else:
            break
    return ok, msg

# ---- 公開API -----------------------------------------------------------------
def send_line(recipients: Any, text: str) -> Dict[str, Any]:
    """
    LINE 送信ユーティリティ（公開関数）
    - recipients: str（userId）または userId の反復可能（list/tupleなど）
    - text: 送信本文（プレーン推奨。HTMLは内部で除去）
    戻り値: {ok: int, ng: int, fails: [{user_id, reason}, ...]}
    """
    if isinstance(recipients, str):
        targets: List[str] = [recipients]
    else:
        targets = [str(x) for x in recipients if str(x).strip()]

    if not targets:
        logging.warning("send_line: recipients is empty")
        return {"ok": 0, "ng": 0, "fails": []}

    safe_text = _sanitize_text(text)
    result = {"ok": 0, "ng": 0, "fails": []}

    # 軽いスロットリング：送信間隔 0.2s（レート制限回避のため）
    for uid in targets:
        ok, reason = _send_with_retry(uid, safe_text)
        if ok:
            result["ok"] += 1
        else:
            result["ng"] += 1
            result["fails"].append({"user_id": uid, "reason": reason})
        time.sleep(0.2)

    # 失敗がある場合はログに残す
    if result["ng"]:
        logging.warning("LINE送信失敗 %s", result["fails"])
    else:
        logging.info("LINE送信OK（%d件）", result["ok"])

    return result