# main.py ーー 起動健全性チェック & ログ強化版

import os
import sys
import traceback
from datetime import datetime, timezone, timedelta

# ===== ログ整形 =====
def _ts():
    return datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S%z")

def log(level, msg):
    print(f"{_ts()} [{level}] {msg}", flush=True)

def _bool(env_name, default="0"):
    return (os.getenv(env_name, default) or "").strip().lower() in ("1","true","yes","on")

def _val(env_name, default=""):
    return (os.getenv(env_name, default) or "").strip()

def _int(env_name, default="0"):
    try:
        return int(_val(env_name, default) or "0")
    except:
        return 0

# ===== 主要ENVの可視化 =====
def show_boot_info():
    # Render cron は UTC 表示、アプリログは JST 表示に合わせて出す
    jst = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S %Z")
    utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    log("BOOT", f"now(JST)={jst} / now(UTC)={utc}")

    START_HOUR = _int("START_HOUR", "10")
    END_HOUR   = _int("END_HOUR", "22")
    FORCE_RUN  = _bool("FORCE_RUN", "0")
    DRY_RUN    = _bool("DRY_RUN", "0")

    log("ENV",  f"START_HOUR={START_HOUR} END_HOUR={END_HOUR} FORCE_RUN={FORCE_RUN} DRY_RUN={DRY_RUN}")
    log("ENV",  f"NOTIFY_ENABLED={_val('NOTIFY_ENABLED','1')} CUTOFF_OFFSET_MIN={_val('CUTOFF_OFFSET_MIN','0')}")
    # 機密は長さのみ
    log("ENV",  f"LINE_ACCESS_TOKEN.len={len(_val('LINE_ACCESS_TOKEN'))}")
    log("ENV",  f"GOOGLE_SHEET_ID.len={len(_val('GOOGLE_SHEET_ID'))} CREDENTIALS.len={len(_val('GOOGLE_CREDENTIALS_JSON'))}")
    log("ENV",  f"USERS_SHEET_NAME={_val('USERS_SHEET_NAME','1')} USERS_USERID_COL={_val('USERS_USERID_COL','H')}")

def _quick_sanity():
    """致命的に足りない場合でも落とさず no-op で成功終了できるように判断"""
    # Google Sheets と LINE の両方が未設定でも、DRY_RUN や FORCE_RUN で動作は可能。
    # ここでは「watcher.main() が動いても確実に失敗しない条件」を先に出す。
    msgs = []
    if not _val("GOOGLE_SHEET_ID") or not _val("GOOGLE_CREDENTIALS_JSON"):
        msgs.append("Google Sheets 未設定（通知TTL/ユーザー取得/記録はスキップ想定）")
    if not _val("LINE_ACCESS_TOKEN"):
        msgs.append("LINE 未設定（DRY_RUN 以外だと送信不可）")
    if msgs:
        for m in msgs:
            log("WARN", m)

def main():
    show_boot_info()
    _quick_sanity()

    try:
        # watcher をここで import（ソースに全角/BOM があっても Python 側で既に直っていればOK）
        from watcher import main as run_once, run_watcher_forever  # noqa: F401
    except Exception as e:
        log("FATAL", "watcher の import に失敗しました。以下スタックトレース：")
        traceback.print_exc()
        # ここで終了コード1にすると Render の「失敗」判定になる
        sys.exit(1)

    # 実行モード：Render の cron は 1回実行想定
    try:
        log("INFO", "WATCHER RUN start")
        run_once()
        log("INFO", "WATCHER OK (completed)")
    except SystemExit as e:
        # watcher 側で sys.exit を使ってもログを残す
        log("ERROR", f"SystemExit: code={e.code}")
        raise
    except Exception as e:
        log("ERROR", f"WATCHER EXCEPTION: {e.__class__.__name__}: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()