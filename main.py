# main.py ーー 起動健全性チェック & ログ強化版
import os, sys, traceback
from datetime import datetime, timezone, timedelta

def _ts(): return datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S%z")
def log(level, msg): print(f"{_ts()} [{level}] {msg}", flush=True)
def _val(k, d=""): return (os.getenv(k, d) or "").strip()
def _int(k, d="0"):
    try: return int(_val(k, d) or "0")
    except: return 0
def _bool(k, d="0"): return (_val(k, d).lower() in ("1","true","yes","on"))

def show_boot_info():
    jst = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S %Z")
    utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    log("BOOT", f"now(JST)={jst} / now(UTC)={utc}")
    log("ENV",  f"START_HOUR={_int('START_HOUR','10')} END_HOUR={_int('END_HOUR','22')} FORCE_RUN={_bool('FORCE_RUN')} DRY_RUN={_bool('DRY_RUN')}")
    log("ENV",  f"NOTIFY_ENABLED={_val('NOTIFY_ENABLED','1')} CUTOFF_OFFSET_MIN={_val('CUTOFF_OFFSET_MIN','12')}")
    log("ENV",  f"LINE_ACCESS_TOKEN.len={len(_val('LINE_ACCESS_TOKEN'))}")
    log("ENV",  f"GOOGLE_SHEET_ID.len={len(_val('GOOGLE_SHEET_ID'))} CREDENTIALS.len={len(_val('GOOGLE_CREDENTIALS_JSON'))}")
    log("ENV",  f"USERS_SHEET_NAME={_val('USERS_SHEET_NAME','1')} USERS_USERID_COL={_val('USERS_USERID_COL','H')}")

def main():
    show_boot_info()
    try:
        # ランタイムは1回実行想定（Cron）
        from watcher import main as run_once
    except Exception:
        log("FATAL", "watcher の import に失敗しました。以下スタックトレース：")
        traceback.print_exc()
        sys.exit(1)
    try:
        log("INFO", "WATCHER RUN start")
        run_once()
        log("INFO", "WATCHER OK (completed)")
    except SystemExit as e:
        log("ERROR", f"SystemExit: code={e.code}"); raise
    except Exception:
        log("ERROR", f"WATCHER EXCEPTION")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()