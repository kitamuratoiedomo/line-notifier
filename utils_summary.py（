from datetime import datetime, timedelta, timezone

JST = timezone(timedelta(hours=9))

def jst_now() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

def jst_today_str() -> str:
    return datetime.now(JST).strftime("%Y%m%d")