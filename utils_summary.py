# -*- coding: utf-8 -*-
from datetime import datetime, timedelta, timezone

# JST ユーティリティ（単機能に分離）
JST = timezone(timedelta(hours=9))

def jst_now() -> str:
    """JST 現在時刻（YYYY-MM-DD HH:MM:SS）"""
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

def jst_today_str() -> str:
    """JST 今日の日付（YYYYMMDD）"""
    return datetime.now(JST).strftime("%Y%m%d")