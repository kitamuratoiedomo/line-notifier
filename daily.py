# ==============================================
# daily.py（新規作成）
# ==============================================
"""
日次サマリの本体。通知ログ（append-only）から当日分を集計し、LINE で配信。
- ロック: /tmp/daily_lock_YYYYMMDD（単インスタンス前提）
- 通知ログのタブ名: ENV SHEET_NOTIFY_LOG_TAB（既定: notify_log）
- ユーザ一覧のタブ名: ENV USERS_TAB（既定: 1）
"""
from __future__ import annotations
import os
import json
from datetime import datetime, timedelta
import pytz

from utils_summary import (
    jst_today_str, jst_yesterday_str, jst_now,
    read_notify_logs_for_day, try_acquire_daily_lock,
    load_user_ids, fetch_results_with_retry,
    build_summary_report_messages
)
from line_api import send_multicast


JST = pytz.timezone('Asia/Tokyo')


def _send_empty_summary():
    msg = (
        "本日の該当レースはありませんでした。\n"
        "※オッズは締切直前まで変化しますので、ご注意ください。\n"
        "※馬券の的中を保証するものではありません。余裕資金の範囲内で馬券購入をお願いします。"
    )
    uids = load_user_ids()
    if not uids:
        print('[DAILY] users=0 skip send')
        return
    send_multicast([msg], uids)


def run_daily_summary_once():
    day = jst_today_str()
    print(f"[DAILY] start day={day} now={jst_now()}")

    if not try_acquire_daily_lock(day):
        print(f"[DAILY] lock exists for {day}, skip")
        return

    rows = read_notify_logs_for_day(day)
    if not rows:
        print('[DAILY] no notify rows for today -> empty summary')
        _send_empty_summary()
        print('[DAILY] done (empty)')
        return

    enriched = fetch_results_with_retry(rows, max_wait_sec=180)
    messages = build_summary_report_messages(enriched, day)

    uids = load_user_ids()
    if not uids:
        print('[DAILY] users=0 skip send')
        return

    send_multicast(messages, uids)
    print(f"[DAILY] sent messages={len(messages)}")


def run_yesterday_catchup():
    day = jst_yesterday_str()
    print(f"[CATCHUP] start day={day} now={jst_now()}")

    # 補完はロックしない（同日再送防止のみロック対象）
    rows = read_notify_logs_for_day(day)
    if not rows:
        print('[CATCHUP] no rows, skip')
        return

    enriched = fetch_results_with_retry(rows, max_wait_sec=180)
    # 欠測が埋まった場合のみ軽量版メッセージで通知
    messages = build_summary_report_messages(enriched, day, compact=True)
    if not messages:
        print('[CATCHUP] nothing to send')
        return

    uids = load_user_ids()
    if not uids:
        print('[CATCHUP] users=0 skip send')
        return

    send_multicast(messages, uids)
    print(f"[CATCHUP] sent messages={len(messages)}")
"""


