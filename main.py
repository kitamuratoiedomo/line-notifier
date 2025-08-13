# -*- coding: utf-8 -*-
"""
起動ランチャー：
- 内部スケジューラ（任意）を起動
- watcher.run_watcher_forever() を呼び出し
- RUN_DAILY_ON_BOOT=1 のとき一回分の run を即時実行
"""

import os
from apscheduler.schedulers.background import BackgroundScheduler
import pytz

from watcher import main as run_once, run_watcher_forever

# daily.py があればスケジュールに登録（無ければスキップ）
try:
    from daily import run_daily_summary_once, run_yesterday_catchup
    HAVE_DAILY = True
except Exception:
    HAVE_DAILY = False

JST = pytz.timezone("Asia/Tokyo")

def start_scheduler():
    sch = BackgroundScheduler(timezone=JST)
    if HAVE_DAILY:
        # 確定配当の反映遅延を吸収するため 21:02 に発火
        sch.add_job(run_daily_summary_once, 'cron', hour=21, minute=2, id='daily_summary')
        # 前日補完（未確定があった場合の再集計）
        sch.add_job(run_yesterday_catchup, 'cron', hour=9, minute=0, id='daily_catchup')
    sch.start()
    return sch

if __name__ == '__main__':
    print('[BOOT] start internal scheduler (JST)')
    start_scheduler()

    # デプロイ直後の動作確認用
    if os.getenv('RUN_DAILY_ON_BOOT') == '1':
        run_once()

    # 常駐監視（1分ごとに1周）
    run_watcher_forever()