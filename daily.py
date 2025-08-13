# 構成概要
# - 監視ループ（既存）を維持したまま、同一プロセス内に内部スケジューラを導入
# - 毎日 JST 21:02 に日次サマリ（前日補完は JST 09:00）
# - 通知ログは append-only（唯一の真実のテーブル）
# - 送信は LINE マルチキャスト、ユーザIDは既存 users シートの H 列
# - ロックは単インスタンス前提で /tmp に日付ロック（必要なら後でシート/Redisに格上げ可能）

# ==============================================
# requirements.txt（追記）
# ==============================================
# apscheduler
# pytz
# requests

# 既に入っていれば追加不要。Render のビルドでインストールされるようにします。


# ==============================================
# main.py（変更点のみ：内部スケジューラの起動）
# ==============================================
"""
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
from daily import run_daily_summary_once, run_yesterday_catchup
from watcher import run_watcher_forever  # 既存の常駐監視

JST = pytz.timezone("Asia/Tokyo")


def start_scheduler():
    sch = BackgroundScheduler(timezone=JST)
    # 確定配当の反映遅延を吸収するため 21:02 に発火
    sch.add_job(run_daily_summary_once, 'cron', hour=21, minute=2, id='daily_summary')
    # 前日補完（未確定があった場合の再集計）
    sch.add_job(run_yesterday_catchup, 'cron', hour=9, minute=0, id='daily_catchup')
    sch.start()


if __name__ == '__main__':
    print('[BOOT] start internal scheduler (JST)')
    start_scheduler()

    # 任意：即時実行のフラグ（デプロイ後の動作確認用）
    import os
    if os.getenv('RUN_DAILY_ON_BOOT') == '1':
        run_daily_summary_once()

    run_watcher_forever()
"""

# 既存の if __name__ == '__main__': ブロックを上記に置き換えます。


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


# ==============================================
# utils_summary.py（新規作成：サマリ用ユーティリティ）
# ==============================================
"""
- Google シート 読取/追記の薄いラッパ（既存のシートラッパがあれば差し替え推奨）
- Rakuten 確定結果取得のリトライ（既存の fetch 関数があれば差し替え）
- 集計＆メッセージ整形（無料/有料の差し替えポイントをコメント）
"""
from __future__ import annotations
import os
import time
from dataclasses import dataclass
from typing import List, Dict, Any
from datetime import datetime, timedelta
import pytz

JST = pytz.timezone('Asia/Tokyo')
SHEET_NOTIFY_LOG_TAB = os.getenv('SHEET_NOTIFY_LOG_TAB', 'notify_log')
USERS_TAB = os.getenv('USERS_TAB', '1')

# 既存の Google シート操作関数を使えるならそれに合わせてください。
# ここでは簡易に gspread 風インタフェースを想定し、関数 stub を用意します。

def _sheet_client():
    """既存の GSheets クライアントを返す想定の関数（プロジェクト依存）。"""
    from sheets import get_client  # 既存プロジェクトの関数に置き換え
    return get_client()


def jst_now():
    return datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')


def jst_today_str():
    return datetime.now(JST).strftime('%Y-%m-%d')


def jst_yesterday_str():
    return (datetime.now(JST) - timedelta(days=1)).strftime('%Y-%m-%d')


# ---- データモデル
@dataclass
class NotifyRow:
    date_jst: str
    race_id: str
    strategy: str  # ①/②/③/④ など
    stake: int     # その通知における推奨投資額合計（円）
    bets_json: str # 買い目 JSON（配列）
    notified_at: str
    jockey_ranks: str  # 任意: "A/B/C" など


# ---- 読み出し

def read_notify_logs_for_day(day_str: str) -> List[Dict[str, Any]]:
    sc = _sheet_client()
    ws = sc.open_by_env().worksheet(SHEET_NOTIFY_LOG_TAB)
    values = ws.get_all_values()
    if not values:
        return []
    header = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(header)}

    rows = []
    for r in values[1:]:
        if len(r) < len(header):
            continue
        if r[idx.get('date_jst', 0)] != day_str:
            continue
        rows.append({h: r[idx[h]] for h in header})
    return rows


# ---- ユーザID

def load_user_ids() -> List[str]:
    sc = _sheet_client()
    ws = sc.open_by_env().worksheet(USERS_TAB)
    values = ws.get_all_values()
    if not values:
        return []
    header = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(header)}
    # 既存仕様：H 列に userId（ヘッダ名は 'userId' を想定）
    key = 'userId' if 'userId' in idx else 'H'
    uids = []
    for r in values[1:]:
        try:
            u = r[idx[key]] if key in idx else r[7]
            if u:
                uids.append(u)
        except Exception:
            continue
    return list(dict.fromkeys(uids))  # 重複除去


# ---- 日付ロック

def try_acquire_daily_lock(day_str: str) -> bool:
    path = f"/tmp/daily_lock_{day_str}"
    if os.path.exists(path):
        return False
    try:
        with open(path, 'w') as f:
            f.write('1')
        return True
    except Exception:
        return False


# ---- 確定結果取得（既存関数がある場合は差し替え）

def _fetch_result_or_none(race_id: str) -> Dict[str, Any] | None:
    # 既存の関数 fetch_result_by_raceid があれば利用
    try:
        from rakuten import fetch_result_by_raceid  # プロジェクトの関数名に合わせて
        return fetch_result_by_raceid(race_id)
    except Exception:
        return None


def fetch_results_with_retry(rows: List[Dict[str, Any]], max_wait_sec: int = 180) -> List[Dict[str, Any]]:
    start = time.time()
    enriched = []
    pending = list(rows)
    backoff = 2
    while pending and time.time() - start < max_wait_sec:
        next_pending = []
        for r in pending:
            rid = r.get('race_id') or r.get('RACEID') or r.get('rid')
            res = _fetch_result_or_none(rid)
            if res and res.get('settled'):  # settled=True を既存側で立てる想定
                enriched.append({**r, **res})
            else:
                next_pending.append(r)
        if not next_pending:
            break
        time.sleep(backoff)
        backoff = min(backoff * 2, 15)
        pending = next_pending
    # 取り切れなかった分もそのまま返す（settled=False として扱う）
    for r in pending:
        r2 = dict(r)
        r2['settled'] = False
        enriched.append(r2)
    return enriched


# ---- 集計と LINE メッセージ整形

def build_summary_report_messages(rows: List[Dict[str, Any]], day_str: str, compact: bool = False) -> List[str]:
    # 戦略別に集計
    by_strategy: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        strat = r.get('strategy') or r.get('戦略') or '-'
        d = by_strategy.setdefault(strat, dict(total_bets=0, invest=0, hit=0, return_amt=0))
        stake = int(str(r.get('stake') or 0))
        d['invest'] += stake
        d['total_bets'] += 1
        if r.get('settled') and r.get('payout_total'):  # 例：確定配当合計（円）
            d['hit'] += 1
            d['return_amt'] += int(r['payout_total'])

    lines = []
    header = f"【日次サマリ】{day_str}"

    # 有料/無料の差し替えポイント（必要ならここで分岐）
    body = []
    for strat, s in sorted(by_strategy.items()):
        roi = (s['return_amt'] / s['invest'] * 100) if s['invest'] else 0.0
        hit_rate = (s['hit'] / s['total_bets'] * 100) if s['total_bets'] else 0.0
        body.append(
            f"戦略{strat}: 投資{format(s['invest'], ',')}円 / 回収{format(s['return_amt'], ',')}円\n"
            f"  回収率 {roi:.1f}% / 的中率 {hit_rate:.1f}%（{s['hit']}/{s['total_bets']}）"
        )

    note = (
        "\n※オッズは締切直前まで変化しますので、ご注意ください。\n"
        "※馬券の的中を保証するものではありません。余裕資金の範囲内で馬券購入をお願いします。"
    )

    if not body:
        return [] if compact else [header + "\n本日の該当レースはありませんでした。" + note]

    text = header + "\n\n" + "\n".join(body) + note
    return [text]
"""


# ==============================================
# line_api.py（新規作成：最小限のマルチキャスト）
# ==============================================
"""
環境変数 LINE_CHANNEL_ACCESS_TOKEN を使用してマルチキャスト送信。
既存の送信ユーティリティがある場合は差し替えてください。
"""
import os
import requests

LINE_TOKEN = os.getenv('LINE_CHANNEL_ACCESS_TOKEN') or os.getenv('LINE_ACCESS_TOKEN')


def send_multicast(messages, user_ids):
    if not LINE_TOKEN:
        print('[LINE] token missing')
        return
    if not user_ids:
        print('[LINE] user_ids empty')
        return

    # LINE の上限安全策として 150 件で分割
    chunk = 150
    url = 'https://api.line.me/v2/bot/message/multicast'
    headers = {
        'Authorization': f'Bearer {LINE_TOKEN}',
        'Content-Type': 'application/json'
        }
    for i in range(0, len(user_ids), chunk):
        to = user_ids[i:i+chunk]
        payload = {
            'to': to,
            'messages': [{ 'type': 'text', 'text': m[:5000] }]  # 文字数上限対策
        }
        r = requests.post(url, headers=headers, json=payload, timeout=15)
        try:
            r.raise_for_status()
            print(f'[LINE] multicast ok to={len(to)}')
        except Exception as e:
            print('[LINE] multicast error', e, r.text)


# ==============================================
# watcher（既存）への最小変更：通知ログへの追記
# ==============================================
"""
# 既存の通知直後に、以下のような追記処理を 1 行足してください。
from utils_notify_log import append_notify_log  # 新規 or 既存関数

append_notify_log({
  'date_jst': jst_today_str(),
  'race_id': race_id,
  'strategy': strategy_id,     # '①' など
  'stake': total_stake_yen,    # 推奨合計投資額
  'bets_json': json.dumps(bets, ensure_ascii=False),
  'notified_at': jst_now(),
  'jockey_ranks': jockey_ranks_str,
})
"""


# ==============================================
# utils_notify_log.py（任意：シート追記の薄い実装例）
# ==============================================
"""
from utils_summary import _sheet_client, SHEET_NOTIFY_LOG_TAB

def append_notify_log(row: dict):
    sc = _sheet_client()
    ws = sc.open_by_env().worksheet(SHEET_NOTIFY_LOG_TAB)
    # 初回はヘッダが必要: ['date_jst','race_id','strategy','stake','bets_json','notified_at','jockey_ranks']
    # 既にヘッダがある前提で、末尾に追記
    ws.append_row([
        row.get('date_jst',''),
        row.get('race_id',''),
        row.get('strategy',''),
        row.get('stake',''),
        row.get('bets_json',''),
        row.get('notified_at',''),
        row.get('jockey_ranks',''),
    ])
"""


# ==============================================
# 環境変数（例）
# ==============================================
# USERS_TAB=1
# SHEET_NOTIFY_LOG_TAB=notify_log
# LINE_CHANNEL_ACCESS_TOKEN=***
# RUN_DAILY_ON_BOOT=0  # 動作確認で 1 にすれば起動時に一度だけ送信


# ==============================================
# 運用メモ
# ==============================================
# - Render で常時稼働（スリープ無効）にしておく
# - デプロイ後の確認手順：
#   1) ログに [BOOT] start internal scheduler (JST) が出ているか
#   2) RUN_DAILY_ON_BOOT=1 で一回だけ即時送信を試し、users=7 に配信されるか
#   3) 21:02(JST) に [DAILY] start ... / sent messages=1 が出るか
#   4) 翌 09:00 に [CATCHUP] が動作するか
# - 配当未確定が残る場合は、max_wait_sec を延長 or 21:03〜21:05 に調整
