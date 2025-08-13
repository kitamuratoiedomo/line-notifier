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