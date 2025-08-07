
import os
import requests

ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN")
USER_ID = os.getenv("LINE_USER_ID")

def notify_if_match(race, pattern):
    jockeys = race.get("jockeys", {})
    message = f"【戦略{pattern} 該当】\n{race['race_name']} 締切5分前！\n"
    message += "買い目：戦略に応じた買い目構成\n"
    message += "騎手ランク：" + " / ".join([f"{k}={v}" for k, v in jockeys.items()])

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "to": USER_ID,
        "messages": [{"type": "text", "text": message}]
    }
    requests.post("https://api.line.me/v2/bot/message/push", headers=headers, json=payload)
