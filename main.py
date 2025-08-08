import os, requests, datetime as dt

ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN")
USER_ID = os.getenv("LINE_USER_ID")

def line(message: str):
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"to": USER_ID, "messages": [{"type": "text", "text": message}]}
    r = requests.post(url, headers=headers, json=payload, timeout=10)
    print("LINE status:", r.status_code, r.text)

if __name__ == "__main__":
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] heartbeat")
    line(f"heartbeat at {now}")
