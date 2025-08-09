# send_test.py
import os, time, logging, requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip()
LINE_USER_ID = os.getenv("LINE_USER_ID", "").strip()

USE_SHEET = os.getenv("USE_SHEET", "1") == "1"
targets = []

def push(user_id: str, text: str):
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {LINE_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": text}]}
    r = requests.post(url, headers=headers, json=payload, timeout=10)
    if r.status_code != 200:
        logging.warning("LINE送信失敗 user=%s status=%s body=%s", user_id, r.status_code, r.text)
    else:
        logging.info("LINE送信OK user=%s", user_id[:6]+"…")

def main():
    global targets
    if not LINE_ACCESS_TOKEN:
        logging.error("LINE_ACCESS_TOKEN が未設定です")
        return

    if USE_SHEET:
        try:
            from sheets_client import fetch_recipients
            recs = fetch_recipients()
            targets = [r["userId"] for r in recs if r.get("enabled") and r.get("userId")]
            logging.info("シートから有効受信者=%d: %s", len(targets), [t[:6]+"…" for t in targets])
        except Exception as e:
            logging.warning("シート読込に失敗: %s", e)

    if not targets and LINE_USER_ID:
        targets = [LINE_USER_ID]
        logging.info("フォールバックで単一宛: %s", LINE_USER_ID[:6]+"…")

    if not targets:
        logging.error("宛先がありません（シートも単一宛も無し）")
        return

    text = "【テスト配信】複数宛先への一斉送信テストです。受信できたらOK。"
    for uid in targets:
        push(uid, text)
        time.sleep(0.15)

if __name__ == "__main__":
    main()