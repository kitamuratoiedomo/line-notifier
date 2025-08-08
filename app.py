import os
import sys
import datetime as dt
import requests
from bs4 import BeautifulSoup

RACE_URL = os.getenv(
    "RACE_URL",
    # 例: 楽天競馬 当日レースカード（ユーザーさんが前に貼ってくれた例）
    "https://keiba.rakuten.co.jp/race_card/list/RACEID/202508070000000000?bmode=1&l-id=sp_top_raceInfoToday_raceCard_pc",
)

UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"
)

def log(msg):
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")
    sys.stdout.flush()

def main():
    log("=== 取得テスト開始 ===")
    log(f"TARGET: {RACE_URL}")

    try:
        resp = requests.get(RACE_URL, headers={"User-Agent": UA}, timeout=20)
        log(f"HTTP status: {resp.status_code}")
        log(f"Content-Type: {resp.headers.get('Content-Type')}")
        text = resp.text
        log(f"取得サイズ: {len(text)} bytes")

        # 先頭 2000 文字をログに出す（レンダリングや JS SPA の有無を確認）
        head = text[:2000].replace("\n", "\\n")  # 改行を潰すと読みやすい
        log(f"HTML head(2000): {head}")

        if resp.status_code != 200 or len(text) < 500:
            log("⚠️ 取得失敗の可能性（ステータス or サイズが不正）")
            return

        # 解析の最小テスト：title を抜く
        soup = BeautifulSoup(text, "html.parser")
        title = (soup.title.string.strip() if soup.title else "（title なし）")
        log(f"page <title>: {title}")

        # JSで描画するタイプかどうかの簡易チェック
        if "id=\"__NEXT_DATA__\"" in text or "id=\"__NUXT\"" in text or "window.__NUXT__" in text:
            log("⚠️ JSレンダリング型の可能性（静的HTMLにデータが無い）")
            log("　→ 解析はXHRのJSON APIを探すか、データが埋め込まれている<script>を抽出する必要あり")

        # ここまで通れば「ひとまず取得はOK」
        log("✅ 取得テスト完了（次は解析手順に進めます）")

    except Exception as e:
        log(f"❌ 例外: {e!r}")

if __name__ == "__main__":
    main()