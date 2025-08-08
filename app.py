# app.py －－－ 診断②：リンク抽出（個別レース/オッズのパターンを掴む）
import os, sys, re, datetime as dt
import requests
from bs4 import BeautifulSoup

TARGET = "https://keiba.rakuten.co.jp/race_card/list/RACEID/202508070000000000?bmode=1"

def log(s): 
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {s}", flush=True)

def main():
    log("=== リンク抽出テスト開始 ===")
    log(f"TARGET: {TARGET}")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        )
    }
    r = requests.get(TARGET, headers=headers, timeout=25)
    log(f"HTTP status: {r.status_code}")
    ctype = r.headers.get("Content-Type", "")
    log(f"Content-Type: {ctype}")
    html = r.text
    log(f"取得サイズ: {len(html)} bytes")

    soup = BeautifulSoup(html, "html.parser")

    # 1) ページタイトル
    title = (soup.title.string.strip() if soup.title and soup.title.string else "")
    log(f"page <title>: {title}")

    # 2) すべてのリンクを抽出
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)
        links.append((href, text))

    log(f"aタグ総数: {len(links)}")

    # 3) race_card / odds を含むリンクを分類表示
    def pick(pattern, name, limit=40):
        found = [(h, t) for h, t in links if pattern in h]
        log(f"--- {name} ({len(found)}件) ---")
        for h, t in found[:limit]:
            log(f"{name}: href={h} | text='{t}'")
        return found

    race_card_links = pick("/race_card/", "race_card_link")
    odds_links      = pick("/odds",       "odds_link")

    # 4) 絶対URLに補正（相対パスなら）
    def abs_url(href):
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            return "https://keiba.rakuten.co.jp" + href
        # 相対→一旦基底からの単純連結（必要に応じて厳密化）
        return "https://keiba.rakuten.co.jp/" + href

    # サンプルとして race_card/odds の先頭数件を絶対URLで出す
    log("--- 代表URL（上位5件） ---")
    for kind, arr in [("race_card", race_card_links), ("odds", odds_links)]:
        for i, (h, t) in enumerate(arr[:5], 1):
            log(f"{kind}[{i}]: {abs_url(h)} | text='{t}'")

    log("✅ 診断②完了：href パターンが見えたら、次は個別ページの中身を解析して『人気・オッズ・締切』を取りに行きます。")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"例外: {e}")
        sys.exit(1)