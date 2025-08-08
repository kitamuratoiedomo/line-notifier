# tanpuku_probe.py
import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1"

# ★ここにテストしたい RACEID を1つ入れてください（リンク抽出ログに出ていたIDでOK）
RACEID = "202508072135050400"  # 例）園田2RっぽいID。手元の実ログに出たものに置き換え

def fetch_tanfuku(raceid: str):
    url = f"https://keiba.rakuten.co.jp/odds/tanfuku/RACEID/{raceid}"
    r = requests.get(url, headers={"User-Agent": UA, "Accept-Language":"ja"}, timeout=25)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    rows = []
    # まずは生テキストで全<tr>をざっと見る（構造掴むフェーズ）
    for tr in soup.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
        if not cells: 
            continue
        rows.append(" | ".join(cells))

    print(f"[probe] URL: {url}")
    print("=== 先頭10行 ===")
    for line in rows[:10]:
        print(line)

if __name__ == "__main__":
    fetch_tanfuku(RACEID)