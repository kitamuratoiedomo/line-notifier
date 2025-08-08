import os, re, requests, pytz
from bs4 import BeautifulSoup
from datetime import datetime

# ===== 戦略ごとの過去実績（通知文用） =====
ROI = {
    "①": {"hit": "22.4%", "roi": "138.5%"},
    "②": {"hit": "43.7%", "roi": "131.4%"},
    "③": {"hit": "16.8%", "roi": "139.2%"},
    "④": {"hit": "21.5%", "roi": "133.7%"},
}

# ===== ユーティリティ =====
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
}

def parse_float(text):
    try:
        t = re.sub(r"[^\d\.]", "", text)
        return float(t) if t else None
    except:
        return None

# ===== 楽天競馬スクレイプ（人気順・単勝オッズ取得） =====
def fetch_race(href_or_url: str):
    url = href_or_url
    if "bmode=1" not in url:
        url = url + ("&" if "?" in url else "?") + "bmode=1"

    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    horses = []

    # 楽天側のテーブル構造に合わせて複数候補を試す
    rows = soup.select("table.odds_table tbody tr, table.oddsTbody tr")
    if not rows:
        rows = soup.select("tbody tr")

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        rank_txt = tds[0].get_text(strip=True)
        if not rank_txt.isdigit():
            continue
        rank = int(rank_txt)

        horse_no  = tds[1].get_text(strip=True)
        horse_name = tds[2].get_text(strip=True)
        jockey    = tds[3].get_text(strip=True)
        odds      = parse_float(tds[4].get_text(strip=True))
        if odds is None:
            continue

        horses.append({
            "rank": rank,
            "horse_no": horse_no,
            "horse_name": horse_name,
            "jockey": jockey,
            "odds": odds
        })

    horses.sort(key=lambda x: x["rank"])
    return horses

# ===== 戦略①〜④の判定 =====
def check_strategies(horses):
    if not horses or len(horses) < 4:
        return []
    s = []
    o1, o2, o3, o4 = [h["odds"] for h in horses[:4]]

    # ① 1〜3番人気BOX
    if 2.0 <= o1 <= 10.0 and o2 < 10.0 and o3 < 10.0 and o4 >= 15.0:
        s.append("①")

    # ② 1番人気1着固定（2,3番人気相手）
    if o1 < 2.0 and o2 < 10.0 and o3 < 10.0:
        s.append("②")

    # ③ 1着固定 × 10〜20倍流し（1番人気 ≤1.5）
    others_10_20 = [h for h in horses[1:] if 10.0 <= h["odds"] <= 20.0]
    if o1 <= 1.5 and others_10_20:
        s.append("③")

    # ④ 3着固定（3番人気固定）
    if o1 <= 3.0 and o2 <= 3.0 and 6.0 <= o3 <= 10.0 and o4 >= 15.0:
        s.append("④")

    return s

# ===== エントリーポイント（テスト1回実行） =====
def main():
    jst = pytz.timezone("Asia/Tokyo")
    now = datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] テスト開始")

    race_url = os.getenv("RACE_URL", "").strip()
    if not race_url:
        print("環境変数 RACE_URL が未設定です。テスト対象の楽天レースURLを設定してください。")
        return

    try:
        horses = fetch_race(race_url)
        if not horses:
            print("出走馬の取得に失敗しました。ページ構造が変わった可能性があります。URL or セレクタ要確認。")
            return

        print("人気順（rank, 馬番, 馬名, 騎手, 単勝オッズ）:")
        for h in horses[:12]:
            print(f"  {h['rank']:>2}位  馬番{h['horse_no']:>2}  {h['horse_name']}  {h['jockey']}  {h['odds']}倍")

        matched = check_strategies(horses)
        if matched:
            print("該当戦略:", "・".join(matched))
            for m in matched:
                print(f"  戦略{m}: 的中率 {ROI[m]['hit']} / 回収率 {ROI[m]['roi']}")
        else:
            print("該当戦略なし")

    except Exception as e:
        print("エラー:", repr(e))

if __name__ == "__main__":
    main()