# 先頭に追加
from odds_client import list_today_raceids, fetch_tanfuku_odds
from strategy_rules import (
    is_strategy_1, is_strategy_2, is_strategy_3, is_strategy_4,
    build_candidates_strategy_1, build_candidates_strategy_2,
    build_candidates_strategy_3, build_candidates_strategy_4
)

def find_strategy_matches() -> List[Dict[str, Any]]:
    """
    本日レースを列挙 → 単勝オッズ/人気取得 → 戦略①〜④を全判定 → ヒットのみ返却
    """
    hits: List[Dict[str, Any]] = []

    # 1) 本日の RACEID 一覧
    rid_list = list_today_raceids()
    if not rid_list:
        logging.info("本日のRACEIDが取得できませんでした。")
        return hits

    for item in rid_list:
        rid = item["rid"]
        venue_guess = item.get("venue", "")
        odds = fetch_tanfuku_odds(rid)
        if not odds:
            continue

        entries = odds["entries"]
        # 人気でソートされていない場合の保険
        entries = sorted(entries, key=lambda x: x["pop"])

        # 発走時刻（HH:MM）→ ISO
        hhmm = odds.get("hhmm")
        today = now_jst().strftime("%Y-%m-%d")
        if hhmm and ":" in hhmm:
            start_iso = f"{today}T{hhmm}:00+09:00"
        else:
            # 時刻不明なら近い将来に仮置き（通知窓の外になる可能性あり）
            start_iso = (now_jst() + timedelta(minutes=15)).isoformat()

        # R番号推定（末尾2桁がR？ 確実でないので “?R” 表記で逃がす）
        race_no = "?"
        m = re.search(r"(\d{2})$", rid)
        if m:
            race_no = f"{int(m.group(1))}R"

        # ①〜④ 判定（重複ヒットをどうするか → ここでは「全部通知」）
        if is_strategy_1(entries):
            hits.append({
                "race_id": rid,
                "venue": venue_guess,
                "race_no": race_no,
                "start_at_iso": start_iso,
                "strategy": "戦略①: 1〜3番人気BOX",
                "candidates": build_candidates_strategy_1(entries),
                "note": "",
            })
        if is_strategy_2(entries):
            hits.append({
                "race_id": rid,
                "venue": venue_guess,
                "race_no": race_no,
                "start_at_iso": start_iso,
                "strategy": "戦略②: 1着固定(1番人気)×2,3番人気",
                "candidates": build_candidates_strategy_2(entries),
                "note": "買い目は2点構成",
            })
        if is_strategy_3(entries):
            hits.append({
                "race_id": rid,
                "venue": venue_guess,
                "race_no": race_no,
                "start_at_iso": start_iso,
                "strategy": "戦略③: 1着固定(1番人気)×10〜20倍流し",
                "candidates": build_candidates_strategy_3(entries),
                "note": "相手は10〜20倍の馬（最大5頭）",
            })
        if is_strategy_4(entries):
            hits.append({
                "race_id": rid,
                "venue": venue_guess,
                "race_no": race_no,
                "start_at_iso": start_iso,
                "strategy": "戦略④: 3着固定(3番人気)",
                "candidates": build_candidates_strategy_4(entries),
                "note": "3着は3番人気固定／買い目2点",
            })

    return hits