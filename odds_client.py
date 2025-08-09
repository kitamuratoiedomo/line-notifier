# odds_client.py
# インターフェイス確定版（まずはImportErrorを解消し、main.pyから利用できる形）
# 1) list_today_raceids(): 今日監視するレースID一覧を返す
# 2) fetch_tanfuku_odds(race_id): レースの単勝/複勝オッズと出走表（最低限）を返す
#
# 現状：
# - 本番スクレイピングは未実装。まずは環境変数からレースIDを受け取り、
#   テスト用ダミーデータ or 将来の実装差し替えポイントを提供。
#
# 将来差し替え：
# - 楽天等からの実オッズ取得を、このファイルの内部に実装して置き換えるだけでOK。
# - 関数の返り値フォーマットは崩さないこと。

from __future__ import annotations
import os
import re
import time
from typing import List, Dict, Any, Optional


def _split_ids(s: str) -> List[str]:
    return [x.strip() for x in re.split(r"[,\s]+", s) if x.strip()]


def list_today_raceids() -> List[str]:
    """
    監視対象レースIDの一覧を返す。
    まずは環境変数から渡す方式：
      - RACEIDS: "KAWASAKI-3R, FUNABASHI-7R" のようにカンマ区切り/空白区切りOK
      - RACEID: 1個だけ渡す場合
    何もなければ空配列を返す（本番スクレイピング未実装のため）。
    """
    s = os.getenv("RACEIDS") or os.getenv("RACEID") or ""
    ids = _split_ids(s)
    return ids


def fetch_tanfuku_odds(race_id: str) -> Dict[str, Any]:
    """
    指定レースの最小限データを返す。返却フォーマットの例：
    {
      "race_id": "KAWASAKI-3R",
      "venue": "川崎",
      "race_no": "3R",
      "start_at_iso": "2025-08-09T10:37:00+09:00",
      "entries": [
        {"num": "1", "horse": "ホースA", "jockey": "矢野貴之", "win": 2.8, "place": 1.4, "pop": 1},
        ...
      ]
    }

    現状はテスト用のダミーデータを返す。
    - 本番化する時は、この関数内部で楽天等からHTML/JSONを取得 → パースして
      上記フォーマットで返すように差し替える。
    """
    # ---- ここから先はダミー（テスト通知を動かすための最低限） ----
    # race_id からそれっぽく venue/race_no を作る（"KAWASAKI-3R" 形式想定）
    venue = race_id.split("-")[0] if "-" in race_id else "川崎"
    race_no = race_id.split("-")[1] if "-" in race_id and len(race_id.split("-")) >= 2 else "3R"

    # 開始時刻：今から +30分 を仮設定（ISO8601 / JST）
    # ※ 実オッズ導入時は、サイトから正確な発走時刻を取って入れてください
    start_at_iso = _in_30min_iso()

    entries = [
        # 人気・オッズはテスト用の固定値。戦略判定コードが動く最低限の形にしてある。
        {"num": "1", "horse": "ホースA", "jockey": "矢野貴之", "win": 2.8, "place": 1.4, "pop": 1},
        {"num": "2", "horse": "ホースB", "jockey": "本田正重", "win": 4.2, "place": 1.9, "pop": 2},
        {"num": "3", "horse": "ホースC", "jockey": "笹川翼", "win": 6.5, "place": 2.3, "pop": 3},
        {"num": "4", "horse": "ホースD", "jockey": "和田譲治", "win": 9.8, "place": 3.0, "pop": 4},
        {"num": "5", "horse": "ホースE", "jockey": "達城龍次", "win": 12.3, "place": 3.6, "pop": 5},
    ]

    return {
        "race_id": race_id,
        "venue": _venue_ja(venue),
        "race_no": race_no,
        "start_at_iso": start_at_iso,
        "entries": entries,
    }


# ---------- ヘルパ ----------

def _in_30min_iso() -> str:
    # JST固定の簡易ISO。依存を増やさないため time モジュールで生成
    # 実オッズ時は正確時刻に差し替え
    t = time.time() + 30 * 60
    # 9時間(=32400秒)進めてJSTっぽくする簡易実装
    jst = t + 9 * 60 * 60
    lt = time.gmtime(jst)  # UTCとして見た時の構造体
    return time.strftime("%Y-%m-%dT%H:%M:00+09:00", lt)


def _venue_ja(s: str) -> str:
    # "KAWASAKI" -> "川崎" などの簡易変換。必要に応じて拡張してください。
    m = {
        "KAWASAKI": "川崎",
        "FUNABASHI": "船橋",
        "OI": "大井",
        "KASAMATSU": "笠松",
        "NAGOYA": "名古屋",
        "MIZUSAWA": "水沢",
        "MORIOKA": "盛岡",
        "KOUCHI": "高知",
        "SONODA": "園田",
        "HIMEJI": "姫路",
        "URAWA": "浦和",
        "SAGAWA": "佐賀",
        "MONBETSU": "門別",
        "KANAZAWA": "金沢",
        "KITA": "北見",
    }
    return m.get(s.upper(), s)