# -*- coding: utf-8 -*-
"""
結果取得ワーカー（厳密突合：馬番チケットで三連単を判定）
- input: notified_log（RACEID / TICKETS_UMA / SENT_AT_EPOCH ...）
- fetch: レース結果の 1着-2着-3着（馬番） と 三連単配当
- output: results_log に HIT, ROI を記録
環境変数:
  GOOGLE_CREDENTIALS_JSON / GOOGLE_SHEET_ID
  NOTIFIED_LOG_SHEET=notified_log
  RESULTS_LOG_SHEET=results_log
  RESULT_DELAY_MIN=5
  BET_UNIT_YEN=100
"""

import os, re, json, time, logging
from typing import List, Dict, Optional, Tuple
import requests
from bs4 import BeautifulSoup

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

JST_OFFSET = 9*3600
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"
})
TIMEOUT=(10,25)

GOOGLE_CREDENTIALS_JSON=os.getenv("GOOGLE_CREDENTIALS_JSON","")
GOOGLE_SHEET_ID=os.getenv("GOOGLE_SHEET_ID","")
NOTIFIED_LOG_SHEET=os.getenv("NOTIFIED_LOG_SHEET","notified_log")
RESULTS_LOG_SHEET=os.getenv("RESULTS_LOG_SHEET","results_log")
RESULT_DELAY_MIN=int(os.getenv("RESULT_DELAY_MIN","5"))
BET_UNIT_YEN=int(os.getenv("BET_UNIT_YEN","100"))

RACEID_RE=re.compile(r"/RACEID/(\d{18})")

def _svc():
    info=json.loads(GOOGLE_CREDENTIALS_JSON)
    creds=Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets","v4",credentials=creds, cache_discovery=False)

def _fetch(url:str)->str:
    r=SESSION.get(url, timeout=TIMEOUT); r.raise_for_status(); r.encoding="utf-8"; return r.text

def _read_notified_rows()->List[Dict]:
    svc=_svc()
    rng=f"'{NOTIFIED_LOG_SHEET}'!A:J"
    res=svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=rng).execute()
    vals=res.get("values",[])
    if not vals or vals[0][0]!="RACEID": return []
    rows=[]
    for row in vals[1:]:
        # A:RID, B:SENT_AT_EPOCH, C:POST_HM, D:VENUE, E:RACE_NO, F:STRATEGY_ID, G:STRATEGY_LABEL, H:TICKETS_POP, I:TICKETS_UMA, J:POP2UMA_JSON
        while len(row)<10: row.append("")
        rid=row[0].strip()
        try: sent=float(row[1])
        except: sent=0.0
        tickets_uma=[s.strip() for s in row[8].split(",") if s.strip()]
        rows.append({"rid":rid,"sent":sent,"tickets_uma":tickets_uma})
    return rows

def _already_done_rids()->set:
    svc=_svc()
    rng=f"'{RESULTS_LOG_SHEET}'!A:A"
    try:
        res=svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=rng).execute()
        vals=res.get("values",[])
        if not vals or vals[0][0]!="RACEID": return set()
        return set(row[0].strip() for row in vals[1:] if row and row[0].strip())
    except: return set()

def _append_result_row(rid:str, finish123:str, trifecta:int, hit:int, hit_ticket:str, tickets:str, roi_pct:float):
    svc=_svc()
    rng=f"'{RESULTS_LOG_SHEET}'!A:H"
    header=["RACEID","RESULT_AT_EPOCH","FINISHERS_123","TRIFECTA_YEN","HIT","HIT_TICKET","TICKETS_UMA","ROI_PCT"]
    try:
        res=svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=rng).execute()
        vals=res.get("values",[])
        if not vals:
            svc.spreadsheets().values().update(spreadsheetId=GOOGLE_SHEET_ID, range=rng,
                valueInputOption="RAW", body={"values":[header]}).execute()
        body={"values":[[rid, time.time(), finish123, trifecta, hit, hit_ticket, tickets, f"{roi_pct:.1f}"]]}
        svc.spreadsheets().values().append(spreadsheetId=GOOGLE_SHEET_ID, range=rng,
            valueInputOption="RAW", insertDataOption="INSERT_ROWS", body=body).execute()
    except Exception as e:
        logging.exception("[ERROR] results_log 追記失敗: %s", e)

def _parse_finish_and_trifecta(rid:str)->Optional[Tuple[str,int]]:
    # 候補URLを順番に試す
    urls=[
        f"https://keiba.rakuten.co.jp/race_card/race_result/RACEID/{rid}",
        f"https://keiba.rakuten.co.jp/race_card/race_detail/RACEID/{rid}",
        f"https://keiba.rakuten.co.jp/odds/odds_payoff/RACEID/{rid}",  # 払戻ページ（あれば）
    ]
    for url in urls:
        try:
            html=_fetch(url)
            soup=BeautifulSoup(html,"lxml")
            # 1) 三連単配当
            trifecta=0
            payoff = soup.find(text=re.compile("三連単"))
            if payoff:
                # 近傍から金額を拾う
                block = payoff.parent if hasattr(payoff,"parent") else soup
                txt = block.get_text(" ", strip=True)
                m = re.search(r"三連単[^0-9]*([0-9,]+)円", txt)
                if not m:
                    # 払戻テーブルの別セル
                    for td in soup.find_all(["td","th"]):
                        t=td.get_text(" ",strip=True)
                        if "三連単" in t:
                            sibl=td.find_next("td")
                            if sibl:
                                mm=re.search(r"([0-9,]+)", sibl.get_text(" ",strip=True))
                                if mm: trifecta=int(mm.group(1).replace(",",""))
                else:
                    trifecta=int(m.group(1).replace(",",""))
            # 2) 着順（上位3頭の馬番）
            # 共通：順位/着順列があるtableを探す
            finish=[]
            for table in soup.find_all("table"):
                head = table.find("thead")
                if not head: continue
                htxt = " ".join(head.stripped_strings)
                if not re.search(r"(着順|順位)", htxt): continue
                body=table.find("tbody") or table
                for tr in body.find_all("tr")[:3]:
                    tds=tr.find_all(["td","th"])
                    txts=[" ".join(td.stripped_strings) for td in tds]
                    # 馬番を含みそうなセルを探す
                    bn=None
                    for s in txts:
                        m=re.search(r"\b(\d{1,2})\b", s)
                        if m:
                            num=int(m.group(1))
                            # 着順っぽい列は最初に来るので馬番の値としては 1~18 程度
                            if 1<=num<=18: bn=num; break
                    if bn is not None: finish.append(bn)
                if len(finish)>=3: break
            if len(finish)>=3:
                return f"{finish[0]}-{finish[1]}-{finish[2]}", trifecta
        except Exception as e:
            logging.warning("[WARN] result fetch fail: %s (%s)", e, url)
    return None

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logging.info("[WORKER] start")
    rows=_read_notified_rows()
    done=_already_done_rids()
    now=time.time()
    for r in rows:
        rid=r["rid"]; sent=r["sent"]; tuma=r["tickets_uma"]
        if not tuma: continue
        if rid in done: continue
        if (now - sent) < RESULT_DELAY_MIN*60:
            continue
        got=_parse_finish_and_trifecta(rid)
        if not got:
            logging.info("[SKIP] 結果未取得 rid=%s", rid); continue
        fin, payout = got
        # 厳密突合
        hit_ticket=""
        hit=0
        for t in tuma:
            if t and t.strip()==fin:
                hit=1; hit_ticket=t; break
        # ROI（1点BET_UNIT_YEN × 点数）
        bet = BET_UNIT_YEN * len([x for x in tuma if x])
        roi = (payout / bet * 100.0) if hit and bet>0 else 0.0
        _append_result_row(rid, fin, payout, hit, hit_ticket, ",".join(tuma), roi)
        logging.info("[OK] rid=%s fin=%s payout=%s hit=%s roi=%.1f", rid, fin, payout, hit, roi)
    logging.info("[WORKER] done")

if __name__=="__main__":
    main()