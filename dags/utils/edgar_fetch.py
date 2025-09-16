import time
import requests

BASE = "https://www.sec.gov/Archives/edgar/daily-index"

def fetch_master_index(date_yyyymmdd: str) -> bytes:
    yyyy = date_yyyymmdd[:4]
    month = int(date_yyyymmdd[4:6])
    qtr = f"QTR{((month - 1) // 3) + 1}"
    fname = f"master.{date_yyyymmdd}.idx"
    url = f"{BASE}/{yyyy}/{qtr}/{fname}"
    headers = {"User-Agent": "edgar-pipeline (contact: example@example.com)"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    time.sleep(0.2)
    return resp.content
