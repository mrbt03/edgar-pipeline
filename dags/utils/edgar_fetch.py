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

    last_exc = None
    for attempt in range(5):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            time.sleep(0.2)
            return resp.content
        except Exception as e:
            last_exc = e
            sleep_s = min(5, 0.5 * (2 ** attempt))
            time.sleep(sleep_s)
    raise last_exc
