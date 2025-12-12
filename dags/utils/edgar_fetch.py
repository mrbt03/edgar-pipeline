# utils/edgar_fetch.py

# this module provides a function to fetch the master index from the SEC EDGAR website.
# it includes:
# - fetch_master_index(): fetches the master index from the SEC EDGAR website

# import python time module and requests module for HTTP requests
import time
import requests

# set the base URL for the SEC EDGAR website
BASE = "https://www.sec.gov/Archives/edgar/daily-index"

# fetch the master index from the SEC EDGAR website
def fetch_master_index(date_yyyymmdd: str) -> bytes:
    # parse the date into year, month, and quarter
    yyyy = date_yyyymmdd[:4]
    month = int(date_yyyymmdd[4:6])
    qtr = f"QTR{((month - 1) // 3) + 1}"
    # set the filename for the master index
    fname = f"master.{date_yyyymmdd}.idx"
    # set the URL for the master index
    url = f"{BASE}/{yyyy}/{qtr}/{fname}"
    # set the headers for the HTTP request
    headers = {"User-Agent": "edgar-pipeline (contact: mrbt4523@gmail.com)"}

    # set the last exception to None
    last_exc = None
    # try to fetch the master index 5 times
    for attempt in range(5):
        # try to fetch the master index
        try:
            # send the HTTP request to the SEC EDGAR website
            resp = requests.get(url, headers=headers, timeout=30)
            # raise an exception if the HTTP request is not successful
            resp.raise_for_status()
            # sleep for 0.2 seconds
            time.sleep(0.2)
            # return the content of the HTTP response
            return resp.content
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            # set the last exception to the current exception
            last_exc = e
            # set the sleep time to the minimum of 5 seconds or 0.5 seconds multiplied by 2 to the power of the attempt
            sleep_s = min(5, 0.5 * (2 ** attempt))
            time.sleep(sleep_s)
    # raise the last exception if the master index could not be fetched
    raise last_exc
