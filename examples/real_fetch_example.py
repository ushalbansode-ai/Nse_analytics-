#!/usr/bin/env python3
import requests
import json
import time
import pandas as pd
from datetime import datetime

# -----------------------------------------
# FIX 1: NSE Updated URLs (Dec 2024–2025)
# -----------------------------------------
URLS = [
    "https://www.nseindia.com/api/option-chain-indices?symbol={}",
    "https://www.nseindia.com/api/option-chain-equities?symbol={}",
    "https://www.nseindia.com/api/option-chain?symbol={}"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/option-chain"
}

SESSION = requests.Session()

# -----------------------------------------
# FIX 2: NSE sends cookies only after homepage visit
# -----------------------------------------
def ensure_nse_cookies():
    try:
        SESSION.get("https://www.nseindia.com", headers=HEADERS, timeout=5)
    except:
        pass


# -----------------------------------------
# FIX 3: Parse ALL possible NSE formats
# -----------------------------------------
def parse_chain(json_data):
    """
    NSE now gives 3 possible response formats:

    1) { "records": { "data": [...] } }  (OLD)
    2) { "data": [...] }                 (NEW)
    3) Direct list                       (Some symbols)

    This parser handles all formats safely.
    """
    if not json_data:
        return []

    # Type-1 (OLD)
    if "records" in json_data and "data" in json_data["records"]:
        return json_data["records"]["data"]

    # Type-2 (NEW)
    if "data" in json_data and isinstance(json_data["data"], list):
        return json_data["data"]

    # Type-3 (list response)
    if isinstance(json_data, list):
        return json_data

    # If nothing works, return empty
    return []


# -----------------------------------------
# FIX 4: Retry loop for throttling
# -----------------------------------------
def fetch_snapshot(symbol):
    ensure_nse_cookies()

    for attempt in range(7):
        for url in URLS:
            try:
                full_url = url.format(symbol)
                r = SESSION.get(full_url, headers=HEADERS, timeout=10)
                data = r.json()

                parsed = parse_chain(data)

                if len(parsed) > 0:
                    return parsed

                print(f"[{symbol}] Missing data. Retrying {attempt+1}/7…")

            except Exception as e:
                print(f"[{symbol}] Error: {e}")

        time.sleep(1.2)

    print(f"[{symbol}] FAILED after 7 attempts.")
    return []


# -----------------------------------------
# FIX 5: Convert chain records to DataFrame cleanly
# -----------------------------------------
def normalize_chain(records):
    if not records:
        return pd.DataFrame()

    rows = []
    for row in records:
        ce = row.get("CE", {})
        pe = row.get("PE", {})
        rows.append({
            "strikePrice": row.get("strikePrice"),
            "CE_OI": ce.get("openInterest"),
            "CE_ChgOI": ce.get("changeinOpenInterest"),
            "CE_LTP": ce.get("lastPrice"),

            "PE_OI": pe.get("openInterest"),
            "PE_ChgOI": pe.get("changeinOpenInterest"),
            "PE_LTP": pe.get("lastPrice"),
        })
    return pd.DataFrame(rows)


# -----------------------------------------
# MAIN PROCESS
# -----------------------------------------
def process_symbol(symbol):
    print(f"\nFetching → {symbol}")

    raw = fetch_snapshot(symbol)

    if not raw:
        print(f"[{symbol}] Empty dataframe")
        return pd.DataFrame()

    df = normalize_chain(raw)
    print(f"[{symbol}] OK → {len(df)} rows")

    return df


def main():
    symbols = ["NIFTY", "BANKNIFTY"]
    final_results = {}

    for sym in symbols:
        final_results[sym] = process_symbol(sym)

    print("\nDONE.\n")


if __name__ == "__main__":
    main()
    
