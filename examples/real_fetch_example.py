#!/usr/bin/env python3
"""
examples/real_fetch_example.py

Robust NSE option-chain fetcher (local + cloud) using cloudscraper.

- Uses cloudscraper to bypass NSE anti-bot protections.
- Tries multiple endpoints and formats (records/data/list).
- Polite retries, exponential backoff.
- Saves per-symbol CSVs and a combined CSV to data/option_chain_raw/.
- Designed to run locally (recommended) and on GitHub Actions (attempts).
"""

import time
import argparse
from pathlib import Path
import pandas as pd
from datetime import datetime
import sys
import math

# Try import cloudscraper; if missing, show message
try:
    import cloudscraper
except Exception as e:
    print("cloudscraper not installed. Please run: pip install cloudscraper")
    raise

# -----------------------
# Config
# -----------------------
DEFAULT_SYMBOLS = ["NIFTY", "BANKNIFTY"]
OUT_DIR = Path("data/option_chain_raw")
REQUEST_TIMEOUT = 18
MAX_RETRIES = 8
BASE_SLEEP = 1.2  # base backoff multiplier

# Candidate endpoints to try (order matters)
ENDPOINTS = [
    "https://www.nseindia.com/api/option-chain-indices?symbol={symbol}",
    "https://www.nseindia.com/api/option-chain-equities?symbol={symbol}",
    # fallback generic (less used)
    "https://www.nseindia.com/api/option-chain?symbol={symbol}"
]

# Browser-like headers used by most working scrapers
HEADERS = {
    "authority": "www.nseindia.com",
    "pragma": "no-cache",
    "cache-control": "no-cache",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "accept": "application/json, text/javascript, */*; q=0.01",
    "referer": "https://www.nseindia.com/option-chain",
    "accept-language": "en-US,en;q=0.9",
}

# -----------------------
# Utilities
# -----------------------
def sleep_backoff(attempt):
    # exponential with jitter
    s = BASE_SLEEP * (1.6 ** (attempt - 1))
    jitter = (0.6 * s) * (0.5 - (time.time() % 1))
    wait = max(0.5, s + jitter)
    time.sleep(wait)

def make_session():
    """
    Create a cloudscraper session and prime it by visiting the homepage (to obtain cookies).
    """
    s = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'mobile': False
        }
    )
    s.headers.update(HEADERS)
    try:
        # prime cookies — sometimes NSE requires an initial homepage GET
        s.get("https://www.nseindia.com", timeout=REQUEST_TIMEOUT)
        time.sleep(0.8)
    except Exception:
        # ignore; still proceed and let retries handle it
        pass
    return s

def is_html(resp_text: str) -> bool:
    if not resp_text:
        return False
    t = resp_text.strip().lower()
    return t.startswith("<!") or t.startswith("<html") or "<html" in t

def extract_records_from_json(json_data):
    """Return a list-like records from multiple possible NSE JSON shapes."""
    if not json_data:
        return []

    # 1) { "records": { "data": [...] } }
    if isinstance(json_data, dict) and "records" in json_data and isinstance(json_data["records"], dict):
        data = json_data["records"].get("data")
        if isinstance(data, list):
            return data

    # 2) { "data": [...] }
    if isinstance(json_data, dict) and "data" in json_data and isinstance(json_data["data"], list):
        return json_data["data"]

    # 3) Sometimes JSON returned directly as list
    if isinstance(json_data, list):
        return json_data

    # 4) Some APIs use different keys (try couple of known fallbacks)
    # - 'filtered', 'filtered' -> {'data': [...]}
    if isinstance(json_data, dict) and "filtered" in json_data and isinstance(json_data["filtered"], dict):
        data = json_data["filtered"].get("data")
        if isinstance(data, list):
            return data

    return []

def flatten_records_to_df(records, symbol, underlying_value=None):
    """
    Convert records (list of strike objects) to flat DataFrame with CE & PE rows.
    Each row represents one option side (CE or PE).
    """
    if not records:
        return pd.DataFrame()

    rows = []
    for r in records:
        # strike may be under strikePrice or strike
        strike = r.get("strikePrice") if r.get("strikePrice") is not None else r.get("strike")
        expiry = r.get("expiryDate") or r.get("expiry")

        # both CE and PE objects might be present
        for side in ("CE", "PE"):
            opt = r.get(side)
            if not opt:
                continue

            row = {
                "symbol": symbol,
                "underlying_value": underlying_value if underlying_value is not None else r.get("underlying"),
                "expiry": expiry,
                "strike": strike,
                "option_type": "CALL" if side == "CE" else "PUT",
                "oi": opt.get("openInterest") or opt.get("openInterest"),
                "change_in_oi": opt.get("changeinOpenInterest") or opt.get("changeInOpenInterest") or opt.get("changeInOI"),
                "volume": opt.get("totalTradedVolume") or opt.get("totalTradedVolume"),
                "ltp": opt.get("lastPrice") or opt.get("lastTradedPrice") or opt.get("last"),
                "bid_price": opt.get("bidprice") or opt.get("bidPrice"),
                "ask_price": opt.get("askPrice") or opt.get("askprice"),
                "iv": opt.get("impliedVolatility") or opt.get("impliedVolatility"),
            }
            rows.append(row)

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # convert numeric columns
    for col in ("oi", "change_in_oi", "volume", "ltp", "bid_price", "ask_price", "iv"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

# -----------------------
# Fetch logic
# -----------------------
def fetch_snapshot_for_symbol(symbol, max_retries=MAX_RETRIES):
    session = make_session()
    attempt = 0

    while attempt < max_retries:
        attempt += 1
        for ep in ENDPOINTS:
            url = ep.format(symbol=symbol)
            try:
                resp = session.get(url, timeout=REQUEST_TIMEOUT)
            except Exception as e:
                print(f"[{symbol}] Request error: {e} (endpoint {url})")
                continue

            text = resp.text
            # if HTML returned (captcha/blocked) -> retry
            if is_html(text):
                print(f"[{symbol}] HTML response (blocked) from {url}. Retrying {attempt}/{max_retries}...")
                continue

            # try parse JSON
            try:
                data = resp.json()
            except Exception as e:
                # sometimes empty response or non-json — log and retry
                print(f"[{symbol}] JSON parse error: {e} (endpoint {url}). Retrying {attempt}/{max_retries}...")
                continue

            records = extract_records_from_json(data)
            if not records:
                print(f"[{symbol}] Missing data in JSON from {url}. Retrying {attempt}/{max_retries}...")
                continue

            # extract underlying value if present
            underlying_value = None
            if isinstance(data, dict):
                underlying_value = data.get("records", {}).get("underlyingValue") if "records" in data else data.get("underlyingValue") or data.get("underlying")

            print(f"[{symbol}] Snapshot fetched from {url} (records: {len(records)})")
            df = flatten_records_to_df(records, symbol, underlying_value=underlying_value)
            return df

        # end endpoints loop — backoff then retry
        sleep_backoff(attempt)

    # all attempts failed
    print(f"[{symbol}] FAILED after {max_retries} attempts")
    return pd.DataFrame()

# -----------------------
# Public process function
# -----------------------
def process_symbol(symbol):
    print(f"\nFetching → {symbol}")
    df = fetch_snapshot_for_symbol(symbol)
    if df.empty:
        print(f"[{symbol}] Empty dataframe")
        return None

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fname = OUT_DIR / f"{symbol}_option_chain_{datetime.utcnow().strftime('%Y-%m-%d')}.csv"
    df.to_csv(fname, index=False)
    print(f"[{symbol}] Saved CSV: {fname} (rows: {len(df)})")
    return df

# -----------------------
# CLI / main
# -----------------------
def main(symbols):
    combined = []
    for s in symbols:
        df = process_symbol(s)
        if df is not None and not df.empty:
            combined.append(df)

    if combined:
        full = pd.concat(combined, ignore_index=True)
        out = OUT_DIR / f"option_chains_combined_{datetime.utcnow().strftime('%Y-%m-%d')}.csv"
        full.to_csv(out, index=False)
        print(f"\nCombined CSV saved: {out} (total rows: {len(full)})")
    else:
        print("\nNo option-chain data saved for the requested symbols.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch NSE option chain snapshots (cloudscraper).")
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS,
                        help="Symbols to fetch (default: NIFTY BANKNIFTY)")
    args = parser.parse_args()
    main(args.symbols)
                                            
