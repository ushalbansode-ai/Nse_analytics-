#!/usr/bin/env python3
"""
Main runner: fetch NSE data and run analytics (example).
Run:
  python3 main.py --fetch
"""

import argparse
from pathlib import Path
import json

def main():
    import src.ingestion.nse_fetcher as fetcher  # relative import to your file structure

    import datetime
    now = datetime.datetime.utcnow().isoformat()
    print("Run at", now)

    res = fetcher.fetch_all(symbol="NIFTY", out_dir="data/latest")
    print("Fetched files manifest:", res)

    # Example: open the option chain JSON (if saved) and print top-level keys
    oc_file = res.get("option_chain")
    if oc_file:
        oc = json.load(open(oc_file, "r"))
        # Print some useful fields if present
        rec = oc.get("records", {})
        print("Underlying value:", rec.get("underlyingValue"))
        print("Expiry dates:", rec.get("expiryDates")[:5] if rec.get("expiryDates") else None)

if __name__ == "__main__":
    main()
    
  
