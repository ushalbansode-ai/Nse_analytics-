#!/usr/bin/env python3
import os
import json
import datetime
from pathlib import Path

import src.ingestion.nse_fetcher as fetcher
import src.analytics.premium_discount as premium_discount
import src.analytics.volume_oi as volume_oi
import src.analytics.reversals as reversals
import src.analytics.oi_skew as oi_skew
import src.analytics.gamma_exposure as gamma_exposure


DATA_LATEST = Path("data/latest")
DATA_SIGNALS = Path("data/signals")


def ensure_folders():
    DATA_LATEST.mkdir(parents=True, exist_ok=True)
    DATA_SIGNALS.mkdir(parents=True, exist_ok=True)


def save_json(path, obj):
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)


def process_symbol(symbol: str):
    """Fetch + compute analytics for one symbol (NIFTY, BANKNIFTY, etc.)."""
    print(f"\n=== Processing {symbol} ===")

    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    # -------- Fetch Data --------
    option_chain = fetcher.fetch_option_chain(symbol)
    quote = fetcher.fetch_quote(symbol)

    option_file = DATA_LATEST / f"option_chain_{symbol}_{ts}.json"
    quote_file = DATA_LATEST / f"quote_{symbol}_{ts}.json"
    manifest_file = DATA_LATEST / f"manifest_{symbol}_{ts}.json"

    save_json(option_file, option_chain)
    save_json(quote_file, quote)

    manifest = {
        "symbol": symbol,
        "timestamp": ts,
        "files": {
            "option_chain": str(option_file),
            "quote": str(quote_file),
            "manifest": str(manifest_file)
        }
    }
    save_json(manifest_file, manifest)

    print(f"{symbol} underlying:", quote.get("underlyingValue"))
    print("Expiries:", option_chain.get("records", {}).get("expiryDates", []))

    # -------- Compute Analytics --------
    signals = {}

    try:
        signals["premium_discount"] = premium_discount.compute(option_chain)
    except Exception as e:
        signals["premium_discount"] = {"error": str(e)}

    try:
        signals["volume_oi_spike"] = volume_oi.compute(option_chain)
    except Exception as e:
        signals["volume_oi_spike"] = {"error": str(e)}

    try:
        signals["reversal_signals"] = reversals.compute(option_chain)
    except Exception as e:
        signals["reversal_signals"] = {"error": str(e)}

    try:
        signals["oi_skew"] = oi_skew.compute(option_chain)
    except Exception as e:
        signals["oi_skew"] = {"error": str(e)}

    try:
        signals["gamma_exposure"] = gamma_exposure.compute(option_chain)
    except Exception as e:
        signals["gamma_exposure"] = {"error": str(e)}

    # -------- Save Analytics --------
    signals_file = DATA_SIGNALS / f"signals_{symbol}_{ts}.json"
    save_json(signals_file, signals)

    print(f"Saved analytics at: {signals_file}")


def main():
    ensure_folders()
    print("Run at", datetime.datetime.utcnow().isoformat())

    # ---- Add Any Symbol Here ----
    symbols = ["NIFTY", "BANKNIFTY"]

    for sym in symbols:
        process_symbol(sym)


if __name__ == "__main__":
    main()
  
