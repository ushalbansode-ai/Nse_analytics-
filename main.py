#!/usr/bin/env python3
"""
MAIN CONTROLLER
Fetches:
- NIFTY option chain
- BANKNIFTY option chain
- Quote data

Runs analyses:
- Option 1 (Premium–Discount)
- Option 2 (BankNifty Strategy)
- Option 3 (Volume Spike + Reversal Analysis)

Saves results in data/latest/
"""

import json
import datetime
from pathlib import Path

# -------------------------------
# Local imports
# -------------------------------
import src.ingestion.nse_fetcher as fetcher

# analytics modules
import src.analytics.premium_discount as premium_discount
import src.analytics.banknifty_option2 as banknifty_option2
import src.analytics.analytics_option3 as analytics_option3

# --------------------------------
# Setup folders
# --------------------------------
BASE_DIR = Path("data")
LATEST_DIR = BASE_DIR / "latest"
LATEST_DIR.mkdir(parents=True, exist_ok=True)


def save_json(data, filename):
    """Helper to save JSON into data/latest/"""
    filepath = LATEST_DIR / filename
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    return str(filepath)


def main():
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    print(f"\nRun at {ts}\n")

    # -----------------------------------------
    # 1) Fetch NIFTY OC + quote
    # -----------------------------------------
    nifty_oc = fetcher.fetch_option_chain("NIFTY")
    nifty_quote = fetcher.get_quote("NIFTY")

    oc_file = save_json(nifty_oc, f"option_chain_NIFTY_{ts}.json")
    qt_file = save_json(nifty_quote, f"quote_NIFTY_{ts}.json")

    print(f"NIFTY OC saved:   {oc_file}")
    print(f"NIFTY Quote saved: {qt_file}")

    underlying_nifty = (

        nifty_quote.get("priceInfo", {})
        .get("lastPrice", nifty_quote.get("underlying", 0))
    )
    print("NIFTY Underlying:", underlying_nifty)

    # -----------------------------------------
    # 2) Fetch BankNifty OC + quote
    # -----------------------------------------
    bn_oc = fetcher.fetch_option_chain("BANKNIFTY")
    bn_quote = fetcher.get_quote("BANKNIFTY")

    bn_oc_file = save_json(bn_oc, f"option_chain_BANKNIFTY_{ts}.json")
    bn_qt_file = save_json(bn_quote, f"quote_BANKNIFTY_{ts}.json")

    print(f"\nBANKNIFTY OC saved:   {bn_oc_file}")
    print(f"BANKNIFTY Quote saved: {bn_qt_file}")

    underlying_bn = (
        bn_quote.get("priceInfo", {})
        .get("lastPrice", bn_quote.get("underlying", 0))
    )
    print("BANKNIFTY Underlying:", underlying_bn)

    # -----------------------------------------
    # 3) ANALYTICS
    # -----------------------------------------

    # Option 1 → Premium Discount Analysis (NIFTY)
    option1_result = premium_discount.analyze_premium_discount(nifty_oc, underlying_nifty)
    save_json(option1_result, f"analytics_option1_{ts}.json")
    print("\nOption 1 (NIFTY Premium Discount) → DONE")

    # Option 2 → BankNifty Strategy
    option2_result = banknifty_option2.analyze_banknifty_option2(bn_oc, underlying_bn)
    save_json(option2_result, f"analytics_option2_{ts}.json")
    print("Option 2 (BankNifty Strategy) → DONE")

    # Option 3 → Advanced Analytics: Volume Spike + Reversal
    option3_result = analytics_option3.full_option3_analysis(nifty_oc)
    save_json(option3_result, f"analytics_option3_{ts}.json")
    print("Option 3 (Advanced Volume/Reversal) → DONE")

    # -----------------------------------------
    # Manifest summary
    # -----------------------------------------
    manifest = {
        "timestamp": ts,
        "nifty": {
            "option_chain": oc_file,
            "quote": qt_file,
            "option1": f"analytics_option1_{ts}.json",
            "option3": f"analytics_option3_{ts}.json"
        },
        "banknifty": {
            "option_chain": bn_oc_file,
            "quote": bn_qt_file,
            "option2": f"analytics_option2_{ts}.json"
        }
    }

    save_json(manifest, f"manifest_{ts}.json")
    print("\nManifest created successfully.\n")


if __name__ == "__main__":
    main()
    
