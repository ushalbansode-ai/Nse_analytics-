import sys
import os
import pandas as pd

# ---------------------------------------------------------
# FIX PYTHON PATH FOR GITHUB ACTIONS + LOCAL EXECUTION
# ---------------------------------------------------------
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT_DIR)

from src.nse_chain.fetcher import NSEOptionChainFetcher
from src.nse_chain.analytics import OptionAnalytics
from src.signals.signal_engine import detect_signal_row


# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
SYMBOL = "BANKNIFTY"      # or NIFTY
STRIKES_AROUND = 10       # fetch ±10 strikes
EXPIRY_INDEX = 0          # next expiry


# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------
if __name__ == "__main__":
    print("Fetching live option chain...")

    fetcher = NSEOptionChainFetcher()

    chain = fetcher.get_option_chain(SYMBOL)
    if chain is None:
        print("❌ Could not fetch chain")
        exit()

    print("✔ Chain fetched")

    # -------------------------------------
    # PROCESS INTO ANALYTICS FORMAT
    # -------------------------------------
    analytics = OptionAnalytics()
    df = analytics.prepare_dataframe(chain, strikes_around=STRIKES_AROUND)

    if df is None or df.empty:
        print("❌ Analytics dataframe empty")
        exit()

    print("✔ Analytics computed")

    # -------------------------------------
    # ADD SIGNALS
    # -------------------------------------
    all_signals = []
    spot_price = chain["underlyingValue"]

    for _, row in df.iterrows():
        signal, reason = detect_signal_row(row, spot_price)
        all_signals.append({
            "strike": row["strike"],
            "signal": signal,
            "reason": reason
        })

    signal_df = pd.DataFrame(all_signals)
    print("\n=========== SIGNALS ===========")
    print(signal_df[signal_df["signal"].notnull()].to_string(index=False))

    # Save
    out_path = f"data/derived_{SYMBOL}.csv"
    os.makedirs("data", exist_ok=True)
    signal_df.to_csv(out_path, index=False)

    print(f"\n✔ Saved to {out_path}")
    
    
