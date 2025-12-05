#!/usr/bin/env python3
# Real-time NSE Option Chain Fetch + Signal Generator

import os
import pandas as pd

# ---------------------------------------------------------
# Correct Imports (relative package import)
# ---------------------------------------------------------
from nse_chain.fetcher import fetch_snapshot
from signals.signal_engine import detect_signal_row


# ---------------------------------------------------------
# OUTPUT FOLDER
# ---------------------------------------------------------
OUT = "signals_output"
os.makedirs(OUT, exist_ok=True)


# ---------------------------------------------------------
# PROCESS ONE SYMBOL
# ---------------------------------------------------------
def process_symbol(symbol):
    print(f"\nFetching → {symbol}")

    snap = fetch_snapshot(symbol)
    if snap is None or "records" not in snap:
        print(f"❌ No valid data for {symbol}")
        return None

    df = pd.DataFrame(snap["records"])
    if df.empty:
        print(f"❌ Empty data for {symbol}")
        return None

    # Ensure numeric
    numeric_cols = [
        "price_change_CE","price_change_PE",
        "oi_change_CE","oi_change_PE",
        "oi_diff","oi_diff_prev",
        "iv_ce","iv_pe"
    ]
    for col in numeric_cols:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Apply signals
    df["signal"] = None
    df["reason"] = ""

    for i, row in df.iterrows():
        sig, rsn = detect_signal_row(row, row.get("spot", None))
        df.loc[i, "signal"] = sig
        df.loc[i, "reason"] = rsn

    return df


# ---------------------------------------------------------
# SAVE SIGNALS
# ---------------------------------------------------------
def save_signals(df_dict):
    frames = []

    for sym, df in df_dict.items():
        if df is None:
            continue

        # Filter signals
        sig_df = df[df["signal"].notna()].copy()
        sig_df["symbol"] = sym
        frames.append(sig_df)

    # Final combined file
    out_file = os.path.join(OUT, "signals.csv")

    if not frames:
        print("⚠ No signals detected — writing empty CSV")
        pd.DataFrame(columns=["symbol", "signal", "reason"]).to_csv(out_file, index=False)
        return

    final_df = pd.concat(frames, ignore_index=True)
    final_df.to_csv(out_file, index=False)
    print(f"✅ Signals saved → {out_file}")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    symbols = ["NIFTY", "BANKNIFTY"]
    results = {}

    for s in symbols:
        results[s] = process_symbol(s)

    save_signals(results)
    print("\n🎯 Completed real-time fetch + signal generation.\n")


if __name__ == "__main__":
    main()
    
