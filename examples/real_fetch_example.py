"""
Real NSE Fetch + Signal Engine
"""

import pandas as pd
from src.nse_chain.fetcher import fetch_snapshot
from src.signals.signal_engine import detect_signal_row

INDEX_LIST = ["NIFTY", "BANKNIFTY"]

OUTPUT = {
    "NIFTY": "data/derived_NIFTY.csv",
    "BANKNIFTY": "data/derived_BANKNIFTY.csv",
    "COMBINED": "data/derived_latest.csv",
}


def prepare_option_chain(df):
    """
    Convert CE/PE rows into single-row per strike.
    """

    ce = df[df["type"] == "CE"].copy()
    pe = df[df["type"] == "PE"].copy()

    merged = ce.merge(
        pe,
        on=["timestamp", "underlying", "expiry", "strike"],
        suffixes=("_CE", "_PE"),
        how="inner"
    )

    # Compute changes
    merged["price_change_CE"] = merged["ltp_CE"] - merged["ltp_prev_CE"]
    merged["price_change_PE"] = merged["ltp_PE"] - merged["ltp_prev_PE"]

    merged["oi_diff"] = merged["oi_CE"] - merged["oi_PE"]
    merged["oi_diff_prev"] = merged["oi_change_CE"] - merged["oi_change_PE"]

    # IV default (NSE doesn't provide IV in this API)
    merged["iv_ce"] = 10
    merged["iv_pe"] = 10

    return merged


def process_symbol(symbol):
    print(f"Fetching → {symbol}")

    raw = fetch_snapshot(symbol)
    if raw.empty:
        print(f"❌ No data received for {symbol}")
        return None

    df = prepare_option_chain(raw)

    spot = df["strike"].iloc[df["ltp_CE"].argmax()]  # rough spot proxy
    signals = []

    for _, row in df.iterrows():
        sig, reason = detect_signal_row(row, spot)
        signals.append(sig if sig else "NO_SIGNAL")

    df["signal"] = signals

    return df


def main():
    all_dfs = []

    for symbol in INDEX_LIST:
        processed = process_symbol(symbol)
        if processed is not None:
            all_dfs.append(processed)
            processed.to_csv(OUTPUT[symbol], index=False)
            print(f"✔ Saved {OUTPUT[symbol]}")

    # Combined file
    if all_dfs:
        final = pd.concat(all_dfs)
        final.to_csv(OUTPUT["COMBINED"], index=False)
        print(f"✔ Saved {OUTPUT['COMBINED']}")


if __name__ == "__main__":
    main()
    
