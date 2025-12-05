import numpy as np
import pandas as pd

def classify_build_up(price_change, oi_change):
    if price_change > 0 and oi_change > 0:
        return "long_build_up"
    if price_change < 0 and oi_change > 0:
        return "short_build_up"
    if price_change > 0 and oi_change < 0:
        return "short_covering"
    if price_change < 0 and oi_change < 0:
        return "long_unwinding"
    return "neutral"

def compute_oi_differences(df):
    # Ensure required columns exist
    required = {"strike", "type", "ltp", "oi", "oi_change", "ltp_prev"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Pivot CE/PE side-by-side
    pivot = df.pivot_table(index="strike", columns="type", values=["ltp", "ltp_prev", "oi", "oi_change"], aggfunc="first")

    # Flatten multi-index columns
    pivot.columns = [f"{c[0]}_{c[1]}" for c in pivot.columns]

    # Fill missing CE/PE entries with 0
    for col in ["ltp_CE", "ltp_PE", "oi_CE", "oi_PE", "oi_change_CE", "oi_change_PE", "ltp_prev_CE", "ltp_prev_PE"]:
        if col not in pivot.columns:
            pivot[col] = 0

    # Compute price change
    pivot["price_change_CE"] = pivot["ltp_CE"] - pivot["ltp_prev_CE"]
    pivot["price_change_PE"] = pivot["ltp_PE"] - pivot["ltp_prev_PE"]

    # Compute OI difference
    pivot["oi_diff"] = pivot["oi_CE"] - pivot["oi_PE"]

    pivot.reset_index(inplace=True)
    return pivot
    

def compute_oi_magnets_and_gaps(df, spot, band=1000, gap_threshold=0.3):
    df = df.copy()
    df["distance"] = (df["strike"] - spot).abs()
    df = df[df["distance"] <= band]

    df["magnet_score"] = df["total_oi"] / (df["distance"] + 1)

    magnets = df.sort_values("magnet_score", ascending=False).head(10)

    median_oi = df["total_oi"].median()
    gaps = df[df["total_oi"] < median_oi * gap_threshold]

    return magnets, gaps
  
