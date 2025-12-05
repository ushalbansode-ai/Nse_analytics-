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
    pivot = df.pivot_table(
        index=["strike"],
        columns="option_type",
        values=["oi", "oi_change", "ltp", "ltp_prev"],
        aggfunc="first"
    )

    pivot.columns = ["_".join(c).strip() for c in pivot.columns]
    pivot = pivot.reset_index()

    pivot["ce_price_change"] = pivot["ltp_CE"] - pivot["ltp_prev_CE"]
    pivot["pe_price_change"] = pivot["ltp_PE"] - pivot["ltp_prev_PE"]

    pivot["total_oi"] = pivot["oi_CE"] + pivot["oi_PE"]
    pivot["oi_diff"] = pivot["oi_CE"] - pivot["oi_PE"]
    pivot["oi_ratio"] = pivot["oi_CE"] / (pivot["oi_PE"] + 1)

    pivot["ce_signal"] = pivot.apply(
        lambda r: classify_build_up(r["ce_price_change"], r["oi_change_CE"]), axis=1
    )
    pivot["pe_signal"] = pivot.apply(
        lambda r: classify_build_up(r["pe_price_change"], r["oi_change_PE"]), axis=1
    )

    pivot["oi_signal"] = pivot["oi_diff"].apply(
        lambda x: "CE_Dominant" if x > 0 else "PE_Dominant"
    )

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
  
