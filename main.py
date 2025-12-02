#!/usr/bin/env python3

import pandas as pd
from analytics import (
    calculate_oi_skew,
    detect_volume_spike,
    compute_gamma_exposure,
    detect_reversal
)

def load_sample():
    return pd.DataFrame({
        "CE_OI": [100, 120, 140],
        "PE_OI": [80, 130, 125],
        "Volume": [5000, 9000, 15000],
        "OI": [2000, 2100, 2300],
        "Gamma": [0.5, 0.6, 0.55],
        "LTP": [150, 152, 155],
        "Open": [100, 120, 155],
        "Close": [110, 115, 150]
    })

def main():
    df = load_sample()

    print("\n=== OI SKEW ===")
    print(calculate_oi_skew(df))

    print("\n=== VOLUME SPIKE ===")
    print(detect_volume_spike(df))

    print("\n=== GAMMA EXPOSURE ===")
    print(compute_gamma_exposure(df))

    print("\n=== REVERSALS ===")
    print(detect_reversal(df))

if __name__ == "__main__":
    main()
  
