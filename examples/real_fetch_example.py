import sys
import os
import pandas as pd
import requests
from datetime import datetime

# Import signal engine
from signals.signal_engine import detect_signal_row

# Allow absolute imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

HEADERS = {
    "user-agent": "Mozilla/5.0",
    "accept-language": "en-US,en;q=0.9",
    "accept": "application/json",
}


# -----------------------------
# Fetch NSE snapshot
# -----------------------------
def fetch_snapshot(symbol: str):
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"

    session = requests.Session()
    session.get("https://www.nseindia.com", headers=HEADERS)
    res = session.get(url, headers=HEADERS).json()

    records = res["records"]["data"]

    rows = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for row in records:
        strike = row["strikePrice"]

        ce = row.get("CE", {})
        pe = row.get("PE", {})

        rows.append({
            "timestamp": timestamp,
            "symbol": symbol,
            "strike": strike,

            # CE
            "ltp_CE": ce.get("lastPrice"),
            "ltp_prev_CE": ce.get("prevClose"),
            "oi_CE": ce.get("openInterest"),
            "oi_change_CE": ce.get("changeinOpenInterest"),
            "iv_ce": ce.get("impliedVolatility"),

            # PE
            "ltp_PE": pe.get("lastPrice"),
            "ltp_prev_PE": pe.get("prevClose"),
            "oi_PE": pe.get("openInterest"),
            "oi_change_PE": pe.get("changeinOpenInterest"),
            "iv_pe": pe.get("impliedVolatility")
        })

    return pd.DataFrame(rows)


# -----------------------------
# Preprocess required columns
# -----------------------------
def preprocess(df):
    if df.empty:
        return df

    # Price changes
    df["price_change_CE"] = df["ltp_CE"] - df["ltp_prev_CE"]
    df["price_change_PE"] = df["ltp_PE"] - df["ltp_prev_PE"]

    # OI difference & shift
    df["oi_diff"] = df["oi_CE"] - df["oi_PE"]
    df["oi_diff_prev"] = df["oi_diff"].shift(1).fillna(0)

    return df


# -----------------------------
# Process one symbol
# -----------------------------
def process_symbol(symbol):
    print(f"\nFetching → {symbol}")

    df = fetch_snapshot(symbol)

    if df.empty:
        print(f"❌ No valid data for {symbol}")
        return pd.DataFrame()

    df = preprocess(df)

    signals = []
    spot = df["strike"].iloc[len(df)//2]  # crude approx

    for _, row in df.iterrows():
        sig, reason = detect_signal_row(row, spot)
        if sig:
            signals.append({
                "timestamp": row["timestamp"],
                "symbol": symbol,
                "strike": row["strike"],
                "signal": sig,
                "reason": reason
            })

    sig_df = pd.DataFrame(signals)

    # Write signals
    out_path = f"signals_{symbol}.csv"
    sig_df.to_csv(out_path, index=False)
    print(f"📁 Written {out_path}")

    return sig_df


# -----------------------------
# Main
# -----------------------------
def main():
    symbols = ["NIFTY", "BANKNIFTY"]

    for sym in symbols:
        process_symbol(sym)

    print("\n🎯 Completed real-time fetch + signal generation.")


if __name__ == "__main__":
    main()
    
