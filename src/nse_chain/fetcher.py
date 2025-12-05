import requests
import pandas as pd
from datetime import datetime

HEADERS = {
    "user-agent": "Mozilla/5.0",
    "accept-language": "en-US,en;q=0.9",
    "accept": "application/json",
}

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

        # CE
        if "CE" in row:
            ce = row["CE"]
            rows.append({
                "timestamp": timestamp,
                "underlying": symbol,
                "expiry": ce["expiryDate"],
                "strike": strike,
                "type": "CE",
                "ltp": ce["lastPrice"],
                "oi": ce["openInterest"],
                "oi_change": ce["changeinOpenInterest"],
                "volume": ce.get("totalTradedVolume", 0),
                "ltp_prev": ce.get("prevClose", 0)
            })

        # PE
        if "PE" in row:
            pe = row["PE"]
            rows.append({
                "timestamp": timestamp,
                "underlying": symbol,
                "expiry": pe["expiryDate"],
                "strike": strike,
                "type": "PE",
                "ltp": pe["lastPrice"],
                "oi": pe["openInterest"],
                "oi_change": pe["changeinOpenInterest"],
                "volume": pe.get("totalTradedVolume", 0),
                "ltp_prev": pe.get("prevClose", 0)
            })

    return pd.DataFrame(rows)
    
