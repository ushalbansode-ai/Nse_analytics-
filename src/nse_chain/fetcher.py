import requests
import pandas as pd
from datetime import datetime

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
    "Accept-Language": "en-US,en;q=0.9",
}

NSE_URL = "https://www.nseindia.com/api/option-chain-indices?symbol="

def fetch_option_chain(symbol="NIFTY"):
    """
    Fetch Option Chain from NSE (HTML JSON endpoint)
    Returns raw JSON dict.
    """
    session = requests.Session()
    session.headers.update(NSE_HEADERS)

    # First request required for cookies
    session.get("https://www.nseindia.com", timeout=5)

    url = NSE_URL + symbol.upper()
    response = session.get(url, timeout=10)

    if response.status_code != 200:
        raise RuntimeError(f"Failed NSE fetch {response.status_code}")

    data = response.json()
    return data


def parse_option_chain_to_snapshot(data, symbol="NIFTY"):
    """
    Convert NSE JSON into a clean DataFrame with EXACT columns required by analytics:
    
    timestamp, underlying, expiry, strike, option_type,
    ltp, iv, oi, oi_change, volume, ltp_prev
    """
    records = data["records"]["data"]

    rows = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for row in records:
        strike = row.get("strikePrice")
        expiry = row.get("expiryDate")

        ce = row.get("CE")
        pe = row.get("PE")

        # CE ROW
        if ce:
            rows.append({
                "timestamp": timestamp,
                "underlying": symbol,
                "expiry": expiry,
                "strike": strike,
                "option_type": "CE",
                "ltp": ce.get("lastPrice"),
                "iv": ce.get("impliedVolatility"),
                "oi": ce.get("openInterest"),
                "oi_change": ce.get("changeinOpenInterest"),
                "volume": ce.get("totalTradedVolume"),
                "ltp_prev": ce.get("prevClose"),
            })

        # PE ROW
        if pe:
            rows.append({
                "timestamp": timestamp,
                "underlying": symbol,
                "expiry": expiry,
                "strike": strike,
                "option_type": "PE",
                "ltp": pe.get("lastPrice"),
                "iv": pe.get("impliedVolatility"),
                "oi": pe.get("openInterest"),
                "oi_change": pe.get("changeinOpenInterest"),
                "volume": pe.get("totalTradedVolume"),
                "ltp_prev": pe.get("prevClose"),
            })

    df = pd.DataFrame(rows)

    return df


def fetch_snapshot(symbol="NIFTY"):
    """
    Fetch + Parse into final DataFrame ready for analytics.
    Usage:
        df = fetch_snapshot("BANKNIFTY")
    """
    raw = fetch_option_chain(symbol)
    df = parse_option_chain_to_snapshot(raw, symbol)
    return df
  
