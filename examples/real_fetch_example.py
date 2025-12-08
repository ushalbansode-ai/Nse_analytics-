#!/usr/bin/env python3
"""
Simpler NSE Option Chain Fetcher
Uses reliable public APIs
"""

import requests
import pandas as pd
import json
from datetime import datetime
import argparse


def fetch_nse_option_chain(symbol):
    """
    Fetch option chain data using a more reliable method
    """
    # Map symbols to their Yahoo Finance equivalents
    symbol_map = {
        "NIFTY": "^NSEI",
        "BANKNIFTY": "^NSEBANK",
    }
    
    yahoo_symbol = symbol_map.get(symbol, symbol)
    
    try:
        # Method 1: Try Yahoo Finance via RapidAPI
        print(f"[{symbol}] Trying Yahoo Finance API...")
        
        # You'll need to sign up for a free API key at rapidapi.com
        # Replace with your actual API key
        api_key = "your-rapidapi-key-here"  # Get from https://rapidapi.com/apidojo/api/yahoo-finance1/
        
        headers = {
            'x-rapidapi-key': api_key,
            'x-rapidapi-host': 'yahoo-finance1.p.rapidapi.com'
        }
        
        # Get option chain from Yahoo Finance
        url = f"https://yahoo-finance1.p.rapidapi.com/stock/v2/get-options"
        params = {
            'symbol': yahoo_symbol,
            'date': '1704067200'  # Example timestamp
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return process_yahoo_data(data, symbol)
    
    except Exception as e:
        print(f"[{symbol}] Yahoo Finance failed: {e}")
    
    # Method 2: Use Indian indices API
    try:
        print(f"[{symbol}] Trying indices API...")
        
        # Free public API for Indian indices
        if symbol == "NIFTY":
            url = "https://api.indiaoptions.in/api/nifty"
        elif symbol == "BANKNIFTY":
            url = "https://api.indiaoptions.in/api/banknifty"
        else:
            return None
        
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return process_indiaoptions_data(data, symbol)
            
    except Exception as e:
        print(f"[{symbol}] Indices API failed: {e}")
    
    return None


def process_yahoo_data(data, symbol):
    """Process Yahoo Finance data"""
    rows = []
    
    if 'optionChain' in data and 'result' in data['optionChain']:
        result = data['optionChain']['result'][0]
        options = result.get('options', [])
        
        for option in options:
            calls = option.get('calls', [])
            puts = option.get('puts', [])
            
            for call, put in zip(calls[:20], puts[:20]):  # Limit to first 20
                rows.append({
                    'symbol': symbol,
                    'strikePrice': call.get('strike', 0),
                    'expiryDate': datetime.fromtimestamp(option.get('expiration', 0)).strftime('%Y-%m-%d'),
                    'ce_ltp': call.get('lastPrice', 0),
                    'pe_ltp': put.get('lastPrice', 0),
                    'ce_oi': call.get('openInterest', 0),
                    'pe_oi': put.get('openInterest', 0),
                    'ce_volume': call.get('volume', 0),
                    'pe_volume': put.get('volume', 0),
                })
    
    return pd.DataFrame(rows)


def process_indiaoptions_data(data, symbol):
    """Process IndiaOptions API data"""
    rows = []
    
    if isinstance(data, list):
        for item in data[:50]:  # Limit to 50 rows
            rows.append({
                'symbol': symbol,
                'strikePrice': item.get('strike', 0),
                'expiryDate': item.get('expiry', ''),
                'ce_ltp': item.get('call_ltp', 0),
                'pe_ltp': item.get('put_ltp', 0),
                'ce_oi': item.get('call_oi', 0),
                'pe_oi': item.get('put_oi', 0),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            })
    
    return pd.DataFrame(rows)


def create_sample_data(symbol):
    """Create sample data for demonstration"""
    import numpy as np
    
    rows = []
    base_price = 22000 if symbol == "NIFTY" else 48000
    
    for i in range(-10, 11):
        strike = base_price + (i * 100)
        
        rows.append({
            'symbol': symbol,
            'strikePrice': strike,
            'expiryDate': '2024-12-26',
            'ce_ltp': max(5, abs(base_price - strike) * 0.5),
            'pe_ltp': max(5, abs(strike - base_price) * 0.5),
            'ce_oi': 10000 + abs(i) * 1000,
            'pe_oi': 10000 + abs(i) * 1000,
            'ce_volume': 500 + abs(i) * 50,
            'pe_volume': 500 + abs(i) * 50,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        })
    
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbols', nargs='+', default=['NIFTY', 'BANKNIFTY'])
    parser.add_argument('--sample', action='store_true', help='Use sample data')
    args = parser.parse_args()
    
    for symbol in args.symbols:
        print(f"\nFetching {symbol}...")
        
        if args.sample:
            df = create_sample_data(symbol)
            print(f"Generated sample data for {symbol}")
        else:
            df = fetch_nse_option_chain(symbol)
            if df is None or df.empty:
                print(f"No data for {symbol}, generating sample...")
                df = create_sample_data(symbol)
        
        if not df.empty:
            # Save to CSV
            df.to_csv(f"{symbol}_options.csv", index=False)
            print(f"Saved {len(df)} rows to {symbol}_options.csv")
            
            # Display
            print("\nFirst 10 rows:")
            print(df[['strikePrice', 'ce_ltp', 'pe_ltp', 'ce_oi', 'pe_oi']].head(10).to_string())


if __name__ == "__main__":
    main()
