#!/usr/bin/env python3
"""
Real Fetch Example — NSE Option Chain (Updated Version)
Fixed for NSE's anti-scraping measures with better session handling
"""

import requests
import json
import time
import argparse
import pandas as pd
from pathlib import Path
import random
from typing import Optional, Dict, Any


# -------------------------------
# Session Handler (Improved)
# -------------------------------
class NSESession:
    def __init__(self):
        self.session = requests.Session()
        self._init_headers()
        self.last_cookie_refresh = 0
        self.cookie_refresh_interval = 300  # Refresh cookies every 5 minutes
        
    def _init_headers(self):
        """Initialize headers to mimic a real browser"""
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        })
    
    def _is_html_response(self, text: str) -> bool:
        """Check if response is HTML instead of JSON"""
        text_lower = text.lower().strip()
        return any(html_tag in text_lower for html_tag in ['<!doctype html', '<html', '<!doctype', '<!html'])
    
    def refresh_cookies(self, symbol: str = None):
        """Visit NSE pages to get proper cookies with realistic delays"""
        current_time = time.time()
        
        # Only refresh if enough time has passed
        if current_time - self.last_cookie_refresh < self.cookie_refresh_interval:
            return
            
        print("🔄 Refreshing NSE cookies…")
        
        try:
            # First visit home page
            self.session.get("https://www.nseindia.com", timeout=10)
            time.sleep(random.uniform(1, 2))
            
            # Visit market page
            self.session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=10)
            time.sleep(random.uniform(1, 2))
            
            # Visit options chain page (for the specific symbol if provided)
            if symbol:
                try:
                    self.session.get(f"https://www.nseindia.com/option-chain?symbol={symbol}", timeout=10)
                    time.sleep(random.uniform(1, 2))
                except:
                    pass
            
            # Visit derivatives page
            self.session.get("https://www.nseindia.com/market-data/derivatives-market", timeout=10)
            time.sleep(random.uniform(1, 2))
            
            self.last_cookie_refresh = current_time
            print("✅ Cookies refreshed successfully")
            
        except Exception as e:
            print(f"⚠️ Cookie refresh had issues: {e}")
    
    def fetch_json(self, url: str, symbol: str, attempt: int) -> Optional[Dict[str, Any]]:
        """Fetch JSON data with better error handling"""
        try:
            # Add random delay to mimic human behavior
            time.sleep(random.uniform(0.5, 1.5))
            
            # Update referer for this specific request
            headers = self.session.headers.copy()
            headers["Referer"] = "https://www.nseindia.com/option-chain"
            
            response = self.session.get(url, headers=headers, timeout=15)
            
            # Check if response is HTML (blocked)
            if self._is_html_response(response.text):
                print(f"[{symbol}] HTML blocked @ {url.split('?')[0]}..., retry {attempt}/8…")
                return None
            
            # Check if response is JSON
            if 'application/json' not in response.headers.get('content-type', '').lower():
                print(f"[{symbol}] Non-JSON response @ {url.split('?')[0]}..., retry {attempt}/8…")
                return None
            
            data = response.json()
            
            # Check for error messages in response
            if 'error' in data:
                print(f"[{symbol}] Error in response: {data.get('error', 'Unknown error')}")
                return None
            
            # Check for required 'records' field
            if "records" not in data:
                print(f"[{symbol}] No 'records' field in response @ {url.split('?')[0]}..., retry {attempt}/8…")
                return None
            
            # Check if records is empty
            if not data.get("records"):
                print(f"[{symbol}] Empty records @ {url.split('?')[0]}..., retry {attempt}/8…")
                return None
            
            return data
            
        except json.JSONDecodeError:
            print(f"[{symbol}] Invalid JSON @ {url.split('?')[0]}..., retry {attempt}/8…")
            return None
        except requests.exceptions.Timeout:
            print(f"[{symbol}] Timeout @ {url.split('?')[0]}..., retry {attempt}/8…")
            return None
        except requests.exceptions.RequestException as e:
            print(f"[{symbol}] Request failed: {e}")
            return None
        except Exception as e:
            print(f"[{symbol}] Unexpected error: {e}")
            return None
    
    def get_chain(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Main fetch method with retries + fallback"""
        
        # Define URLs in order of priority
        urls = [
            f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}",
            # Try alternative endpoints with different parameters
            f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}&expiryDate=",
            f"https://www.nseindia.com/api/option-chain?symbol={symbol}",
        ]
        
        # Refresh cookies before starting
        self.refresh_cookies(symbol)
        
        for attempt in range(1, 9):
            print(f"[{symbol}] Attempt {attempt}/8…")
            
            # Refresh cookies every 2 attempts
            if attempt % 2 == 0:
                self.refresh_cookies(symbol)
            
            for url in urls:
                data = self.fetch_json(url, symbol, attempt)
                if data:
                    print(f"[{symbol}] ✓ Data received successfully from {url.split('/')[-1].split('?')[0]}")
                    return data
            
            # Exponential backoff with jitter
            wait_time = min(2 ** attempt + random.uniform(0, 1), 10)
            print(f"[{symbol}] Waiting {wait_time:.1f}s before next attempt...")
            time.sleep(wait_time)
        
        print(f"[{symbol}] ❌ FAILED after 8 attempts")
        return None


# ------------------------------
# Data Processing
# ------------------------------
def convert_to_dataframe(js: Dict[str, Any], symbol: str) -> pd.DataFrame:
    """Convert JSON response to DataFrame"""
    if js is None or "records" not in js:
        print(f"[{symbol}] Empty JSON → returning empty dataframe")
        return pd.DataFrame()
    
    rows = []
    
    # Check if we have data in the expected format
    if "data" not in js["records"]:
        print(f"[{symbol}] No 'data' field in records")
        return pd.DataFrame()
    
    for item in js["records"]["data"]:
        # Handle different response formats
        ce = item.get("CE", {})
        pe = item.get("PE", {})
        
        # Skip if both CE and PE are empty
        if not ce and not pe:
            continue
        
        row = {
            "symbol": symbol,
            "strikePrice": item.get("strikePrice"),
            "expiryDate": item.get("expiryDate"),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        
        # Add CE data if available
        if ce:
            row.update({
                "ce_oi": ce.get("openInterest"),
                "ce_change_oi": ce.get("changeinOpenInterest"),
                "ce_ltp": ce.get("lastPrice"),
                "ce_volume": ce.get("totalTradedVolume"),
                "ce_iv": ce.get("impliedVolatility"),
                "ce_delta": ce.get("delta"),
                "ce_theta": ce.get("theta"),
                "ce_vega": ce.get("vega"),
                "ce_gamma": ce.get("gamma"),
            })
        
        # Add PE data if available
        if pe:
            row.update({
                "pe_oi": pe.get("openInterest"),
                "pe_change_oi": pe.get("changeinOpenInterest"),
                "pe_ltp": pe.get("lastPrice"),
                "pe_volume": pe.get("totalTradedVolume"),
                "pe_iv": pe.get("impliedVolatility"),
                "pe_delta": pe.get("delta"),
                "pe_theta": pe.get("theta"),
                "pe_vega": pe.get("vega"),
                "pe_gamma": pe.get("gamma"),
            })
        
        rows.append(row)
    
    if not rows:
        print(f"[{symbol}] No valid option data found")
        return pd.DataFrame()
    
    df = pd.DataFrame(rows)
    print(f"[{symbol}] Generated DataFrame with {len(df)} rows")
    return df


# ------------------------------
# Save results
# ------------------------------
def save_output(df: pd.DataFrame, symbol: str):
    """Save DataFrame to CSV"""
    Path("output").mkdir(exist_ok=True)
    
    out_path = f"output/{symbol}_option_chain.csv"
    df.to_csv(out_path, index=False)
    
    print(f"[{symbol}] Saved → {out_path} ({len(df)} rows)")
    
    # Also save a sample for quick viewing
    sample_path = f"output/{symbol}_sample.csv"
    df.head(20).to_csv(sample_path, index=False)
    print(f"[{symbol}] Sample → {sample_path}")


# ------------------------------
# MAIN
# ------------------------------
def main():
    parser = argparse.ArgumentParser(description="Fetch NSE option chain data")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["NIFTY", "BANKNIFTY"],
        help="Symbols to fetch (default: NIFTY BANKNIFTY)"
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Output directory for CSV files (default: output)"
    )
    args = parser.parse_args()
    
    print(f"Starting NSE Option Chain Fetcher")
    print(f"Symbols: {', '.join(args.symbols)}")
    print(f"Output directory: {args.output_dir}")
    print("-" * 50)
    
    session = NSESession()
    all_data = {}
    
    for sym in args.symbols:
        print(f"\n{'='*50}")
        print(f"Fetching → {sym}")
        print(f"{'='*50}")
        
        js = session.get_chain(sym)
        df = convert_to_dataframe(js, sym)
        
        if df.empty:
            print(f"[{sym}] ⚠ No data fetched - trying fallback approach...")
            # Try alternative symbol naming (sometimes lowercase works)
            if sym.isupper():
                alt_sym = sym.lower()
                print(f"[{sym}] Trying alternative symbol: {alt_sym}")
                js = session.get_chain(alt_sym)
                df = convert_to_dataframe(js, sym)
        
        if df.empty:
            print(f"[{sym}] ❌ Failed to fetch data")
            continue
        
        # Save individual symbol data
        save_output(df, sym)
        all_data[sym] = df
        
        # Display summary
        print(f"\n[{sym}] Data Summary:")
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {len(df.columns)}")
        print(f"  Expiry Dates: {df['expiryDate'].unique()[:3]}")  # Show first 3 expiry dates
        print(f"  Strike Range: {df['strikePrice'].min()} - {df['strikePrice'].max()}")
        
        # Show sample
        print(f"\n[{sym}] First 5 rows:")
        print(df[['strikePrice', 'expiryDate', 'ce_ltp', 'pe_ltp', 'ce_oi', 'pe_oi']].head())
    
    # Save combined data if multiple symbols
    if len(all_data) > 1:
        combined_df = pd.concat(all_data.values(), ignore_index=True)
        combined_path = f"{args.output_dir}/combined_option_chains.csv"
        combined_df.to_csv(combined_path, index=False)
        print(f"\n✅ Combined data saved → {combined_path}")
        print(f"   Total rows: {len(combined_df)}")
    
    print(f"\n{'='*50}")
    print("Fetching complete!")
    print(f"{'='*50}")


# ------------------------------
# ENTRY
# ------------------------------
if __name__ == "__main__":
    main()
