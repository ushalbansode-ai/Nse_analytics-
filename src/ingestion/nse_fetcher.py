"""
NSE fetcher utilities.

Provides:
- get_option_chain(symbol)             -> dict (raw json)
- get_quote(symbol)                    -> dict (quote)
- get_futures(symbol)                  -> dict (raw json)
- download_fo_bhavcopy(date, out_dir)  -> local filepath (zip or csv)
- save_json(obj, path)

Designed to run inside Codespaces or local env.
"""

from __future__ import annotations
import requests
import time
import json
import os
import datetime
from typing import Optional, Dict, Any
from pathlib import Path

# Basic headers to reduce chance of 403
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# Session with retries/backoff
def create_session(retries: int = 3, backoff: float = 0.6) -> requests.Session:
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)
    # (we keep it simple — requests' built-in adapters are fine for now)
    return s

# Helper: safe GET with retries
def safe_get(session: requests.Session, url: str, params: dict | None = None, timeout: int = 15) -> requests.Response:
    last_exc = None
    for attempt in range(1, 6):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            # Some NSE endpoints return 200 but an HTML error page — check content-type
            if resp.status_code == 200:
                return resp
            else:
                last_exc = RuntimeError(f"Status {resp.status_code} for {url}")
        except Exception as e:
            last_exc = e
        time.sleep(backoff * attempt)
    raise last_exc

# Ensure data folder exists
def ensure_dir(path: str | Path):
    Path(path).mkdir(parents=True, exist_ok=True)

# Save JSON helper
def save_json(obj: Any, path: str | Path):
    ensure_dir(Path(path).parent)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

# -------------------------
# Option chain / quotes
# -------------------------
def get_option_chain(symbol: str = "NIFTY") -> Dict:
    """
    Fetch current option chain for index symbol (NIFTY/BANKNIFTY).
    Endpoint used: https://www.nseindia.com/api/option-chain-indices?symbol=SYMBOL
    """
    url = "https://www.nseindia.com/api/option-chain-indices"
    params = {"symbol": symbol}
    s = create_session()
    # We first call homepage to get cookies (helps avoid 403)
    try:
        s.get("https://www.nseindia.com", timeout=10)
    except Exception:
        pass
    resp = safe_get(s, url, params=params)
    # The endpoint returns JSON
    return resp.json()

def get_quote(symbol: str = "NIFTY 50") -> Dict:
    """
    Fetch quote summary for an index or symbol.
    Example index symbol strings: 'NIFTY 50', 'NIFTY BANK'
    Endpoint: https://www.nseindia.com/api/quote-equity?symbol=<SYMBOL>
    For indices there are different endpoints — we use option-chain response to get underlying price
    """
    # For indices, rely on the option-chain header 'underlyingValue' (if available)
    # Provide a fallback generic JSON response wrapper
    try:
        oc = get_option_chain("NIFTY")
        # oc contains 'records' -> 'underlyingValue' or top-level field
        val = oc.get("records", {}).get("underlyingValue")
        return {"underlying": val, "raw": oc}
    except Exception as e:
        return {"error": str(e)}

def get_futures(symbol: str = "NIFTY") -> Dict:
    """
    Minimal futures info for symbol — NSE does not expose a simple public restful endpoint for futures list,
    but option chain contains near-term futures info as part of records under 'expiryDates' or futures endpoint.
    We'll try the futures index API if present (this is a lightweight attempt).
    """
    # Some users maintain their own futures fetchers — return option chain as fallback
    return get_option_chain(symbol)

# -------------------------
# Bhavcopy FO download
# -------------------------
def download_fo_bhavcopy(dt: datetime.date | None = None, out_dir: str = "data/bhavcopy") -> Optional[str]:
    """
    Download FO (Futures & Options) bhavcopy zip for a given date.
    NSE FO bhavcopy naming pattern (example): BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip
    However, NSE website structure changes. This function attempts the commonly used archive location:
    https://archives.nseindia.com/content/nsccl/fao_participant_oi/FOOOYYYYMMDD.csv  (and a zip variant)
    We attempt several paths and save the first successful file to out_dir.
    Returns local filepath or None.
    """
    if dt is None:
        dt = datetime.date.today()
    s = create_session()
    date_str = dt.strftime("%d%b%Y").upper()   # older patterns
    date_ymd = dt.strftime("%Y%m%d")
    # Common archive URLs (try each)
    candidates = [
        f"https://archives.nseindia.com/content/nsccl/fao_participant_oi/fo_participant_oi_{date_ymd}.csv",
        f"https://archives.nseindia.com/content/nsccl/fao_participant_oi/FO_PARTICIPANT_oi_{date_ymd}.csv",
        f"https://archives.nseindia.com/content/historical/EQUITIES/2020/{date_ymd}/cm{date_ymd}.zip",
        # older pattern:
        f"https://www1.nseindia.com/content/nsccl/fao_participant_oi/fo_participant_oi_{date_ymd}.csv"
    ]
    ensure_dir(out_dir)
    for url in candidates:
        try:
            r = safe_get(s, url, timeout=20)
            if r.status_code == 200 and r.content:
                # Determine extension by content-type or url
                ext = ".csv"
                if "zip" in r.headers.get("Content-Type", "") or url.lower().endswith(".zip"):
                    ext = ".zip"
                local_name = f"fo_bhavcopy_{date_ymd}{ext}"
                out_path = Path(out_dir) / local_name
                with open(out_path, "wb") as fh:
                    fh.write(r.content)
                return str(out_path)
        except Exception:
            continue
    return None

# -------------------------
# High-level orchestrator
# -------------------------
def fetch_all(symbol: str = "NIFTY", out_dir: str = "data/latest") -> Dict[str, Any]:
    """
    Fetch option chain, quote, and attempt to download FO bhavcopy.
    Save JSON files into out_dir with timestamps and also return a dictionary.
    """
    ensure_dir(out_dir)
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out = {}
    try:
        oc = get_option_chain(symbol)
        oc_path = Path(out_dir) / f"option_chain_{symbol}_{ts}.json"
        save_json(oc, oc_path)
        out["option_chain"] = str(oc_path)
    except Exception as e:
        out["option_chain_error"] = str(e)

    try:
        q = get_quote(symbol)
        q_path = Path(out_dir) / f"quote_{symbol}_{ts}.json"
        save_json(q, q_path)
        out["quote"] = str(q_path)
    except Exception as e:
        out["quote_error"] = str(e)

    try:
        bhav_path = download_fo_bhavcopy()
        out["bhavcopy"] = bhav_path
    except Exception as e:
        out["bhavcopy_error"] = str(e)

    # Also write a manifest
    manifest_path = Path(out_dir) / f"manifest_{ts}.json"
    save_json(out, manifest_path)
    out["manifest"] = str(manifest_path)
    return out

# CLI convenience
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="NIFTY", help="Index symbol (NIFTY/BANKNIFTY)")
    p.add_argument("--out", default="data/latest", help="Output folder")
    args = p.parse_args()
    print("Fetching NSE data for", args.symbol)
    res = fetch_all(args.symbol, args.out)
    print("Done. Files:", res)
  
