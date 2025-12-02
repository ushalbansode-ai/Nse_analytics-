"""
NSE fetcher utilities.

Provides:
- fetch_option_chain(symbol)           -> dict (raw json)
- get_quote(symbol)                    -> dict (quote)
- get_futures(symbol)                  -> dict (raw json)
- download_fo_bhavcopy(date, out_dir)  -> local filepath (zip or csv)
- save_json(obj, path)
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
    return s

# Helper: safe GET with retries
def safe_get(session: requests.Session, url: str, params: dict | None = None, timeout: int = 15) -> requests.Response:
    last_exc = None
    for attempt in range(1, 6):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp
            else:
                last_exc = RuntimeError(f"Status {resp.status_code} for {url}")
        except Exception as e:
            last_exc = e
        time.sleep(backoff * attempt)
    raise last_exc

# Ensure directory exists
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
def fetch_option_chain(symbol: str = "NIFTY") -> Dict:
    """
    Fetch current option chain for index symbol (NIFTY/BANKNIFTY).
    Endpoint used: https://www.nseindia.com/api/option-chain-indices?symbol=SYMBOL
    """
    url = "https://www.nseindia.com/api/option-chain-indices"
    params = {"symbol": symbol}
    s = create_session()

    # Call homepage to get cookies
    try:
        s.get("https://www.nseindia.com", timeout=10)
    except Exception:
        pass

    resp = safe_get(s, url, params=params)
    return resp.json()

def get_quote(symbol: str = "NIFTY 50") -> Dict:
    """
    Fetch quote summary for an index.
    Uses option-chain underlying value.
    """
    try:
        oc = fetch_option_chain("NIFTY")
        val = oc.get("records", {}).get("underlyingValue")
        return {"underlying": val, "raw": oc}
    except Exception as e:
        return {"error": str(e)}

def get_futures(symbol: str = "NIFTY") -> Dict:
    return fetch_option_chain(symbol)

# -------------------------
# Bhavcopy FO download
# -------------------------
def download_fo_bhavcopy(dt: datetime.date | None = None, out_dir: str = "data/bhavcopy") -> Optional[str]:
    if dt is None:
        dt = datetime.date.today()

    s = create_session()
    date_ymd = dt.strftime("%Y%m%d")

    candidates = [
        f"https://archives.nseindia.com/content/nsccl/fao_participant_oi/fo_participant_oi_{date_ymd}.csv",
        f"https://archives.nseindia.com/content/nsccl/fao_participant_oi/FO_PARTICIPANT_oi_{date_ymd}.csv",
        f"https://www1.nseindia.com/content/nsccl/fao_participant_oi/fo_participant_oi_{date_ymd}.csv"
    ]

    ensure_dir(out_dir)

    for url in candidates:
        try:
            r = safe_get(s, url, timeout=20)
            if r.status_code == 200 and r.content:
                ext = ".csv"
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
    ensure_dir(out_dir)
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out = {}

    try:
        oc = fetch_option_chain(symbol)
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

    manifest_path = Path(out_dir) / f"manifest_{ts}.json"
    save_json(out, manifest_path)
    out["manifest"] = str(manifest_path)

    return out

# CLI
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="NIFTY")
    p.add_argument("--out", default="data/latest")
    args = p.parse_args()

    print("Fetching NSE data for", args.symbol)
    res = fetch_all(args.symbol, args.out)
    print("Done. Files:", res)
    
