# ===========================================================
# MarketData.app GEX Backfill Utility (Debug/Fix Mode)
# ===========================================================
# PURPOSE: Generates HISTORICAL data for Bar Replay.
# UPDATES: Added verbose debugging to identify why rows are skipped.
# ===========================================================

import os
import time
import re
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ===============================================
# Configuration
# ===============================================
API_KEY = os.getenv("MARKETDATA_KEY") or ""
BASE_URL = "https://api.marketdata.app/v1"

# Default to 30 days unless specified by Workflow
DAYS_TO_BACKFILL = int(os.getenv("DAYS_TO_BACKFILL", 30))

MAX_OPTIONS = 1000   
STRIKE_RANGE_PCT = 0.15 
TICKERS = ["SPY", "QQQ", "IWM"] 

print(f"🚀 Starting GEX Backfill (Debug Mode) for last {DAYS_TO_BACKFILL} days...")

# ===============================================
# Helper Functions
# ===============================================
def get_historical_price(symbol, date_str):
    # Try candle endpoint first
    url = f"{BASE_URL}/stocks/candles/D/{symbol}?from={date_str}&to={date_str}&token={API_KEY}"
    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            data = r.json()
            if data.get("s") == "ok" and "c" in data:
                return float(data["c"][0])
    except: pass
    return None

def get_historical_chain(symbol, date_str):
    url = f"{BASE_URL}/options/chain/{symbol}?date={date_str}&token={API_KEY}"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code in (200, 203):
            data = r.json()
            if data.get("s") == "ok":
                return data.get("optionSymbol", [])
    except Exception as e:
        print(f"   ❌ Chain error {date_str}: {e}")
    return []

def get_historical_quote(option_symbol, date_str):
    # For historical quotes, we use the quotes endpoint with a date parameter
    url = f"{BASE_URL}/options/quotes/{option_symbol}?date={date_str}&token={API_KEY}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code in (200, 203):
            return r.json()
    except: pass
    return None

def parse_option_symbol(symbol):
    match = re.search(r'([A-Z]+)(\d{6})([CP])(\d+)', symbol)
    if match:
        expiry = match.group(2)
        strike = int(match.group(4)) / 1000.0
        return expiry, strike
    return "999999", 0.0

def infer_option_type(symbol_str):
    if symbol_str.endswith("C"): return "C"
    if symbol_str.endswith("P"): return "P"
    return "C" if "C" in symbol_str else "P"

def safe_extract(d, keys):
    if not isinstance(d, dict): return None
    for k in keys:
        if k in d and d[k] is not None:
            val = d[k]
            # Handle list returns if API wraps value in list
            if isinstance(val, list) and len(val) > 0:
                return val[0]
            # Return value directly if it's a number/string
            if not isinstance(val, (list, dict)): 
                return val
    return None

# ===============================================
# Core Builder
# ===============================================
def build_day(symbol, target_date):
    date_str = target_date.strftime("%Y-%m-%d")
    file_tag = target_date.strftime("%Y%m%d")
    fname = f"{symbol}_GEX_robust_{file_tag}.csv"

    if os.path.exists(fname):
        print(f"   ⏭️  Skipping {date_str} (File exists)")
        return

    print(f"   📅 Fetching History for {date_str}...")

    spot_price = get_historical_price(symbol, date_str)
    if not spot_price:
        print(f"      ⚠️ No price data (Holiday/Weekend?).")
        return 

    raw_chain = get_historical_chain(symbol, date_str)
    if not raw_chain:
        print("      No chain data.")
        return

    # Filter Strikes
    filtered_opts = []
    for sym in raw_chain:
        _, strike = parse_option_symbol(sym)
        low = spot_price * (1 - STRIKE_RANGE_PCT)
        high = spot_price * (1 + STRIKE_RANGE_PCT)
        if low <= strike <= high:
            filtered_opts.append(sym)
    
    final_list = filtered_opts[:MAX_OPTIONS]
    print(f"      Processing {len(final_list)} options...")

    rows = []
    missing_greeks_count = 0
    
    for i, opt in enumerate(final_list):
        q = get_historical_quote(opt, date_str)
        if not q: continue
        
        try:
            # Try multiple keys for robustness
            gamma = safe_extract(q, ["gamma", "gam", "g"])
            oi = safe_extract(q, ["openInterest", "open_interest", "oi"])
            underlying = safe_extract(q, ["underlyingPrice", "underlying"]) or spot_price

            # DEBUG: Print the first failure to see what the API returns
            if (gamma is None or oi is None) and missing_greeks_count == 0:
                print(f"      [DEBUG] Sample Missing Data for {opt}:")
                print(f"      Response: {q}")
                print(f"      Extracted -> Gamma: {gamma}, OI: {oi}")
                missing_greeks_count += 1

            if gamma is None or oi is None: 
                missing_greeks_count += 1
                continue

            gex = float(gamma) * float(oi) * 100 * float(underlying)
            _, strike = parse_option_symbol(opt)
            
            rows.append({
                "strike": float(strike),
                "GEX": gex,
                "type": infer_option_type(opt)
            })
        except Exception as e:
            if i == 0: print(f"      [DEBUG] Exception on row: {e}")
            continue
        
        if i % 50 == 0: time.sleep(0.05)

    if missing_greeks_count > 0:
        print(f"      ⚠️ Skipped {missing_greeks_count} options due to missing Gamma/OI.")

    if not rows:
        print(f"      ❌ No valid rows generated for {date_str}.")
        return

    # Save CSV
    df = pd.DataFrame(rows)
    grouped = df.groupby(["strike", "type"])["GEX"].sum().unstack(fill_value=0)
    grouped.rename(columns={"C": "call_gex", "P": "put_gex"}, inplace=True)
    
    if "call_gex" not in grouped.columns: grouped["call_gex"] = 0.0
    if "put_gex" not in grouped.columns: grouped["put_gex"] = 0.0

    grouped["net_gex"] = grouped["call_gex"] - grouped["put_gex"]
    
    grouped.reset_index().to_csv(fname, index=False)
    print(f"      ✅ Saved {fname} ({len(grouped)} strikes)")

# ===============================================
# Loop Last N Days
# ===============================================
today = datetime.now()

for i in range(1, DAYS_TO_BACKFILL + 1):
    past_date = today - timedelta(days=i)
    if past_date.weekday() >= 5: 
        continue
        
    print(f"\nProcessing Backfill Day {i}/{DAYS_TO_BACKFILL} ({past_date.strftime('%Y-%m-%d')})")
    for ticker in TICKERS:
        try:
            build_day(ticker, past_date)
        except Exception as e:
            print(f"❌ Error {ticker}: {e}")

print("\n🏁 Backfill Complete.")
