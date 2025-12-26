# ===========================================================
# MarketData.app GEX Backfill Utility (History Builder)
# ===========================================================
# PURPOSE: Generates HISTORICAL data for Bar Replay.
# 
# Usage:
# 1. Change DAYS_TO_BACKFILL to how far back you want to go.
# 2. Run this script LOCALLY on your computer once.
# 3. It will generate CSVs for past dates (e.g., 20241101, 20241102).
# 4. Run the "gex_to_pinescript_converter.py" afterwards to merge.
#
# NOTE: gex_builder.py handles "Today". This script handles "Yesterday" backwards.
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

# 🛠️ CHANGE THIS NUMBER to backtest further!
# 30 days covers about 1.5 months of trading.
# 60 days covers about 3 months.
# WARNING: Going back >90 days consumes a lot of API credits.
DAYS_TO_BACKFILL = 30 

MAX_OPTIONS = 1000   
STRIKE_RANGE_PCT = 0.15 
TICKERS = ["SPY", "QQQ", "IWM"] 

print(f"🚀 Starting GEX Backfill for last {DAYS_TO_BACKFILL} days...")
print(f"Tickers: {', '.join(TICKERS)}\n")

# ===============================================
# Helper Functions
# ===============================================
def get_historical_price(symbol, date_str):
    """
    Fetches the closing price of the underlying for a specific past date.
    date_str format: YYYY-MM-DD
    """
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
    """
    Fetches the option chain that was active on a specific past date.
    """
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
    """
    Fetches the End-of-Day quote for an option on a specific past date.
    """
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
            return val[0] if isinstance(val, list) and len(val) > 0 else val
    return None

# ===============================================
# Core Builder
# ===============================================
def build_day(symbol, target_date):
    date_str = target_date.strftime("%Y-%m-%d")
    file_tag = target_date.strftime("%Y%m%d")
    
    # Check if file already exists to save credits
    fname = f"{symbol}_GEX_robust_{file_tag}.csv"
    if os.path.exists(fname):
        print(f"   ⏭️  Skipping {date_str} (File exists)")
        return

    print(f"   📅 Fetching History for {date_str}...")

    # 1. Get Spot Price
    spot_price = get_historical_price(symbol, date_str)
    if not spot_price:
        print(f"   ⚠️ No price data for {date_str}. Market closed?")
        return # Skip weekends/holidays

    # 2. Get Chain
    raw_chain = get_historical_chain(symbol, date_str)
    if not raw_chain:
        print("      No chain data.")
        return

    # 3. Filter Strikes (Precision Mode)
    filtered_opts = []
    for sym in raw_chain:
        _, strike = parse_option_symbol(sym)
        low = spot_price * (1 - STRIKE_RANGE_PCT)
        high = spot_price * (1 + STRIKE_RANGE_PCT)
        if low <= strike <= high:
            filtered_opts.append(sym)
    
    # Slice to limit
    final_list = filtered_opts[:MAX_OPTIONS]
    print(f"      Processing {len(final_list)} options...")

    rows = []
    for i, opt in enumerate(final_list):
        q = get_historical_quote(opt, date_str)
        if not q: continue
        
        try:
            gamma = safe_extract(q, ["gamma"])
            oi = safe_extract(q, ["openInterest", "open_interest", "oi"])
            underlying = safe_extract(q, ["underlyingPrice", "underlying"]) or spot_price

            if gamma is None or oi is None: continue

            gex = float(gamma) * float(oi) * 100 * float(underlying)
            _, strike = parse_option_symbol(opt)
            
            rows.append({
                "strike": float(strike),
                "GEX": gex,
                "type": infer_option_type(opt)
            })
        except: continue
        
        # Rate limit protection
        if i % 50 == 0: time.sleep(0.05)

    if not rows:
        return

    # 4. Save CSV
    df = pd.DataFrame(rows)
    grouped = df.groupby(["strike", "type"])["GEX"].sum().unstack(fill_value=0)
    grouped.rename(columns={"C": "call_gex", "P": "put_gex"}, inplace=True)
    
    if "call_gex" not in grouped.columns: grouped["call_gex"] = 0.0
    if "put_gex" not in grouped.columns: grouped["put_gex"] = 0.0

    grouped["net_gex"] = grouped["call_gex"] - grouped["put_gex"]
    
    grouped.reset_index().to_csv(fname, index=False)
    print(f"      ✅ Saved {fname}")

# ===============================================
# Loop Last N Days
# ===============================================
today = datetime.now()

# Loop starts from 1 (Yesterday) down to DAYS_TO_BACKFILL
# This prevents it from overwriting "Today" which is handled by gex_builder.py
for i in range(1, DAYS_TO_BACKFILL + 1):
    past_date = today - timedelta(days=i)
    # Simple check to skip weekends (0=Mon, 6=Sun)
    if past_date.weekday() >= 5: 
        print(f"Skipping Weekend: {past_date.strftime('%Y-%m-%d')}")
        continue
        
    print(f"\nProcessing Backfill Day {i}/{DAYS_TO_BACKFILL} ({past_date.strftime('%Y-%m-%d')})")
    
    for ticker in TICKERS:
        try:
            build_day(ticker, past_date)
        except Exception as e:
            print(f"❌ Error {ticker}: {e}")

print("\n🏁 Backfill Complete. Now run 'gex_to_pinescript_converter.py'!")
