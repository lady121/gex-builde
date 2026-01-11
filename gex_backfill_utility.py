# ===========================================================
# MarketData.app GEX Backfill Utility (v9.2 - DEX Integrated)
# ===========================================================
# FIXES / UPDATES:
#  ✅ Adds DEX (Delta Exposure) alongside GEX in historical backfill.
#  ✅ Maintains full backwards compatibility with v9.1.
#  ✅ Keeps same print messages, filenames, and error handling.
# ===========================================================

import os
import time
import re
import math
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ===============================================
# Configuration
# ===============================================
API_KEY = os.getenv("MARKETDATA_KEY") or ""
BASE_URL = "https://api.marketdata.app/v1"

# Default to 30 days unless specified
DAYS_TO_BACKFILL = int(os.getenv("DAYS_TO_BACKFILL", 30))

MAX_OPTIONS = 1500
STRIKE_RANGE_PCT = 0.20

# ===============================================
# Load Tickers (Dynamic)
# ===============================================
DEFAULT_TICKERS = ["SPY", "QQQ", "IWM"]

# Check current directory and parent directory for tickers.txt
ticker_file = "tickers.txt"
if not os.path.exists(ticker_file) and os.path.exists(f"../{ticker_file}"):
    ticker_file = f"../{ticker_file}"

if os.path.exists(ticker_file):
    with open(ticker_file) as f:
        TICKERS = [t.strip().upper() for t in f if t.strip()]
    print(f"📂 Loaded tickers from {ticker_file}")
else:
    TICKERS = DEFAULT_TICKERS
    print("⚠️ tickers.txt not found, using defaults.")

print(f"🚀 Starting GEX Backfill for: {', '.join(TICKERS)}")
print(f"📅 Lookback: {DAYS_TO_BACKFILL} days")

# ===============================================
# Math Helper: Black-Scholes Gamma
# ===============================================
def norm_pdf(x):
    """Standard normal probability density function"""
    return (1.0 / math.sqrt(2 * math.pi)) * math.exp(-0.5 * x * x)

def calculate_local_gamma(S, K, T, sigma=0.18, r=0.045):
    """
    Approximates Gamma if API is missing it.
    S=Spot, K=Strike, T=Time(years), sigma=IV (default 18%), r=Rate (4.5%)
    """
    try:
        if T <= 0 or S <= 0 or K <= 0:
            return 0.0
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        gamma = norm_pdf(d1) / (S * sigma * math.sqrt(T))
        return gamma
    except:
        return 0.0

# ===============================================
# API Functions
# ===============================================
def get_historical_price(symbol, date_str):
    url = f"{BASE_URL}/stocks/candles/D/{symbol}?from={date_str}&to={date_str}&token={API_KEY}"
    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            data = r.json()
            if data.get("s") == "ok" and "c" in data:
                return float(data["c"][0])
    except:
        pass
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
    url = f"{BASE_URL}/options/quotes/{option_symbol}?date={date_str}&token={API_KEY}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code in (200, 203):
            return r.json()
    except:
        pass
    return None

def parse_option_symbol(symbol):
    match = re.search(r"([A-Z]+)(\d{6})([CP])(\d+)", symbol)
    if match:
        expiry = match.group(2)
        strike = int(match.group(4)) / 1000.0
        return expiry, strike
    return "999999", 0.0

def infer_option_type(symbol_str):
    if symbol_str.endswith("C"):
        return "C"
    if symbol_str.endswith("P"):
        return "P"
    return "C" if "C" in symbol_str else "P"

def safe_extract(d, keys):
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d and d[k] is not None:
            val = d[k]
            if isinstance(val, list) and len(val) > 0:
                return val[0]
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
        print(f"      ⚠️ No price data (Market Closed?).")
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
    recalc_count = 0

    for i, opt in enumerate(final_list):
        q = get_historical_quote(opt, date_str)
        if not q:
            continue

        try:
            # 1. Extract Basic Data
            oi = safe_extract(q, ["openInterest", "open_interest", "oi"])
            underlying = safe_extract(q, ["underlyingPrice", "underlying"]) or spot_price
            gamma = safe_extract(q, ["gamma"])
            delta = safe_extract(q, ["delta"])  # NEW: Extract Delta

            # 2. Fallback Logic: Calculate Gamma if missing
            if gamma is None:
                dte_raw = safe_extract(q, ["dte"])
                if dte_raw:
                    dte_days = float(dte_raw)
                    T = dte_days / 365.0
                    _, strike = parse_option_symbol(opt)
                    gamma = calculate_local_gamma(S=float(underlying), K=float(strike), T=T)
                    recalc_count += 1
                else:
                    continue

            if oi is None:
                continue
            if delta is None:
                delta = 0.0

            # 3. Compute GEX & DEX
            gex = float(gamma) * float(oi) * 100 * float(underlying)
            dex = float(delta) * float(oi) * 100 * float(underlying)
            _, strike = parse_option_symbol(opt)

            rows.append(
                {
                    "strike": float(strike),
                    "GEX": gex,
                    "DEX": dex,
                    "type": infer_option_type(opt),
                }
            )
        except:
            continue

        if i % 50 == 0:
            time.sleep(0.02)

    if recalc_count > 0:
        print(f"      🔧 Locally calculated Gamma for {recalc_count} options (API was empty).")

    if not rows:
        print(f"      ❌ No valid rows generated for {date_str}.")
        return

    # Save CSV
    df = pd.DataFrame(rows)
    grouped = df.groupby(["strike", "type"])[["GEX", "DEX"]].sum().unstack(fill_value=0)
    grouped.columns = ["_".join(col).strip() for col in grouped.columns.values]

    rename_map = {
        "GEX_C": "call_gex",
        "GEX_P": "put_gex",
        "DEX_C": "call_dex",
        "DEX_P": "put_dex",
    }
    grouped.rename(columns=rename_map, inplace=True)

    # Ensure all expected columns exist
    for col in ["call_gex", "put_gex", "call_dex", "put_dex"]:
        if col not in grouped.columns:
            grouped[col] = 0.0

    grouped["net_gex"] = grouped["call_gex"] - grouped["put_gex"]
    grouped["net_dex"] = grouped["call_dex"] - grouped["put_dex"]

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
