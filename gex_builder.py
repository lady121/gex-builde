# ===========================================================
# MarketData.app GEX Builder v8.3 — CSV Summary
# Author: PulsR | Maintained by Code GPT
# ===========================================================
# Fixes from v8.2:
#  ✅ Changed Gamma Summary output from .txt to .csv
#  ✅ Includes Spot, Flip, and Net GEX in the CSV.
#  ✅ Rounded values to 2 decimals for cleaner viewing.
# ===========================================================

import os
import time
import re
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# ===============================================
# Configuration
# ===============================================
API_KEY = os.getenv("MARKETDATA_KEY") or ""
BASE_URL = "https://api.marketdata.app/v1"
ENABLE_PLOTS = True
MAX_OPTIONS = 1000  # Increased to capture multi-week flow
STRIKE_RANGE_PCT = 0.15  # +/- 15% from spot price

# ===============================================
# Load tickers
# ===============================================
DEFAULT_TICKERS = ["SPY", "QQQ", "IWM", "NVDA", "AMD"]

if os.path.exists("tickers.txt"):
    with open("tickers.txt") as f:
        TICKERS = [t.strip().upper() for t in f if t.strip()]
else:
    TICKERS = DEFAULT_TICKERS

print("🚀 Starting MarketData GEX Builder (v8.3 — CSV Summary)")
print(f"Tickers: {', '.join(TICKERS)}")

# ===============================================
# Helper Functions
# ===============================================
def get_underlying_price(symbol):
    """Fetches the real-time price of the underlying stock."""
    # Try different endpoints in case one is restricted
    endpoints = [
        f"{BASE_URL}/stocks/quotes/{symbol}/?token={API_KEY}",
        f"{BASE_URL}/stocks/candles/D/{symbol}?from={datetime.now().strftime('%Y-%m-%d')}&to={datetime.now().strftime('%Y-%m-%d')}&token={API_KEY}"
    ]
    
    for url in endpoints:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                data = r.json()
                if data.get("s") == "ok":
                    if "last" in data: return float(data["last"][0])
                    if "mid" in data: return float(data["mid"][0])
                    if "c" in data: return float(data["c"][0]) # Close from candle
        except:
            continue
    return None

def get_chain_symbols(symbol):
    """
    Fetch raw list of option symbols. 
    Forces a date range to ensure we don't just get 0DTE.
    """
    d_from = datetime.now().strftime("%Y-%m-%d")
    d_to = (datetime.now() + timedelta(days=45)).strftime("%Y-%m-%d")
    
    url = f"{BASE_URL}/options/chain/{symbol}?from={d_from}&to={d_to}&token={API_KEY}"
    
    try:
        r = requests.get(url, timeout=20)
        if r.status_code in (200, 203):
            data = r.json()
            if data.get("s") == "ok":
                return data.get("optionSymbol", [])
    except Exception as e:
        print(f"❌ Error fetching chain for {symbol}: {e}")
    return []

def get_quote(option_symbol):
    url = f"{BASE_URL}/options/quotes/{option_symbol}?token={API_KEY}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code in (200, 203):
            return r.json()
    except:
        pass
    return None

def parse_option_symbol(symbol):
    # Extracts Date and Strike from OCC symbol
    # Example: SPY231223C00450000 -> Date: 231223, Strike: 450.0
    match = re.search(r'([A-Z]+)(\d{6})([CP])(\d+)', symbol)
    if match:
        expiry = match.group(2)
        strike_raw = match.group(4)
        strike = int(strike_raw) / 1000.0
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

def compute_flip_zone(df):
    if df.empty: return None
    try:
        df_sorted = df.sort_index().reset_index()
        df_sorted["strike"] = pd.to_numeric(df_sorted["strike"], errors='coerce')
        df_sorted = df_sorted.dropna(subset=["strike"])
        
        df_sorted["cum_gex"] = df_sorted["net_gex"].cumsum()
        signs = np.sign(df_sorted["cum_gex"])
        flips = np.where(np.diff(signs))[0]
        
        if len(flips) > 0:
            idx = flips[0]
            low = df_sorted.loc[idx, "strike"]
            high = df_sorted.loc[idx + 1, "strike"]
            return (low + high) / 2
    except:
        return None
    return None

# ===============================================
# Core Function
# ===============================================
def build_gex(symbol):
    print(f"\n📈 Processing {symbol}")
    
    # 1. Fetch Full Chain (Raw)
    raw_chain = get_chain_symbols(symbol)
    if not raw_chain:
        print("   ❌ No chain found.")
        return None, {}

    # 2. Get Underlying Price (with Fallback)
    spot_price = get_underlying_price(symbol)
    
    if spot_price is None:
        print("   ⚠️ Stock API failed. Deriving price from option chain...")
        # Fallback: Get quote for the first option to find underlying price
        try:
            test_sym = raw_chain[0]
            q = get_quote(test_sym)
            val = safe_extract(q, ["underlyingPrice", "underlying_price", "underlying"])
            if val:
                spot_price = float(val)
                print(f"   ✅ Derived Spot Price: ${spot_price}")
        except:
            pass
            
    if spot_price is None:
        print("   ❌ Could not determine spot price. Skipping precision filter.")
        # Proceed with raw chain, but risk hitting limits
        
    # 3. Local Filtering (Python-side)
    filtered_chain_tuples = []
    
    for sym in raw_chain:
        expiry, strike = parse_option_symbol(sym)
        
        # Strike Filter
        if spot_price:
            low = spot_price * (1 - STRIKE_RANGE_PCT)
            high = spot_price * (1 + STRIKE_RANGE_PCT)
            if not (low <= strike <= high):
                continue # Skip strikes outside range
        
        filtered_chain_tuples.append((sym, expiry))

    # 4. Sort by Expiration
    filtered_chain_tuples.sort(key=lambda x: x[1])
    
    unique_expiries = sorted(list(set(x[1] for x in filtered_chain_tuples)))
    print(f"   Found {len(unique_expiries)} expirations. Processing nearest...")

    # 5. Select Final List (respecting MAX_OPTIONS)
    final_list = []
    count = 0
    
    for expiry in unique_expiries:
        expiry_opts = [x[0] for x in filtered_chain_tuples if x[1] == expiry]
        
        if count + len(expiry_opts) > MAX_OPTIONS:
            if count == 0:
                final_list.extend(expiry_opts[:MAX_OPTIONS])
            else:
                print(f"   ⚠️ Limit ({MAX_OPTIONS}) reached at expiry {expiry}. Dropping later dates.")
            break
        
        final_list.extend(expiry_opts)
        count += len(expiry_opts)

    print(f"   Processing {len(final_list)} options...")

    # 6. Fetch Data
    rows = []
    for i, opt in enumerate(final_list):
        q = get_quote(opt)
        if not q: continue
            
        try:
            strike = safe_extract(q, ["strike", "strikePrice"])
            gamma = safe_extract(q, ["gamma"])
            oi = safe_extract(q, ["openInterest", "open_interest", "oi"])
            underlying = safe_extract(q, ["underlyingPrice", "underlying"])

            if any(v is None for v in [strike, gamma, oi, underlying]): continue 

            gex = float(gamma) * float(oi) * 100 * float(underlying)
            otype = infer_option_type(opt)
            
            rows.append({
                "strike": float(strike),
                "GEX": gex,
                "type": otype
            })
        except:
            continue
        
        if i % 50 == 0 and i > 0: time.sleep(0.05)

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"⚠️ No valid GEX data found for {symbol}")
        return None, {}

    # 7. Aggregation
    grouped = df.groupby(["strike", "type"])["GEX"].sum().unstack(fill_value=0)
    grouped.rename(columns={"C": "call_gex", "P": "put_gex"}, inplace=True)

    if "call_gex" not in grouped.columns: grouped["call_gex"] = 0.0
    if "put_gex" not in grouped.columns: grouped["put_gex"] = 0.0

    grouped["net_gex"] = grouped["call_gex"] - grouped["put_gex"]
    
    # Statistics
    flip_zone = compute_flip_zone(grouped)
    total_net_gex = grouped["net_gex"].sum()
    
    stats = {
        "spot": spot_price if spot_price else 0.0,
        "flip": flip_zone if flip_zone else 0.0,
        "total_gex": total_net_gex
    }

    # Save
    date_tag = datetime.now().strftime("%Y%m%d")
    fname = f"{symbol}_GEX_robust_{date_tag}.csv"
    grouped.reset_index().to_csv(fname, index=False)
    print(f"   💾 Saved {fname}")

    # Plot
    if ENABLE_PLOTS:
        try:
            plt.figure(figsize=(10, 6))
            colors = np.where(grouped["net_gex"] >= 0, '#2ecc71', '#e74c3c')
            plt.bar(grouped.index, grouped["net_gex"], color=colors, alpha=0.7)
            plt.axhline(0, color="black", lw=1)
            
            if spot_price:
                plt.axvline(spot_price, color="orange", ls="-", lw=1.5, label=f"Spot: {spot_price}")
            if flip_zone:
                plt.axvline(flip_zone, color="blue", ls="--", lw=2, label=f"Flip: {flip_zone:.2f}")
                
            plt.title(f"{symbol} Net GEX (Robust)")
            plt.xlabel("Strike")
            plt.ylabel("Net GEX ($)")
            plt.legend()
            plt.tight_layout()
            plt.savefig(f"{symbol}_GEX_robust_{date_tag}.png", dpi=100)
            plt.close()
        except: pass

    return fname, stats

# ===============================================
# Main Loop
# ===============================================
generated_files = []
summary_data = []

for ticker in TICKERS:
    try:
        result, stats = build_gex(ticker)
        if result: 
            generated_files.append(result)
        
        # Always append to summary if we attempted processing, even if partial
        if stats:
            summary_data.append({
                "Ticker": ticker,
                "Spot Price": stats.get('spot', 0.0),
                "Flip Zone": stats.get('flip', 0.0),
                "Total Net GEX ($B)": stats.get('total_gex', 0.0) / 1_000_000_000
            })
            
    except Exception as e:
        print(f"❌ Error {ticker}: {e}")

# Save Gamma Summary (CSV)
print("\n📝 Generating Gamma Summary (CSV)...")
try:
    if summary_data:
        summary_df = pd.DataFrame(summary_data)
        summary_df = summary_df.round(2) # Clean up floats
        summary_df.to_csv("gamma_summary.csv", index=False)
        print("📘 Saved gamma_summary.csv")
    else:
        print("⚠️ No data available for summary.")
except Exception as e:
    print(f"❌ Failed to save summary: {e}")

print("\n🏁 Robust Build Complete.")
