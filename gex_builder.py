# ===========================================================
# MarketData.app GEX Builder v7.5 — Full Gamma Suite (Fixed)
# Author: PulsR | Maintained by Code GPT
# ===========================================================
# Features:
#  ✅ Fixed 'KeyError: strike' crash (handled index correctly)
#  ✅ Flip Zone Detection (where net cumulative GEX crosses zero)
#  ✅ Gamma Squeeze Index (GSI)
#  ✅ Call/Put/Net GEX analytics
#  ✅ Safe recursive key extraction
#  ✅ Auto-detects and converts strikes to float
# ===========================================================

import os
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt

# ===============================================
# Configuration
# ===============================================
API_KEY = os.getenv("MARKETDATA_KEY") or ""  # Leave empty if relying on backup or manual input
BASE_URL = "https://api.marketdata.app/v1/options"
ENABLE_PLOTS = True
MAX_OPTIONS = 400  # Limits API calls per ticker to avoid rate limits

# ===============================================
# Load tickers
# ===============================================
# Defaults if file missing
DEFAULT_TICKERS = ["SPY", "QQQ", "NVDA", "IWM", "AMD"]

if os.path.exists("tickers.txt"):
    with open("tickers.txt") as f:
        TICKERS = [t.strip().upper() for t in f if t.strip()]
else:
    TICKERS = DEFAULT_TICKERS

print("🚀 Starting MarketData GEX Builder (v7.5 — Fixed)")
print(f"Tickers: {', '.join(TICKERS)}")
print(f"API key present: {'Yes' if API_KEY else 'No'}\n")

# ===============================================
# Helper Functions
# ===============================================
def get_chain(symbol):
    """Fetch list of option symbols for a given ticker."""
    url = f"{BASE_URL}/chain/{symbol}?token={API_KEY}"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code not in (200, 203):
            print(f"❌ Chain fetch failed for {symbol}: {r.status_code}")
            return []
        data = r.json()
        if data.get("s") != "ok":
            print(f"⚠️ No valid chain data for {symbol}")
            return []
        return data.get("optionSymbol", [])
    except Exception as e:
        print(f"❌ Error fetching chain for {symbol}: {e}")
        return []

def get_quote(option_symbol):
    """Fetch individual option quote data."""
    url = f"{BASE_URL}/quotes/{option_symbol}?token={API_KEY}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code not in (200, 203):
            return None
        data = r.json()
        if data.get("s") != "ok":
            return None
        return data
    except Exception:
        return None

def infer_option_type(symbol_str):
    """Guess if an option is a Call or Put from its symbol."""
    # Strict suffix check first (safest for tickers containing C or P like OPEN/PAC)
    if symbol_str.endswith("C"):
        return "C"
    if symbol_str.endswith("P"):
        return "P"
    
    # Fallback checks
    if "C" in symbol_str and "P" not in symbol_str:
        return "C"
    if "P" in symbol_str and "C" not in symbol_str:
        return "P"
    
    return None

def safe_extract(d, keys, default=None):
    """Recursively search for a key inside nested dictionaries or lists."""
    if not isinstance(d, dict):
        return default

    # Direct lookup
    for k in keys:
        if k in d and d[k] is not None:
            val = d[k]
            if isinstance(val, list) and len(val) > 0:
                return val[0]
            return val

    # Nested search
    for v in d.values():
        if isinstance(v, dict):
            result = safe_extract(v, keys, default)
            if result is not None:
                return result
        elif isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    result = safe_extract(item, keys, default)
                    if result is not None:
                        return result
    return default

def compute_flip_zone(df):
    """Find flip zone where cumulative GEX crosses zero."""
    # CRITICAL FIX: The input 'df' comes from .unstack(), so 'strike' is the INDEX.
    # We must sort by index and reset index to make 'strike' accessible as a column.
    
    if df.empty:
        return None

    try:
        df_sorted = df.sort_index().reset_index()
        
        # Ensure strike is numeric for calculation
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
    except Exception as e:
        print(f"⚠️ Calculation error in flip zone: {e}")
        return None
        
    return None

# ===============================================
# Core Function
# ===============================================
def build_gex(symbol):
    print(f"\n📈 Processing {symbol}")
    chain = get_chain(symbol)
    if not chain:
        return None, None

    rows = []
    # Limit processing to MAX_OPTIONS to prevent long runtimes/timeouts
    for i, opt in enumerate(chain[:MAX_OPTIONS]):
        q = get_quote(opt)
        if not q:
            continue
            
        try:
            strike = safe_extract(q, ["strike", "strikePrice", "strike_price"])
            gamma = safe_extract(q, ["gamma"])
            oi = safe_extract(q, ["openInterest", "open_interest", "oi"])
            underlying = safe_extract(q, ["underlyingPrice", "underlying_price", "underlying"])

            # Basic validation
            if any(v is None for v in [strike, gamma, oi, underlying]):
                continue 

            gex = float(gamma) * float(oi) * 100 * float(underlying)
            otype = infer_option_type(opt)
            
            if otype:
                rows.append({
                    "strike": float(strike),
                    "gamma": float(gamma),
                    "oi": float(oi),
                    "underlying": float(underlying),
                    "GEX": gex,
                    "type": otype
                })
        except Exception as e:
            # Silent skip for individual bad rows
            continue

        # Small delay to be nice to the API
        if i % 50 == 0 and i > 0:
            time.sleep(0.1)

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"⚠️ No valid GEX data found for {symbol}")
        return None, None

    # ===============================================
    # Aggregation & Metrics
    # ===============================================
    # This creates a DF with 'strike' as the INDEX
    grouped = df.groupby(["strike", "type"])["GEX"].sum().unstack(fill_value=0)
    
    # Rename columns explicitly
    grouped.rename(columns={"C": "call_gex", "P": "put_gex"}, inplace=True)

    # Ensure necessary columns exist
    if "call_gex" not in grouped.columns:
        grouped["call_gex"] = 0.0
    if "put_gex" not in grouped.columns:
        grouped["put_gex"] = 0.0

    if df["type"].nunique() < 2:
        print(f"ℹ️  Note: Only one option type (Calls or Puts) found for {symbol}.")

    grouped["net_gex"] = grouped["call_gex"] - grouped["put_gex"]
    grouped["abs_net_gex"] = grouped["net_gex"].abs()

    # Flip Zone Calculation
    flip_zone = compute_flip_zone(grouped)

    # Gamma Squeeze Index (GSI)
    total_gex = grouped["call_gex"].abs() + grouped["put_gex"].abs()
    grouped["GSI"] = np.where(total_gex > 0, grouped["call_gex"].abs() / total_gex, 0)

    # Pressure Zones (top 10% of strikes by activity)
    cutoff = grouped["abs_net_gex"].quantile(0.9)
    grouped["pressure_zone"] = grouped["abs_net_gex"] >= cutoff

    # ===============================================
    # Save Outputs
    # ===============================================
    date_tag = datetime.now().strftime("%Y%m%d")
    fname = f"{symbol}_GEX_full_{date_tag}.csv"
    
    # Reset index here so 'strike' is saved as a column in CSV
    grouped.reset_index().to_csv(fname, index=False)
    print(f"✅ Saved {fname} ({len(grouped)} strikes)")

    # ===============================================
    # Visualization
    # ===============================================
    if ENABLE_PLOTS:
        try:
            plt.figure(figsize=(10, 6))
            # Plot Net GEX bars
            plt.bar(grouped.index, grouped["net_gex"], color=np.where(grouped["net_gex"] >= 0, 'green', 'red'), alpha=0.6, label="Net GEX")
            
            plt.axhline(0, color="black", linestyle="-", linewidth=1)
            
            if flip_zone:
                plt.axvline(flip_zone, color="blue", linestyle="--", linewidth=2, label=f"Flip: {flip_zone:.2f}")
                
            plt.title(f"{symbol} Net Gamma Exposure Profile")
            plt.xlabel("Strike Price")
            plt.ylabel("Net Gamma Exposure ($)")
            plt.legend()
            plt.grid(alpha=0.3)
            
            plot_name = f"{symbol}_GEX_plot_{date_tag}.png"
            plt.savefig(plot_name, dpi=100)
            plt.close()
        except Exception as plot_e:
            print(f"⚠️ Could not generate plot: {plot_e}")

    return fname, flip_zone

# ===============================================
# Main Execution Loop
# ===============================================
generated_files = []
flip_summary = {}

for ticker in TICKERS:
    try:
        result, flip = build_gex(ticker)
        if result:
            generated_files.append(result)
        if flip:
            flip_summary[ticker] = flip
    except Exception as e:
        print(f"❌ Critical error processing {ticker}: {e}")

# ===============================================
# Summary Files
# ===============================================
if generated_files:
    latest_date = datetime.now().strftime("%Y%m%d")
    with open("latest.txt", "w") as f:
        f.write(latest_date)
    print(f"\n🕒 Updated latest.txt")

if flip_summary:
    with open("flip_zones.txt", "w") as f:
        f.write(f"Flip Zones for {datetime.now().strftime('%Y-%m-%d')}\n")
        f.write("=========================================\n")
        for k, v in flip_summary.items():
            f.write(f"{k}: {v:.2f}\n")
    print("📘 Saved flip_zones.txt")

print("\n🏁 Process Complete.")
