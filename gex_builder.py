# ===========================================================
# MarketData.app GEX Builder v9.1 — Greek Delta Added
# Author: PulsR | Maintained by Code GPT
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
# Configuration & API Key Check
# ===============================================
API_KEY = os.getenv("MARKETDATA_KEY") or ""

if not API_KEY:
    print("⚠️ WARNING: GitHub Secret 'MARKETDATA_KEY' was not found! Using unauthenticated/delayed data.")
else:
    print("✅ API Key successfully loaded from GitHub Secrets!")

BASE_URL = "https://api.marketdata.app/v1"
ENABLE_PLOTS = True
MAX_OPTIONS = 1000
STRIKE_RANGE_PCT = 0.15

# Load tickers
DEFAULT_TICKERS = ["SPY", "QQQ", "IWM", "NVDA", "AMD"]
if os.path.exists("tickers.txt"):
    with open("tickers.txt") as f:
        TICKERS = [t.strip().upper() for t in f if t.strip()]
else:
    TICKERS = DEFAULT_TICKERS

print("🚀 Starting MarketData GEX Builder (v9.1)")
print(f"Tickers: {', '.join(TICKERS)}")

# ===============================================
# Helper Functions
# ===============================================
def get_underlying_price(symbol):
    endpoints = [
        f"{BASE_URL}/stocks/quotes/{symbol}/?token={API_KEY}",
        f"{BASE_URL}/stocks/candles/D/{symbol}?from={datetime.now().strftime('%Y-%m-%d')}&to={datetime.now().strftime('%Y-%m-%d')}&token={API_KEY}"
    ]
    for url in endpoints:
        try:
            r = requests.get(url, timeout=5)
            # 200 = Success, 203 = Success (Delayed)
            if r.status_code in (200, 203):
                data = r.json()
                if data.get("s") == "ok":
                    if "last" in data: return float(data["last"][0])
                    if "mid" in data: return float(data["mid"][0])
                    if "c" in data: return float(data["c"][0])
            else:
                print(f"   ⚠️ Spot API Error {r.status_code}: {r.text}")
        except Exception as e: 
            print(f"   ⚠️ Spot Request Exception: {e}")
            continue
    return None

def get_chain_symbols(symbol):
    d_from = datetime.now().strftime("%Y-%m-%d")
    d_to = (datetime.now() + timedelta(days=45)).strftime("%Y-%m-%d")
    url = f"{BASE_URL}/options/chain/{symbol}?from={d_from}&to={d_to}&token={API_KEY}"
    try:
        r = requests.get(url, timeout=20)
        # 200 = Success, 203 = Success (Delayed)
        if r.status_code in (200, 203):
            data = r.json()
            if data.get("s") == "ok": return data.get("optionSymbol", [])
        else:
            print(f"   ⚠️ Chain API Error {r.status_code}: {r.text}")
    except Exception as e:
        print(f"   ⚠️ Chain Request Exception: {e}")
    return []

def get_quote(option_symbol):
    url = f"{BASE_URL}/options/quotes/{option_symbol}?token={API_KEY}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code in (200, 203): return r.json()
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
    return "P" if "P" in symbol_str or symbol_str.endswith("P") else "C"

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
    except: return None
    return None

# ===============================================
# Core Function
# ===============================================
def build_gex(symbol):
    print(f"\n📈 Processing {symbol}")
    
    # 1. Fetch Chain & Price
    raw_chain = get_chain_symbols(symbol)
    if not raw_chain:
        print("   ❌ No chain found.")
        return None, {}

    spot_price = get_underlying_price(symbol)
    if spot_price is None:
        try:
            test_sym = raw_chain[0]
            q = get_quote(test_sym)
            val = safe_extract(q, ["underlyingPrice", "underlying_price", "underlying"])
            if val: spot_price = float(val)
        except: pass
            
    if spot_price is None:
        print("   ⚠️ No spot price found. Proceeding with caution.")

    # 2. Filter & Sort
    filtered_chain_tuples = []
    for sym in raw_chain:
        expiry, strike = parse_option_symbol(sym)
        if spot_price:
            low = spot_price * (1 - STRIKE_RANGE_PCT)
            high = spot_price * (1 + STRIKE_RANGE_PCT)
            if not (low <= strike <= high): continue
        filtered_chain_tuples.append((sym, expiry))

    filtered_chain_tuples.sort(key=lambda x: x[1])
    unique_expiries = sorted(list(set(x[1] for x in filtered_chain_tuples)))
    
    final_list = []
    count = 0
    for expiry in unique_expiries:
        expiry_opts = [x[0] for x in filtered_chain_tuples if x[1] == expiry]
        if count + len(expiry_opts) > MAX_OPTIONS:
            if count == 0: final_list.extend(expiry_opts[:MAX_OPTIONS])
            break
        final_list.extend(expiry_opts)
        count += len(expiry_opts)

    print(f"   Fetching {len(final_list)} options...")

    # 3. Fetch Data (Includes Delta now)
    rows = []
    for i, opt in enumerate(final_list):
        q = get_quote(opt)
        if not q: continue
        try:
            strike = safe_extract(q, ["strike", "strikePrice"])
            gamma = safe_extract(q, ["gamma"])
            delta = safe_extract(q, ["delta"]) # Extract Delta
            oi = safe_extract(q, ["openInterest", "open_interest", "oi"])
            underlying = safe_extract(q, ["underlyingPrice", "underlying"])

            if any(v is None for v in [strike, gamma, delta, oi, underlying]): continue 

            f_oi = float(oi)
            f_delta = float(delta)

            # Calculate GEX ($ Gamma) and DEX ($ Delta)
            gex = float(gamma) * f_oi * 100 * float(underlying)
            dex = f_delta * f_oi * 100 * float(underlying)
            
            # Helper for Greek Delta Calculation
            weighted_delta = f_delta * f_oi

            otype = infer_option_type(opt)
            rows.append({
                "strike": float(strike), 
                "GEX": gex, 
                "DEX": dex,
                "weighted_delta": weighted_delta,
                "OI": f_oi,
                "type": otype
            })
        except: continue
        if i % 50 == 0 and i > 0: time.sleep(0.05)

    df = pd.DataFrame(rows)
    if df.empty: return None, {}

    # 4. Aggregation (GEX & DEX)
    grouped = df.groupby(["strike", "type"])[["GEX", "DEX"]].sum().unstack(fill_value=0)
    grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]
    
    strike_stats = df.groupby("strike")[["weighted_delta", "OI"]].sum()
    strike_stats["net_greek_delta"] = strike_stats["weighted_delta"] / strike_stats["OI"].replace(0, 1) 
    
    grouped = grouped.join(strike_stats["net_greek_delta"])

    rename_map = {
        "GEX_C": "call_gex", "GEX_P": "put_gex",
        "DEX_C": "call_dex", "DEX_P": "put_dex"
    }
    grouped.rename(columns=rename_map, inplace=True)
    
    for col in ["call_gex", "put_gex", "call_dex", "put_dex"]:
        if col not in grouped.columns: grouped[col] = 0.0

    grouped["net_gex"] = grouped["call_gex"] - grouped["put_gex"]
    
    # Calculate Stats
    call_wall = grouped["call_gex"].idxmax()
    put_wall = grouped["put_gex"].idxmax()
    flip_zone = compute_flip_zone(grouped)
    total_net_gex = grouped["net_gex"].sum()
    
    # Regime Detection
    regime = "NEUTRAL"
    if flip_zone is None:
        if total_net_gex > 0: regime = "ALL_CALLS"
        elif total_net_gex < 0: regime = "ALL_PUTS"
    else:
        regime = "NORMAL"

    stats = {
        "spot": spot_price if spot_price else 0.0,
        "flip": flip_zone, 
        "total_gex": total_net_gex,
        "call_wall": call_wall,
        "put_wall": put_wall,
        "regime": regime
    }

    # Save CSV
    date_tag = datetime.now().strftime("%Y%m%d")
    fname = f"{symbol}_GEX_robust_{date_tag}.csv"
    grouped.reset_index().to_csv(fname, index=False)
    print(f"   💾 Saved {fname}")
    
    # Save Visual Check PNG
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
            
            title_main = f"{symbol} Net GEX ({date_tag})"
            regime_text = ""
            if regime == "ALL_CALLS": regime_text = " (ALL CALLS - BULLISH)"
            elif regime == "ALL_PUTS": regime_text = " (ALL PUTS - BEARISH)"
            
            spot_str = f"Spot: ${spot_price:.2f}" if spot_price else "Spot: N/A"
            flip_str = f"Flip: ${flip_zone:.2f}" if flip_zone else "Flip: N/A"
            
            full_title = f"{title_main}{regime_text}\n{spot_str} | {flip_str}"
                
            plt.title(full_title)
            plt.tight_layout()
            plt.savefig(f"{symbol}_GEX_robust_{date_tag}.png", dpi=100)
            plt.close()
        except: pass

    return fname, stats

# ===============================================
# Main Loop (Cumulative Summary Update)
# ===============================================
summary_data = []

for ticker in TICKERS:
    try:
        result, stats = build_gex(ticker)
        if stats:
            summary_data.append({
                "Ticker": ticker,
                "Data": stats
            })
    except Exception as e:
        print(f"❌ Error {ticker}: {e}")

# Save Cumulative Summary
print("\n📝 Updating Gamma Summary (Cumulative)...")

new_rows = []
for item in summary_data:
    d = item["Data"]
    
    flip_display = "N/A"
    if d["flip"]: 
        flip_display = f"{d['flip']:.2f}"
    elif d["regime"] == "ALL_CALLS":
        flip_display = "ALL_CALLS"
    elif d["regime"] == "ALL_PUTS":
        flip_display = "ALL_PUTS"
        
    new_rows.append({
        "Ticker": item["Ticker"],
        "Spot": d["spot"],
        "Flip": flip_display,
        "Call Wall": d["call_wall"],
        "Put Wall": d["put_wall"],
        "Net GEX ($B)": round(d["total_gex"] / 1e9, 2),
        "Regime": d["regime"]
    })
    
new_df = pd.DataFrame(new_rows)
master_df = pd.DataFrame()
if os.path.exists("gamma_summary.csv"):
    try:
        master_df = pd.read_csv("gamma_summary.csv")
    except: pass

if not master_df.empty and not new_df.empty:
    master_df = master_df[~master_df['Ticker'].isin(new_df['Ticker'])]
    final_df = pd.concat([master_df, new_df], ignore_index=True)
elif not new_df.empty:
    final_df = new_df
else:
    final_df = master_df

if not final_df.empty:
    final_df = final_df.sort_values("Ticker")
    final_df.to_csv("gamma_summary.csv", index=False)
    print("📘 Saved updated gamma_summary.csv")
else:
    print("⚠️ No data to save.")

print("\n🏁 Data Build Complete. Run 'gex_to_pinescript_converter.py' next.")
