# ===========================================================
# MarketData.app GEX Builder v8.6 — Data Producer
# Author: PulsR | Maintained by Code GPT
# ===========================================================
# - Generates robust CSV files for the Pine Script Converter.
# - Includes visual PNG checks.
# - Saves 'gamma_summary.csv' for spreadsheet use.
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
MAX_OPTIONS = 1000
STRIKE_RANGE_PCT = 0.15

DEFAULT_TICKERS = ["SPY", "QQQ", "IWM", "NVDA", "AMD"]

if os.path.exists("tickers.txt"):
    with open("tickers.txt") as f:
        TICKERS = [t.strip().upper() for t in f if t.strip()]
else:
    TICKERS = DEFAULT_TICKERS

print("🚀 Starting MarketData GEX Builder (v8.6)")
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
            if r.status_code == 200:
                data = r.json()
                if data.get("s") == "ok":
                    if "last" in data: return float(data["last"][0])
                    if "mid" in data: return float(data["mid"][0])
                    if "c" in data: return float(data["c"][0])
        except: continue
    return None

def get_chain_symbols(symbol):
    d_from = datetime.now().strftime("%Y-%m-%d")
    d_to = (datetime.now() + timedelta(days=45)).strftime("%Y-%m-%d")
    url = f"{BASE_URL}/options/chain/{symbol}?from={d_from}&to={d_to}&token={API_KEY}"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code in (200, 203):
            data = r.json()
            if data.get("s") == "ok": return data.get("optionSymbol", [])
    except: pass
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

    # 3. Fetch Data
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
            rows.append({"strike": float(strike), "GEX": gex, "type": otype})
        except: continue
        if i % 50 == 0 and i > 0: time.sleep(0.05)

    df = pd.DataFrame(rows)
    if df.empty: return None, {}

    # 4. Aggregation & Walls
    grouped = df.groupby(["strike", "type"])["GEX"].sum().unstack(fill_value=0)
    grouped.rename(columns={"C": "call_gex", "P": "put_gex"}, inplace=True)
    if "call_gex" not in grouped.columns: grouped["call_gex"] = 0.0
    if "put_gex" not in grouped.columns: grouped["put_gex"] = 0.0
    grouped["net_gex"] = grouped["call_gex"] - grouped["put_gex"]
    
    # Calculate Stats
    call_wall = grouped["call_gex"].idxmax()
    put_wall = grouped["put_gex"].idxmax()
    flip_zone = compute_flip_zone(grouped)
    total_net_gex = grouped["net_gex"].sum()

    stats = {
        "spot": spot_price if spot_price else 0.0,
        "flip": flip_zone, 
        "total_gex": total_net_gex,
        "call_wall": call_wall,
        "put_wall": put_wall
    }

    # Save CSV (Required by Converter)
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
                
            plt.title(f"{symbol} Net GEX ({date_tag})")
            plt.tight_layout()
            plt.savefig(f"{symbol}_GEX_robust_{date_tag}.png", dpi=100)
            plt.close()
        except: pass

    return fname, stats

# ===============================================
# Main Loop
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

# Save CSV Summary
if summary_data:
    csv_rows = []
    for item in summary_data:
        d = item["Data"]
        csv_rows.append({
            "Ticker": item["Ticker"],
            "Spot": d["spot"],
            "Flip": d["flip"] if d["flip"] else "N/A",
            "Call Wall": d["call_wall"],
            "Put Wall": d["put_wall"],
            "Net GEX ($B)": round(d["total_gex"] / 1e9, 2)
        })
    pd.DataFrame(csv_rows).to_csv("gamma_summary.csv", index=False)
    print("\n📘 Saved gamma_summary.csv")

print("\n🏁 Data Build Complete. Run 'gex_to_pinescript_converter.py' next.")
