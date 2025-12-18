# gex_builder.py
# -------------------------------------------------------
# Pulls option-chain data from Massive (Polygon.io rebrand),
# computes per-strike Gamma Exposure (GEX),
# and writes <SYMBOL>_GEX.csv for TradingView.
# Includes full debugging and fail-safes.
# -------------------------------------------------------

import os
import requests
import pandas as pd
from datetime import datetime

# ───────────────────────────────────────────────
# 1. CONFIGURATION
# ───────────────────────────────────────────────
API_KEY = os.getenv("API_KEY")  # GitHub secret (must match workflow env)
SYMBOL = "AAPL"                 # you can change this or loop through tickers
BASE_URL = "https://api.massive.com"

print("🚀 Starting GEX builder")
print(f"Using symbol: {SYMBOL}")
print(f"API key present: {'Yes' if API_KEY else 'No'}")

# ───────────────────────────────────────────────
# 2. TRY SNAPSHOT ENDPOINT FIRST
# ───────────────────────────────────────────────
url = f"{BASE_URL}/v3/snapshot/options/{SYMBOL}?apiKey={API_KEY}"
print(f"Requesting snapshot endpoint: {url}")

try:
    r = requests.get(url, timeout=30)
    print(f"HTTP status: {r.status_code}")
    print("Raw response (first 800 chars):")
    print(r.text[:800])
    r.raise_for_status()
    data = r.json()
except Exception as e:
    print(f"❌ Snapshot request failed: {e}")
    data = {}

results = data.get("results", [])
print(f"Snapshot results count: {len(results)}")

# ───────────────────────────────────────────────
# 3. IF EMPTY, TRY CONTRACTS ENDPOINT
# ───────────────────────────────────────────────
if not results:
    alt_url = f"{BASE_URL}/v3/reference/options/contracts?ticker={SYMBOL}&apiKey={API_KEY}"
    print(f"⚠️ No snapshot results; trying contracts endpoint:\n{alt_url}")
    try:
        r2 = requests.get(alt_url, timeout=30)
        print(f"Contracts HTTP status: {r2.status_code}")
        print("Contracts raw response (first 800 chars):")
        print(r2.text[:800])
        r2.raise_for_status()
        data = r2.json()
        results = data.get("results", [])
        print(f"Contracts endpoint returned {len(results)} records.")
    except Exception as e:
        print(f"❌ Contracts request failed: {e}")
        results = []

# ───────────────────────────────────────────────
# 4. COMPUTE GEX
# ───────────────────────────────────────────────
rows = []
for opt in results:
    # Try multiple possible field names (Massive changed JSON keys several times)
    details = opt.get("details", {})
    greeks = opt.get("greeks", {})
    strike = (
        details.get("strike_price")
        or opt.get("strike_price")
        or opt.get("strike")
        or opt.get("strikePrice")
    )
    oi = opt.get("open_interest") or opt.get("openInterest")
    gamma = greeks.get("gamma") or opt.get("gamma")
    under = (
        opt.get("underlying_asset", {}).get("price")
        or opt.get("underlying_price")
        or opt.get("underlyingPrice")
    )

    if all([strike, oi, gamma, under]):
        try:
            gex = gamma * oi * 100 * under
            rows.append({"strike": strike, "GEX": gex})
        except Exception as calc_error:
            print(f"⚠️ Calc error for strike {strike}: {calc_error}")

print(f"Total valid rows built: {len(rows)}")

# ───────────────────────────────────────────────
# 5. SAVE CSV OR WARN
# ───────────────────────────────────────────────
df = pd.DataFrame(rows)

if "strike" in df.columns and not df.empty:
    df = df.sort_values("strike")
    filename = f"{SYMBOL}_GEX.csv"
    df.to_csv(filename, index=False)
    print(f"✅ {datetime.now()}  Saved {filename}  ({len(df)} strikes)")
else:
    print("⚠️ No valid 'strike' data found in response.")
    print(f"Received {len(results)} option entries in total.")
    print("Double-check API key, endpoint permissions, or ticker symbol.")

print("🏁 Finished run.")
