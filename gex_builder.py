# gex_builder.py
# -------------------------------------------------------
# Pulls option-chain data from MarketData.app,
# computes per-strike Gamma Exposure (GEX),
# and writes <SYMBOL>_GEX.csv for TradingView.
# -------------------------------------------------------

import os
import requests
import pandas as pd
from datetime import datetime

# ───────────────────────────────────────────────
# CONFIGURATION
# ───────────────────────────────────────────────
API_KEY = os.getenv("MARKETDATA_KEY")  # secret from GitHub Actions
SYMBOL = "AAPL"                        # change or loop through symbols
BASE_URL = "https://api.marketdata.app/v1/options/chain"

print("🚀 Starting MarketData GEX Builder")
print(f"Symbol: {SYMBOL}")
print(f"API key present: {'Yes' if API_KEY else 'No'}")

# ───────────────────────────────────────────────
# 1. Request the option chain
# ───────────────────────────────────────────────
url = f"{BASE_URL}/{SYMBOL}?token={API_KEY}"
print(f"Requesting: {url}")

try:
    r = requests.get(url, timeout=30)
    print(f"HTTP status: {r.status_code}")
    if r.status_code != 200:
        print(r.text[:500])
        raise SystemExit(1)
    data = r.json()
except Exception as e:
    print(f"❌ Request failed: {e}")
    raise SystemExit(1)

# MarketData returns dict with keys 'calls' and 'puts'
calls = data.get("calls", [])
puts = data.get("puts", [])
results = calls + puts
print(f"Returned {len(results)} total options")

# ───────────────────────────────────────────────
# 2. Compute GEX per strike
# ───────────────────────────────────────────────
rows = []
for opt in results:
    try:
        strike = opt.get("strike")
        oi = opt.get("open_interest")
        gamma = opt.get("greeks", {}).get("gamma")
        under = opt.get("underlying_price") or data.get("underlying", {}).get("price")

        if all([strike, oi, gamma, under]):
            gex = gamma * oi * 100 * under
            rows.append({"strike": strike, "GEX": gex})
    except Exception as e:
        print(f"⚠️ Error processing option: {e}")

print(f"Total valid rows built: {len(rows)}")

# ───────────────────────────────────────────────
# 3. Save CSV
# ───────────────────────────────────────────────
df = pd.DataFrame(rows)
if "strike" in df.columns and not df.empty:
    df = df.sort_values("strike")
    filename = f"{SYMBOL}_GEX.csv"
    df.to_csv(filename, index=False)
    print(f"✅ {datetime.now()}  Saved {filename}  ({len(df)} strikes)")
else:
    print("⚠️ No valid strike data found — check symbol or plan permissions.")

print("🏁 Finished run.")
