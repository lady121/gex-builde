# gex_builder.py
# -------------------------------------------------------
# Updated MarketData.app version
# Step 1: Get all option symbols for the ticker
# Step 2: Fetch snapshots for each contract (OI, gamma, etc.)
# Step 3: Compute GEX and save CSV
# -------------------------------------------------------

import os
import requests
import pandas as pd
from datetime import datetime
import time

API_KEY = os.getenv("MARKETDATA_KEY")
SYMBOL = "AAPL"
BASE_URL = "https://api.marketdata.app/v1/options"

print("🚀 Starting MarketData GEX Builder (v2)")
print(f"Symbol: {SYMBOL}")
print(f"API key present: {'Yes' if API_KEY else 'No'}")

# ───────────────────────────────────────────────
# 1. Get list of option symbols
# ───────────────────────────────────────────────
chain_url = f"{BASE_URL}/chain/{SYMBOL}?token={API_KEY}"
print(f"Requesting chain: {chain_url}")
r = requests.get(chain_url, timeout=30)

if r.status_code != 200:
    print(f"❌ Chain request failed ({r.status_code}): {r.text[:500]}")
    raise SystemExit(1)

chain_data = r.json()
option_symbols = chain_data.get("optionSymbol", [])
print(f"Returned {len(option_symbols)} option symbols")

if not option_symbols:
    print("⚠️ No option symbols found — check API plan or symbol")
    raise SystemExit(0)

# Limit to first 500 to stay within rate limits
option_symbols = option_symbols[:500]

# ───────────────────────────────────────────────
# 2. Fetch snapshots for each option
# ───────────────────────────────────────────────
rows = []
for i, opt_sym in enumerate(option_symbols, start=1):
    snap_url = f"{BASE_URL}/snapshot/{opt_sym}?token={API_KEY}"
    try:
        snap_r = requests.get(snap_url, timeout=15)
        if snap_r.status_code != 200:
            continue
        d = snap_r.json()
        oi = d.get("openInterest")
        gamma = d.get("gamma")
        strike = d.get("strike")
        underlying = d.get("underlyingPrice")

        if all([oi, gamma, strike, underlying]):
            gex = gamma * oi * 100 * underlying
            rows.append({"strike": strike, "GEX": gex})
    except Exception as e:
        print(f"⚠️ Error on {opt_sym}: {e}")
        continue

    # Pause briefly to avoid rate limits
    time.sleep(0.1)

print(f"✅ Processed {len(rows)} valid contracts")

# ───────────────────────────────────────────────
# 3. Save results
# ───────────────────────────────────────────────
df = pd.DataFrame(rows)
if "strike" in df.columns and not df.empty:
    df = df.sort_values("strike")
    filename = f"{SYMBOL}_GEX.csv"
    df.to_csv(filename, index=False)
    print(f"✅ {datetime.now()} Saved {filename} ({len(df)} strikes)")
else:
    print("⚠️ No valid GEX rows — check API response or limits.")

print("🏁 Finished run.")
