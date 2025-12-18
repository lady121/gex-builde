# gex_builder.py
# -------------------------------------------------------
# MarketData.app GEX Builder (v3)
# Handles 200 and 203 status codes
# -------------------------------------------------------

import os, time, requests, pandas as pd
from datetime import datetime

API_KEY = os.getenv("MARKETDATA_KEY")
SYMBOL  = "AAPL"
BASE    = "https://api.marketdata.app/v1/options"

print("🚀 Starting MarketData GEX Builder (v3)")
print(f"Symbol: {SYMBOL}")
print(f"API key present: {'Yes' if API_KEY else 'No'}")

# ───────────────────────────────────────────────
# 1️⃣  Get list of option symbols
# ───────────────────────────────────────────────
chain_url = f"{BASE}/chain/{SYMBOL}?token={API_KEY}"
print(f"Requesting chain: {chain_url}")
r = requests.get(chain_url, timeout=30)

if r.status_code not in (200, 203):
    print(f"❌ Chain request failed ({r.status_code}): {r.text[:400]}")
    raise SystemExit(1)

chain_data = r.json()
option_symbols = chain_data.get("optionSymbol", [])
print(f"Returned {len(option_symbols)} option symbols")

if not option_symbols:
    print("⚠️ No option symbols found — check API plan or symbol")
    raise SystemExit(0)

option_symbols = option_symbols[:400]  # safety limit

# ───────────────────────────────────────────────
# 2️⃣  Fetch snapshots and compute GEX
# ───────────────────────────────────────────────
rows = []
for i, sym in enumerate(option_symbols, start=1):
    snap_url = f"{BASE}/snapshot/{sym}?token={API_KEY}"
    try:
        sr = requests.get(snap_url, timeout=15)
        if sr.status_code not in (200, 203):
            continue
        d = sr.json()
        strike = d.get("strike")
        oi = d.get("openInterest")
        gamma = d.get("gamma")
        underlying = d.get("underlyingPrice")
        if all([strike, oi, gamma, underlying]):
            gex = gamma * oi * 100 * underlying
            rows.append({"strike": strike, "GEX": gex})
    except Exception as e:
        print(f"⚠️ Error on {sym}: {e}")
    time.sleep(0.1)  # stay within rate limits

print(f"✅ Processed {len(rows)} valid contracts")

# ───────────────────────────────────────────────
# 3️⃣  Save results
# ───────────────────────────────────────────────
df = pd.DataFrame(rows)
if not df.empty and "strike" in df.columns:
    df = df.sort_values("strike")
    fname = f"{SYMBOL}_GEX.csv"
    df.to_csv(fname, index=False)
    print(f"✅ {datetime.now()}  Saved {fname}  ({len(df)} strikes)")
else:
    print("⚠️ No valid GEX rows — check API response or limits.")

print("🏁 Finished run.")
