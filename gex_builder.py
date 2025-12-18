# gex_builder.py
# -------------------------------------------------------
# Pulls option-chain data from Massive (formerly Polygon.io),
# computes per-strike Gamma Exposure (GEX),
# and writes <SYMBOL>_GEX.csv for TradingView.
# -------------------------------------------------------

import os
import requests
import pandas as pd
from datetime import datetime

# ───────────────────────────────────────────────
# 1.  Configuration
# ───────────────────────────────────────────────
API_KEY = os.getenv("API_KEY")          # must match your GitHub secret name
SYMBOL  = "AAPL"                        # change if desired
BASE_URL = "https://api.massive.com"

# ───────────────────────────────────────────────
# 2.  Try main snapshot endpoint first
# ───────────────────────────────────────────────
url = f"{BASE_URL}/v3/snapshot/options/{SYMBOL}?apiKey={API_KEY}"
print(f"Requesting: {url}")

try:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
except Exception as e:
    print(f"❌ Request failed: {e}")
    raise SystemExit(1)

print(f"HTTP {r.status_code}")
print("First 500 chars of response:")
print(str(data)[:500])
results = data.get("results", [])

# ───────────────────────────────────────────────
# 3.  If snapshot returned nothing, try contracts endpoint
# ───────────────────────────────────────────────
if not results:
    alt_url = f"{BASE_URL}/v3/reference/options/contracts?ticker={SYMBOL}&apiKey={API_KEY}"
    print(f"⚠️ No results in snapshot; trying contracts endpoint:\n{alt_url}")
    try:
        r2 = requests.get(alt_url, timeout=30)
        r2.raise_for_status()
        data = r2.json()
        results = data.get("results", [])
        print(f"Contracts endpoint returned {len(results)} records")
    except Exception as e:
        print(f"❌ Second request failed: {e}")
        results = []

# ───────────────────────────────────────────────
# 4.  Compute GEX
# ───────────────────────────────────────────────
rows = []
for opt in results:
    details = opt.get("details", {})
    greeks  = opt.get("greeks", {})
    strike  = details.get("strike_price") or opt.get("strike_price")
    oi      = opt.get("open_interest")
    gamma   = greeks.get("gamma") or opt.get("gamma")
    under   = opt.get("underlying_asset", {}).get("price") or opt.get("underlying_price")

    if all([strike, oi, gamma, under]):
        gex = gamma * oi * 100 * under
        rows.append({"strike": strike, "GEX": gex})

# ───────────────────────────────────────────────
# 5.  Save or warn
# ───────────────────────────────────────────────
df = pd.DataFrame(rows)
if "strike" in df.columns and not df.empty:
    df = df.sort_values("strike")
    filename = f"{SYMBOL}_GEX.csv"
    df.to_csv(filename, index=False)
    print(f"✅ {datetime.now()} Saved {filename} ({len(df)} strikes)")
else:
    print("⚠️ No option data or 'strike' column found. "
          "Check API key, symbol, or endpoint response.")

print("Finished run.")
