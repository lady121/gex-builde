# gex_builder.py
# -------------------------------------------------------
# Pulls option-chain data from Massive (Polygon.io rebrand),
# computes per-strike Gamma Exposure (GEX),
# and writes <SYMBOL>_GEX.csv for TradingView.
# -------------------------------------------------------

import os
import math
import requests
import pandas as pd
from datetime import datetime

# ───────────────────────────────────────────────
# 1.  Configuration
# ───────────────────────────────────────────────
API_KEY = os.getenv("API_KEY")  # GitHub secret MASSIVE_KEY will populate this
SYMBOL  = "AAPL"                # change or loop through tickers if you like

BASE_URL = "https://api.massive.com/v3/snapshot/options"

# ───────────────────────────────────────────────
# 2.  Fetch option snapshot from Massive
# ───────────────────────────────────────────────
url = f"{BASE_URL}/{SYMBOL}?apiKey={API_KEY}"
print(f"Requesting: {url}")

try:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
except Exception as e:
    print(f"❌  Request failed: {e}")
    raise SystemExit(1)

# quick peek at what came back
print(f"HTTP {r.status_code}")
if isinstance(data, dict):
    print(f"Top-level keys: {list(data.keys())}")
else:
    print(f"Returned type: {type(data)}")

# Massive returns results list under "results"
results = data.get("results", [])
print(f"Results count: {len(results)}")

# ───────────────────────────────────────────────
# 3.  Build rows and compute GEX
# ───────────────────────────────────────────────
rows = []
for opt in results:
    details = opt.get("details", {})
    greeks  = opt.get("greeks", {})
    strike  = details.get("strike_price")
    oi      = opt.get("open_interest")
    gamma   = greeks.get("gamma")
    under   = opt.get("underlying_asset", {}).get("price")

    if all([strike, oi, gamma, under]):
        gex = gamma * oi * 100 * under
        rows.append({"strike": strike, "GEX": gex})

# ───────────────────────────────────────────────
# 4.  Save or warn
# ───────────────────────────────────────────────
if rows:
    df = pd.DataFrame(rows).sort_values("strike")
    filename = f"{SYMBOL}_GEX.csv"
    df.to_csv(filename, index=False)
    print(f"✅  {datetime.now()}  Saved {filename}  ({len(df)} strikes)")
else:
    print("⚠️  No valid option data returned from Massive. "
          "Check API key, symbol, or market hours.")

# ───────────────────────────────────────────────
# 5.  Done
# ───────────────────────────────────────────────
print("Finished run.")
