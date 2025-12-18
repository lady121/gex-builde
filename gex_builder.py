# gex_builder.py
# -------------------------------------------------------
# Pulls option-chain data, computes GEX, and saves to CSV.
# -------------------------------------------------------

import os
import requests
import pandas as pd
from datetime import datetime
import sys

# ───────────────────────────────────────────────
# 1. Configuration
# ───────────────────────────────────────────────
API_KEY = os.getenv("API_KEY")
SYMBOL  = "AAPL"
BASE_URL = "https://api.massive.com/v3/snapshot/options"

# Ensure API Key is present
if not API_KEY:
    print("❌ Error: API_KEY environment variable is missing.")
    sys.exit(1)

# ───────────────────────────────────────────────
# 2. Fetch option snapshot
# ───────────────────────────────────────────────
url = f"{BASE_URL}/{SYMBOL}"
params = {"apiKey": API_KEY}

print(f"Requesting: {url} ...")

try:
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
except requests.exceptions.RequestException as e:
    print(f"❌ Request failed: {e}")
    # If response exists, print it for debugging
    if 'r' in locals() and r is not None:
        print(f"Response text: {r.text[:500]}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Unexpected error: {e}")
    sys.exit(1)

results = data.get("results", [])
print(f"✅ Data received. Found {len(results)} option contracts.")

if not results:
    print("⚠️ Warning: 'results' list is empty. Check market hours or symbol.")
    sys.exit(0)

# ───────────────────────────────────────────────
# 3. Build rows and compute GEX
# ───────────────────────────────────────────────
rows = []
for opt in results:
    details = opt.get("details", {})
    greeks  = opt.get("greeks", {})
    
    strike  = details.get("strike_price")
    oi      = opt.get("open_interest")
    gamma   = greeks.get("gamma")
    
    # Safely get underlying price (handle potential missing nested dicts)
    under_asset = opt.get("underlying_asset")
    under = under_asset.get("price") if isinstance(under_asset, dict) else None

    # Strict check: ensure all values are present and are numbers
    if all(v is not None for v in [strike, oi, gamma, under]):
        try:
            gex = float(gamma) * float(oi) * 100 * float(under)
            rows.append({"strike": float(strike), "GEX": gex})
        except (ValueError, TypeError):
            continue

print(f"Computed GEX for {len(rows)} valid strikes.")

# ───────────────────────────────────────────────
# 4. Save to CSV
# ───────────────────────────────────────────────
# Define columns explicitly to prevent KeyError if rows is empty
df = pd.DataFrame(rows, columns=["strike", "GEX"])

if not df.empty:
    df = df.sort_values("strike")
    filename = f"{SYMBOL}_GEX.csv"
    df.to_csv(filename, index=False)
    print(f"✅ {datetime.now()} Saved {filename} ({len(df)} rows)")
    
    # Print preview
    print("\nTop 5 Strikes by GEX:")
    print(df.nlargest(5, "GEX"))
else:
    print("⚠️ No valid GEX data computed. CSV was not created.")
    # Debug: Print first raw result to see structure if logic failed
    if results:
        print("\nDEBUG: Sample raw data (first item):")
        print(results[0])
