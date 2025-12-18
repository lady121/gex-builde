# gex_builder.py
# -------------------------------------------------------
# MarketData.app GEX Builder (v4.2)
# Supports multiple tickers via tickers.txt
# Fetches option chains, computes GEX, saves CSV per symbol
# -------------------------------------------------------

import os
import time
import requests
import pandas as pd
from datetime import datetime

API_KEY = os.getenv("MARKETDATA_KEY")
BASE = "https://api.marketdata.app/v1/options"

# ───────────────────────────────────────────────
# 1️⃣ Load tickers list
# ───────────────────────────────────────────────
TICKERS = []
if os.path.exists("tickers.txt"):
    with open("tickers.txt") as f:
        TICKERS = [x.strip().upper() for x in f if x.strip()]
else:
    TICKERS = ["AAPL", "SPY", "QQQ"]  # fallback if file missing

print("🚀 Starting MarketData Multi-GEX Builder")
print(f"Tickers: {', '.join(TICKERS)}")
print(f"API key present: {'Yes' if API_KEY else 'No'}")


# ───────────────────────────────────────────────
# 2️⃣ Helper functions
# ───────────────────────────────────────────────
def fetch_chain(symbol):
    """Get list of option symbols for a ticker."""
    url = f"{BASE}/chain/{symbol}?token={API_KEY}"
    r = requests.get(url, timeout=30)
    if r.status_code not in (200, 203):
        print(f"❌ Chain fail for {symbol}: {r.status_code}")
        return []
    data = r.json()
    return data.get("optionSymbol", [])[:400]  # limit to 400 to respect rate limits


def fetch_snapshot(option_symbol):
    """Get single option snapshot with gamma and OI."""
    url = f"{BASE}/snapshot/{option_symbol}?token={API_KEY}"
    r = requests.get(url, timeout=15)
    if r.status_code not in (200, 203):
        return None
    return r.json()


# ───────────────────────────────────────────────
# 3️⃣ Main GEX processing loop
# ───────────────────────────────────────────────
for symbol in TICKERS:
    print(f"\n📈 Processing {symbol}")
    option_list = fetch_chain(symbol)
    print(f"Found {len(option_list)} option symbols")

    rows = []
    for i, opt_sym in enumerate(option_list, start=1):
        snap = fetch_snapshot(opt_sym)
        if not snap:
            continue

        try:
            strike = snap.get("strike")
            oi = snap.get("openInterest")
            gamma = snap.get("gamma")
            underlying = snap.get("underlyingPrice")

            if all([strike, oi, gamma, underlying]):
                gex = gamma * oi * 100 * underlying
                rows.append({"strike": strike, "GEX": gex})
        except Exception as e:
            print(f"⚠️ Error on {opt_sym}: {e}")

        # small sleep to respect rate limits
        time.sleep(0.1)

    # Save results
    df = pd.DataFrame(rows)
    if not df.empty and "strike" in df.columns:
        df = df.sort_values("strike")
        filename = f"{symbol}_GEX.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Saved {filename} ({len(df)} strikes)")
    else:
        print(f"⚠️ No data returned for {symbol}")

print("\n🏁 Finished all tickers.")
