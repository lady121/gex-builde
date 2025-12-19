# gex_builder.py
# -------------------------------------------------------
# MarketData.app GEX Builder (v5)
# Uses /v1/options/quotes/{optionSymbol}/ endpoint
# -------------------------------------------------------

import os
import time
import requests
import pandas as pd
from datetime import datetime

API_KEY = os.getenv("MARKETDATA_KEY")
BASE = "https://api.marketdata.app/v1/options"

# Load tickers from tickers.txt
TICKERS = []
if os.path.exists("tickers.txt"):
    with open("tickers.txt") as f:
        TICKERS = [x.strip().upper() for x in f if x.strip()]
else:
    TICKERS = ["SPY", "QQQ", "NVDA"]

print("🚀 Starting MarketData GEX Builder (v5 – quotes endpoint)")
print(f"Tickers: {', '.join(TICKERS)}")
print(f"API key present: {'Yes' if API_KEY else 'No'}")

def fetch_chain(symbol):
    """Return list of option contract symbols for the ticker."""
    url = f"{BASE}/chain/{symbol}?token={API_KEY}"
    r = requests.get(url, timeout=30)
    if r.status_code not in (200, 203):
        print(f"❌ Chain fail for {symbol}: {r.status_code}")
        return []
    data = r.json()
    return data.get("optionSymbol", [])[:400]  # limit to 400 to respect rate limits

def fetch_quote(option_symbol):
    """Return option quote with greeks + OI."""
    url = f"{BASE}/quotes/{option_symbol}?token={API_KEY}"
    r = requests.get(url, timeout=15)
    if r.status_code not in (200, 203):
        return None
    return r.json()

# -------------------------------------------------------
# Main loop
# -------------------------------------------------------
for symbol in TICKERS:
    print(f"\n📈 Processing {symbol}")
    option_list = fetch_chain(symbol)
    print(f"Found {len(option_list)} option symbols")

    rows = []
    for i, opt_sym in enumerate(option_list, start=1):
        data = fetch_quote(opt_sym)
        if not data:
            continue

        # Debug: print first 2 samples
        if i <= 2:
            print(f"🔍 Sample for {symbol}: {data}")

        try:
            strike = data.get("strike") or data.get("strikePrice")
            oi = data.get("open_interest") or data.get("openInterest")
            gamma = None
            greeks = data.get("greeks")
            if isinstance(greeks, dict):
                gamma = greeks.get("gamma")
            underlying = (
                data.get("underlying_price")
                or data.get("underlyingPrice")
            )

            if all([strike, oi, gamma, underlying]):
                gex = gamma * oi * 100 * underlying
                rows.append({"strike": strike, "GEX": gex})
        except Exception as e:
            print(f"⚠️ Error on {opt_sym}: {e}")

        time.sleep(0.1)  # prevent API throttling

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("strike")
        fname = f"{symbol}_GEX.csv"
        df.to_csv(fname, index=False)
        print(f"✅ Saved {fname} ({len(df)} strikes)")
    else:
        print(f"⚠️ No valid data for {symbol}")

print("\n🏁 Finished all tickers.")
