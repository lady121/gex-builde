# gex_builder.py
# -------------------------------------------------------
# MarketData.app GEX Builder (chain-full endpoint)
# -------------------------------------------------------

import os, time, requests, pandas as pd
from datetime import datetime

API_KEY = os.getenv("MARKETDATA_KEY")
BASE = "https://api.marketdata.app/v1/options"

# load tickers
TICKERS = []
if os.path.exists("tickers.txt"):
    with open("tickers.txt") as f:
        TICKERS = [x.strip().upper() for x in f if x.strip()]
else:
    TICKERS = ["SPY", "QQQ", "NVDA"]

print("🚀 Starting MarketData GEX Builder – chain-full version")
print(f"Tickers: {', '.join(TICKERS)}")

def fetch_chain_full(symbol):
    url = f"{BASE}/chain-full/{symbol}?token={API_KEY}"
    r = requests.get(url, timeout=45)
    if r.status_code not in (200,203):
        print(f"❌ chain-full failed for {symbol}: {r.status_code}")
        return []
    data = r.json()
    return data.get("data") or data.get("results") or []

for symbol in TICKERS:
    print(f"\n📈 Processing {symbol}")
    chain = fetch_chain_full(symbol)
    print(f"Retrieved {len(chain)} option records")

    rows = []
    for opt in chain:
        strike = opt.get("strike") or opt.get("strikePrice")
        oi = opt.get("open_interest") or opt.get("openInterest")
        gamma = opt.get("gamma") or opt.get("greeks", {}).get("gamma")
        under = opt.get("underlying_price") or opt.get("underlyingPrice")

        if all([strike, oi, gamma, under]):
            gex = gamma * oi * 100 * under
            rows.append({"strike": strike, "GEX": gex})

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("strike")
        fname = f"{symbol}_GEX.csv"
        df.to_csv(fname, index=False)
        print(f"✅ Saved {fname} ({len(df)} strikes)")
    else:
        print(f"⚠️ No valid data for {symbol}")

print("\n🏁 Finished all tickers.")
