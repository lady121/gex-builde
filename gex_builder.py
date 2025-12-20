# gex_builder.py
# ==================================================
# MarketData.app GEX Builder (v6.1)
# Builds strike-level GEX CSVs for all tickers in tickers.txt
# and automatically writes the latest date into latest.txt
# ==================================================

import os
import time
import requests
import pandas as pd
from datetime import datetime

API_KEY = os.getenv("MARKETDATA_KEY") or "YOUR_BACKUP_TOKEN"
BASE_URL = "https://api.marketdata.app/v1/options"

# Load tickers
if os.path.exists("tickers.txt"):
    with open("tickers.txt") as f:
        TICKERS = [t.strip().upper() for t in f if t.strip()]
else:
    TICKERS = ["SPY", "QQQ", "NVDA"]

print("🚀 Starting MarketData GEX Builder (v6.1)")
print(f"Tickers: {', '.join(TICKERS)}")
print(f"API key present: {'Yes' if API_KEY else 'No'}\n")

def get_chain(symbol):
    """Get the list of option contract symbols."""
    url = f"{BASE_URL}/chain/{symbol}?token={API_KEY}"
    r = requests.get(url, timeout=20)
    if r.status_code not in (200, 203):
        print(f"❌ Chain fetch failed for {symbol}: {r.status_code}")
        return []
    data = r.json()
    if data.get("s") != "ok":
        print(f"⚠️ No valid chain data for {symbol}")
        return []
    return data.get("optionSymbol", [])

def get_quote(option_symbol):
    """Get the quote for an individual option contract."""
    url = f"{BASE_URL}/quotes/{option_symbol}?token={API_KEY}"
    r = requests.get(url, timeout=15)
    if r.status_code not in (200, 203):
        return None
    data = r.json()
    if data.get("s") != "ok":
        return None
    return data

def build_gex(symbol):
    print(f"📈 Processing {symbol}")
    chain = get_chain(symbol)
    if not chain:
        print(f"⚠️ No option symbols found for {symbol}")
        return None

    rows = []
    for i, opt in enumerate(chain[:400]):  # limit for speed & rate control
        q = get_quote(opt)
        if not q:
            continue

        try:
            strike = q.get("strike", [None])[0] if isinstance(q.get("strike"), list) else q.get("strike")
            gamma = q.get("gamma", [None])[0] if isinstance(q.get("gamma"), list) else q.get("gamma")
            oi = q.get("openInterest", [None])[0] if isinstance(q.get("openInterest"), list) else q.get("openInterest")
            underlying = q.get("underlyingPrice", [None])[0] if isinstance(q.get("underlyingPrice"), list) else q.get("underlyingPrice")

            if all(v is not None for v in [strike, gamma, oi, underlying]):
                gex = gamma * oi * 100 * underlying
                rows.append({
                    "strike": strike,
                    "gamma": gamma,
                    "oi": oi,
                    "underlying": underlying,
                    "GEX": gex
                })
        except Exception:
            continue

        if i % 25 == 0:
            time.sleep(0.2)  # avoid throttling

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"⚠️ No valid GEX data for {symbol}")
        return None

    df = df.sort_values("strike").reset_index(drop=True)
    date_tag = datetime.now().strftime("%Y%m%d")
    fname = f"{symbol}_GEX_{date_tag}.csv"
    df.to_csv(fname, index=False)
    print(f"✅ Saved {fname} ({len(df)} rows)")
    return fname


# ===========================
# Main
# ===========================
generated_files = []
for ticker in TICKERS:
    result = build_gex(ticker)
    if result:
        generated_files.append(result)

# Write latest.txt for TradingView auto-fetch
if generated_files:
    latest_date = datetime.now().strftime("%Y%m%d")
    with open("latest.txt", "w") as f:
        f.write(latest_date)
    print(f"🕒 Updated latest.txt with {latest_date}")

print("\n🏁 Finished all tickers.")
