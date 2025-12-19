import os
import requests
import pandas as pd
from datetime import datetime
from time import sleep

# ==========================
# MarketData GEX Builder (v5)
# ==========================

API_KEY = os.getenv("MARKETDATA_KEY") or "YOUR_FALLBACK_KEY_HERE"
BASE_URL = "https://api.marketdata.app/v1/options"

# Read tickers from tickers.txt
with open("tickers.txt") as f:
    TICKERS = [t.strip().upper() for t in f if t.strip()]

print("🚀 Starting MarketData Multi-GEX Builder (v5)")
print(f"Tickers: {', '.join(TICKERS)}")
print(f"API key present: {'Yes' if API_KEY else 'No'}\n")

def get_option_chain(ticker):
    url = f"{BASE_URL}/chain/{ticker}?token={API_KEY}"
    r = requests.get(url)
    if r.status_code != 200:
        print(f"❌ Chain fetch failed for {ticker}: {r.status_code}")
        return None

    data = r.json()
    if data.get("s") != "ok" or "optionSymbol" not in data:
        print(f"⚠️ No valid chain data for {ticker}")
        return None

    return data["optionSymbol"]

def get_option_quote(symbol):
    url = f"{BASE_URL}/quotes/{symbol}?token={API_KEY}"
    r = requests.get(url)
    if r.status_code != 200:
        return None
    data = r.json()
    if data.get("s") != "ok":
        return None
    return data.get("results", [{}])[0]

def build_gex_for_ticker(ticker):
    print(f"📈 Processing {ticker}")
    symbols = get_option_chain(ticker)
    if not symbols:
        return pd.DataFrame()

    rows = []
    count = 0
    for sym in symbols[:400]:  # limit for free/trial plans
        q = get_option_quote(sym)
        if q and "greeks" in q:
            greeks = q["greeks"]
            try:
                rows.append({
                    "symbol": sym,
                    "strike": q.get("strike"),
                    "gamma": greeks.get("gamma"),
                    "open_interest": q.get("open_interest"),
                    "last": q.get("last"),
                })
                count += 1
            except Exception:
                continue
        sleep(0.05)  # small delay to avoid rate limits

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"⚠️ No valid option data found for {ticker}\n")
        return df

    df = df.sort_values("strike").reset_index(drop=True)
    out_file = f"{ticker}_GEX_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(out_file, index=False)
    print(f"✅ Saved {ticker} data → {out_file} ({len(df)} rows)\n")
    return df

def main():
    for ticker in TICKERS:
        build_gex_for_ticker(ticker)
    print("🏁 Finished all tickers.")

if __name__ == "__main__":
    main()
