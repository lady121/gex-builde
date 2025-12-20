import os
import time
import requests
import pandas as pd
from datetime import datetime

API_KEY = os.getenv("MARKETDATA_KEY")
TICKERS = ["SPY", "QQQ", "NVDA", "BBAI", "RR", "BMNU"]
BASE_URL = "https://api.marketdata.app/v1/options/chain/"
HEADERS = {"User-Agent": "GEX-Builder/1.0"}

def fetch_option_chain(symbol, retries=3, delay=5):
    """Fetch option chain data from MarketData with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            url = f"{BASE_URL}{symbol}?token={API_KEY}"
            response = requests.get(url, headers=HEADERS, timeout=20)
            if response.status_code == 200:
                data = response.json()
                if "optionSymbol" in data:
                    print(f"✅ [{symbol}] Chain fetched successfully ({len(data['optionSymbol'])} symbols)")
                    return data
                else:
                    print(f"⚠️ [{symbol}] No 'optionSymbol' in response.")
                    return None
            elif response.status_code == 203:
                print(f"⚠️ [{symbol}] Access limited (203) – check plan permissions.")
                return None
            else:
                print(f"⚠️ [{symbol}] HTTP {response.status_code}")
        except Exception as e:
            print(f"❌ [{symbol}] Attempt {attempt}/{retries} failed: {e}")
        if attempt < retries:
            wait = delay * attempt
            print(f"🔁 Retrying {symbol} in {wait}s...")
            time.sleep(wait)
    print(f"🚫 [{symbol}] Failed after {retries} retries.")
    return None


def build_gex(symbol):
    """Build GEX dataframe for a given ticker."""
    data = fetch_option_chain(symbol)
    if not data or "optionSymbol" not in data:
        print(f"⚠️ Skipping {symbol}: No data received.")
        return None

    rows = []
    for i, opt in enumerate(data["optionSymbol"]):
        try:
            strike = data.get("strike", [])[i] if "strike" in data else None
            gamma = data.get("gamma", [0])[i] if "gamma" in data else 0
            oi = data.get("openInterest", [0])[i] if "openInterest" in data else 0
            under = data.get("underlying", [symbol])[i] if "underlying" in data else symbol
            gex = gamma * oi * 100 if gamma and oi else 0
            if strike:
                rows.append([strike, gamma, oi, under, gex])
        except Exception as e:
            print(f"⚠️ [{symbol}] Parsing error: {e}")

    if not rows:
        print(f"⚠️ [{symbol}] No valid rows to save.")
        return None

    df = pd.DataFrame(rows, columns=["strike", "gamma", "oi", "underlying", "GEX"]).sort_values("strike")
    date_tag = datetime.now().strftime("%Y%m%d")
    fname = f"{symbol}_GEX_{date_tag}.csv"
    df.to_csv(fname, index=False)
    print(f"💾 [{symbol}] Saved {fname} ({len(df)} rows)")
    return fname


def main():
    print("🚀 Starting MarketData Multi-GEX Builder (v6 Stable)")
    if not API_KEY:
        print("❌ Missing MARKETDATA_KEY environment variable.")
        return

    saved = []
    for symbol in TICKERS:
        print(f"\n📈 Processing {symbol}")
        fname = build_gex(symbol)
        if fname:
            saved.append(fname)

    if saved:
        latest_file = "latest.txt"
        date_tag = datetime.now().strftime("%Y%m%d")
        with open(latest_file, "w") as f:
            f.write(date_tag)
        print(f"🕒 Updated {latest_file} with {date_tag}")
    else:
        print("⚠️ No valid CSVs created.")

    print("\n🏁 GEX Builder finished successfully.")


if __name__ == "__main__":
    main()
