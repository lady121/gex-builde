import requests
import pandas as pd
import os
from datetime import datetime
import time

# === CONFIGURATION ===
MARKETDATA_TOKEN = os.getenv("MARKETDATA_TOKEN")
BASE_URL = "https://api.marketdata.app/v1/options"
OUTPUT_DIR = "."
TICKER_FILE = "tickers.txt"

# === LOAD TICKERS ===
def load_tickers_from_file(filename=TICKER_FILE):
    try:
        with open(filename, "r") as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
        print(f"✅ Loaded {len(tickers)} tickers: {', '.join(tickers)}")
        return tickers
    except Exception as e:
        print(f"⚠️ Could not read {filename}: {e}")
        return ["SPY", "QQQ"]

# === FETCH OPTION CHAIN (FULL DATA) ===
def fetch_chain(symbol):
    url = f"{BASE_URL}/chain/{symbol}?token={MARKETDATA_TOKEN}"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            print(f"⚠️ {symbol}: HTTP {r.status_code}")
            return None
        data = r.json()
        if data.get("s") != "ok":
            print(f"⚠️ {symbol}: API returned {data.get('s')}")
            return None

        # Ensure arrays exist
        keys = ["strike", "gamma", "openInterest", "underlyingPrice"]
        if not all(k in data for k in keys):
            print(f"⚠️ {symbol}: Missing keys in response.")
            return None

        df = pd.DataFrame({
            "strike": data["strike"],
            "gamma": data["gamma"],
            "oi": data["openInterest"],
            "underlying": data["underlyingPrice"],
        })
        df["GEX"] = df["gamma"] * df["oi"] * 100
        df = df.dropna().sort_values("strike")
        print(f"✅ {symbol}: {len(df)} records processed.")
        return df
    except Exception as e:
        print(f"❌ {symbol}: Error fetching data: {e}")
        return None

# === SAVE CSV ===
def save_csv(symbol, df):
    date_str = datetime.utcnow().strftime("%Y%m%d")
    filename = f"{OUTPUT_DIR}/{symbol}_GEX_{date_str}.csv"
    try:
        df.to_csv(filename, index=False, header=False)
        print(f"💾 Saved {symbol} → {filename}")
        return True
    except Exception as e:
        print(f"❌ Error saving {symbol}: {e}")
        return False

# === UPDATE LATEST.TXT ===
def update_latest():
    date_str = datetime.utcnow().strftime("%Y%m%d")
    with open("latest.txt", "w") as f:
        f.write(date_str)
    print(f"🕒 Updated latest.txt → {date_str}")

# === MAIN ===
def run_builder():
    print("🚀 Starting MarketData GEX Builder (Original Mode)")
    tickers = load_tickers_from_file()
    for sym in tickers:
        print(f"\n📈 Processing {sym}")
        df = fetch_chain(sym)
        if df is not None and not df.empty:
            save_csv(sym, df)
        time.sleep(1)
    update_latest()
    print("\n🏁 Done.")

if __name__ == "__main__":
    run_builder()
