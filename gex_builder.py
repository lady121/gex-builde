import requests
import pandas as pd
import io
import time
import os
from datetime import datetime

# === CONFIGURATION ===
# Token comes from GitHub Secret
MARKETDATA_TOKEN = os.getenv("MARKETDATA_TOKEN")  # Automatically set from GitHub Secrets
BASE_URL = "https://api.marketdata.app/v1/options"
OUTPUT_DIR = "."
LOG_DIR = "logs"
TICKER_FILE = "tickers.txt"

# === Ensure log directory exists ===
os.makedirs(LOG_DIR, exist_ok=True)

# === LOAD TICKERS FROM FILE ===
def load_tickers_from_file(filename=TICKER_FILE):
    try:
        with open(filename, "r") as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
        print(f"✅ Loaded {len(tickers)} tickers from {filename}: {', '.join(tickers)}")
        return tickers
    except Exception as e:
        print(f"⚠️ Could not load {filename}, using fallback list. Error: {e}")
        return ["SPY", "QQQ", "NVDA"]

# === FETCH OPTION DATA FOR A SINGLE TICKER ===
def fetch_option_chain(symbol):
    if not MARKETDATA_TOKEN:
        print("❌ No MarketData token found! Please add it as a GitHub secret named MARKETDATA_TOKEN.")
        return None
    url = f"{BASE_URL}/chain/{symbol}?token={MARKETDATA_TOKEN}"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            data = r.json()
            if "s" in data and data["s"] == "ok":
                count = len(data.get("optionSymbol", []))
                print(f"📊 {symbol}: {count} option contracts found.")
                return data
            else:
                print(f"⚠️ {symbol}: No option data returned (s=no_data).")
        else:
            print(f"⚠️ {symbol}: HTTP {r.status_code}.")
    except Exception as e:
        print(f"❌ {symbol}: Exception fetching chain -> {e}")
    return None

# === COMPUTE GEX FROM OPTION CHAIN ===
def build_gex_dataframe(symbol, chain_json):
    try:
        df = pd.DataFrame({
            "strike": chain_json.get("strike", []),
            "gamma": chain_json.get("gamma", []),
            "oi": chain_json.get("openInterest", []),
            "underlying": chain_json.get("underlyingPrice", []),
        })
        df["GEX"] = df["gamma"] * df["oi"] * 100
        df = df.dropna()
        if df.empty:
            print(f"⚠️ {symbol}: No valid GEX data to save.")
            return None
        return df
    except Exception as e:
        print(f"❌ {symbol}: Error building dataframe -> {e}")
        return None

# === SAVE CSV ===
def save_csv(symbol, df):
    date_str = datetime.utcnow().strftime("%Y%m%d")
    filename = f"{OUTPUT_DIR}/{symbol}_GEX_{date_str}.csv"
    try:
        df.to_csv(filename, index=False, header=False)
        print(f"✅ Saved {symbol} → {filename} ({len(df)} rows)")
        return True
    except Exception as e:
        print(f"❌ Failed to save {symbol}: {e}")
        return False

# === WRITE latest.txt ===
def update_latest_file():
    date_str = datetime.utcnow().strftime("%Y%m%d")
    try:
        with open(f"{OUTPUT_DIR}/latest.txt", "w") as f:
            f.write(date_str)
        print(f"🕒 Updated latest.txt → {date_str}")
    except Exception as e:
        print(f"⚠️ Failed to update latest.txt: {e}")

# === MAIN BUILDER ===
def run_gex_builder():
    print("🚀 Starting MarketData GEX Builder (Dynamic Ticker Mode)")
    tickers = load_tickers_from_file()

    for symbol in tickers:
        print(f"\n📈 Processing {symbol}")
        chain = fetch_option_chain(symbol)
        if not chain:
            continue

        df = build_gex_dataframe(symbol, chain)
        if df is None or df.empty:
            continue

        save_csv(symbol, df)
        time.sleep(0.5)

    update_latest_file()
    print("\n🏁 Finished all tickers.")

if __name__ == "__main__":
    run_gex_builder()
