import requests
import pandas as pd
import concurrent.futures
import os
import time
from datetime import datetime

# === CONFIGURATION ===
MARKETDATA_TOKEN = os.getenv("MARKETDATA_TOKEN")  # read from GitHub Secrets
BASE_URL = "https://api.marketdata.app/v1/options"
OUTPUT_DIR = "."
LOG_DIR = "logs"
TICKER_FILE = "tickers.txt"
MAX_CONTRACTS = 100  # per ticker to avoid rate limits
THREADS = 10         # concurrent requests for Greeks

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

# === FETCH OPTION CHAIN ===
def fetch_option_chain(symbol):
    if not MARKETDATA_TOKEN:
        print("❌ No MarketData token found! Add it as a GitHub secret named MARKETDATA_TOKEN.")
        return None
    url = f"{BASE_URL}/chain/{symbol}?token={MARKETDATA_TOKEN}"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            data = r.json()
            if "s" in data and data["s"] == "ok":
                contracts = data.get("optionSymbol", [])[:MAX_CONTRACTS]
                print(f"📊 {symbol}: {len(contracts)} contracts fetched.")
                return contracts
            else:
                print(f"⚠️ {symbol}: No data returned (s=no_data).")
        else:
            print(f"⚠️ {symbol}: HTTP {r.status_code}.")
    except Exception as e:
        print(f"❌ {symbol}: Error fetching chain -> {e}")
    return None

# === FETCH GREEKS FOR ONE CONTRACT ===
def fetch_greeks(contract):
    g_url = f"{BASE_URL}/greeks/{contract}?token={MARKETDATA_TOKEN}"
    try:
        g = requests.get(g_url, timeout=10)
        if g.status_code == 200:
            j = g.json()
            strike = j.get("strike")
            gamma = j.get("gamma", 0)
            oi = j.get("openInterest", 0)
            under = j.get("underlyingPrice", 0)
            gex_val = (gamma or 0) * (oi or 0) * 100
            if gex_val != 0:
                return [strike, gamma, oi, under, gex_val]
    except Exception as e:
        print(f"⚠️ Error fetching {contract}: {e}")
    return None

# === BUILD GEX DATAFRAME ===
def build_gex_dataframe(symbol, contracts):
    print(f"🔍 Fetching Greeks for {symbol} ({len(contracts)} contracts)...")
    records = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
        for res in executor.map(fetch_greeks, contracts):
            if res:
                records.append(res)
            time.sleep(0.05)
    if not records:
        print(f"⚠️ {symbol}: No valid GEX records.")
        return None
    df = pd.DataFrame(records, columns=["strike", "gamma", "oi", "underlying", "GEX"])
    return df.sort_values("strike")

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

# === WRITE LATEST.TXT ===
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
    print("🚀 Starting MarketData GEX Builder (Multithreaded Mode)")
    tickers = load_tickers_from_file()
    for symbol in tickers:
        print(f"\n📈 Processing {symbol}")
        contracts = fetch_option_chain(symbol)
        if not contracts:
            continue
        df = build_gex_dataframe(symbol, contracts)
        if df is None or df.empty:
            continue
        save_csv(symbol, df)
        time.sleep(0.5)
    update_latest_file()
    print("\n🏁 Finished all tickers.")

if __name__ == "__main__":
    run_gex_builder()
