import requests
import pandas as pd
import os
import datetime
from concurrent.futures import ThreadPoolExecutor

API_KEY = os.getenv("MARKETDATA_KEY")
BASE_URL = "https://api.marketdata.app/v1/options/chain/"
OUTPUT_DIR = "."
MAX_THREADS = 5

# ====================================================
def load_tickers():
    """Load tickers from tickers.txt dynamically."""
    if not os.path.exists("tickers.txt"):
        print("❌ No tickers.txt found.")
        return []
    with open("tickers.txt", "r") as f:
        tickers = [line.strip().upper() for line in f if line.strip()]
    print(f"✅ Loaded {len(tickers)} tickers: {', '.join(tickers)}")
    return tickers

# ====================================================
def fetch_and_process_ticker(ticker):
    """Fetch MarketData options chain and calculate GEX metrics."""
    try:
        url = f"{BASE_URL}{ticker}?token={API_KEY}"
        resp = requests.get(url, timeout=15)

        if resp.status_code != 200:
            print(f"⚠️  {ticker}: HTTP {resp.status_code}")
            return None

        data = resp.json()
        if not data or "optionSymbol" not in data:
            print(f"⚠️  {ticker}: No valid options data.")
            return None

        # Build DataFrame from the response
        df = pd.DataFrame({
            "strike": data.get("strike", []),
            "gamma": data.get("gamma", []),
            "vanna": data.get("vanna", [0] * len(data["optionSymbol"])),
            "charm": data.get("charm", [0] * len(data["optionSymbol"])),
            "oi": data.get("openInterest", [0] * len(data["optionSymbol"])),
            "underlying": data.get("underlyingPrice", [0] * len(data["optionSymbol"]))
        })

        # Compute GEX (Gamma Exposure)
        df["GEX"] = df["gamma"] * df["oi"] * (df["underlying"] ** 2) * 0.01
        df["VannaExp"] = df["vanna"] * df["oi"] * df["underlying"]
        df["CharmExp"] = df["charm"] * df["oi"] * df["underlying"]
        df["cum_gex"] = df["GEX"].cumsum()

        # Clean and drop invalid
        df = df.dropna()
        df = df[df["strike"] > 0]
        df = df.sort_values("strike")

        if df.empty:
            print(f"⚠️  {ticker}: No valid option chain data.")
            return None

        today = datetime.date.today().strftime("%Y%m%d")
        output_file = os.path.join(OUTPUT_DIR, f"{ticker}_GEX_{today}.csv")
        df.to_csv(output_file, index=False)

        print(f"📊 {ticker}: {len(df)} rows → {output_file}")
        return output_file

    except Exception as e:
        print(f"❌ {ticker}: {e}")
        return None

# ====================================================
def main():
    tickers = load_tickers()
    if not tickers:
        return

    print("🚀 Starting MarketData GEX Builder")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for ticker in tickers:
            results.append(executor.submit(fetch_and_process_ticker, ticker))

    valid_files = [r.result() for r in results if r.result()]

    if not valid_files:
        print("⚠️ No successful tickers — nothing written.")
        return

    today = datetime.date.today().strftime("%Y%m%d")
    with open("latest.txt", "w") as f:
        f.write(today)

    print(f"🕒 Updated latest.txt → {today}")
    print("✅ All done.")

# ====================================================
if __name__ == "__main__":
    main()
