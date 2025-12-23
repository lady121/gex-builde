"""
GEX Builder (CSV-Only Version)
------------------------------
✅ Pulls option-chain data from MarketData.app
✅ Calculates Gamma Exposure (GEX) and Flip Zone
✅ Exports one CSV per ticker + a master gamma_summary.csv
❌ Does not create any TradingView Pine file
"""

import os
import io
import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# === CONFIGURATION ===
API_KEY = os.getenv("MARKETDATA_API_KEY")  # stored in environment or GitHub secret
API_URL = "https://api.marketdata.app/v1/options/chain"
MAX_WORKERS = 5


# === UTILITIES ===
def load_tickers(filename="tickers.txt"):
    """Load tickers from tickers.txt"""
    if not os.path.exists(filename):
        print("❌ No tickers.txt found.")
        return []
    with open(filename) as f:
        tickers = [t.strip().upper() for t in f if t.strip()]
    print(f"✅ Loaded {len(tickers)} tickers: {', '.join(tickers)}")
    return tickers


def fetch_chain(ticker):
    """Fetch option-chain data from MarketData.app"""
    try:
        resp = requests.get(f"{API_URL}/{ticker}", params={"token": API_KEY}, timeout=20)
        if resp.status_code != 200:
            print(f"⚠️  {ticker}: HTTP {resp.status_code}")
            return None
        data = resp.json()
        if not data or data.get("s") != "ok":
            print(f"⚠️  {ticker}: invalid response {data}")
            return None
        return data
    except Exception as e:
        print(f"⚠️  {ticker}: fetch failed ({e})")
        return None


def compute_gex(df, underlying_price):
    """Compute Gamma Exposure and cumulative GEX"""
    df["GEX"] = df["gamma"] * df["oi"] * (underlying_price ** 2) * 0.01
    df["cum_gex"] = df["GEX"].cumsum()
    return df


def summarize(df, symbol):
    """Compute Flip Zone and Dealer Regime"""
    total_gex = df["GEX"].sum()
    flip_idx = (df["cum_gex"] - total_gex / 2).abs().idxmin()
    flip_zone = df.loc[flip_idx, "strike"]
    dealer_regime = "Short Vol (Pos GEX)" if total_gex > 0 else "Long Vol (Neg GEX)"
    return {
        "symbol": symbol,
        "total_gex": total_gex,
        "flip_zone": flip_zone,
        "gamma_max": df["GEX"].max(),
        "gamma_min": df["GEX"].min(),
        "dealer_regime": dealer_regime,
    }


def process_ticker(symbol, output_dir):
    """Process one ticker and output CSV"""
    data = fetch_chain(symbol)
    if not data:
        return None

    u_price = data.get("underlyingPrice")
    if isinstance(u_price, list):
        u_price = u_price[0] if u_price else None
    if not u_price:
        print(f"⚠️  {symbol}: missing underlying price.")
        return None

    rows = []
    for o in data.get("options", []):
        try:
            strike = float(o.get("strike"))
            gamma = float(o.get("gamma", 0))
            oi = int(o.get("openInterest", 0))
            rows.append({"strike": strike, "gamma": gamma, "oi": oi})
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"⚠️  {symbol}: no valid option data.")
        return None

    df = df.sort_values("strike")
    df = compute_gex(df, float(u_price))
    summary = summarize(df, symbol)

    # Save CSV
    date_str = datetime.now().strftime("%Y%m%d")
    out_path = os.path.join(output_dir, f"{symbol}_GEX_{date_str}.csv")
    df.to_csv(out_path, index=False)
    print(f"📊  {symbol}: {len(df)} rows → {out_path}")

    return summary


def main():
    tickers = load_tickers()
    if not tickers:
        return

    output_dir = "."
    date_str = datetime.now().strftime("%Y%m%d")
    results = []

    print("🚀 Starting MarketData GEX Builder")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process_ticker, t, output_dir): t for t in tickers}
        for f in as_completed(futures):
            res = f.result()
            if res:
                results.append(res)

    if results:
        pd.DataFrame(results).to_csv("gamma_summary.csv", index=False)
        with open("latest.txt", "w") as f:
            f.write(date_str)
        print(f"🕒 Updated latest.txt → {date_str}")
        print("✅ All tickers processed successfully.")
    else:
        print("⚠️ No successful tickers — nothing written.")


if __name__ == "__main__":
    main()
