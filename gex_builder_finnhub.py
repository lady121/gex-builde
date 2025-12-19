# gex_builder_finnhub.py
# -------------------------------------------------------
# GEX Builder using Finnhub.io free API
# Computes Gamma via Black-Scholes, saves CSV per ticker
# -------------------------------------------------------

import os
import math
import time
import requests
import pandas as pd
from datetime import datetime, date

FINNHUB_KEY = os.getenv("FINNHUB_KEY")
BASE = "https://finnhub.io/api/v1/stock/option-chain"

# ───────────────────────────────────────────────
# Load tickers
# ───────────────────────────────────────────────
TICKERS = []
if os.path.exists("tickers.txt"):
    with open("tickers.txt") as f:
        TICKERS = [x.strip().upper() for x in f if x.strip()]
else:
    TICKERS = ["AAPL", "SPY", "QQQ"]

print("🚀 Starting Finnhub GEX Builder")
print(f"Tickers: {', '.join(TICKERS)}")
print(f"API key present: {'Yes' if FINNHUB_KEY else 'No'}")

# ───────────────────────────────────────────────
# Black–Scholes Gamma Calculation
# ───────────────────────────────────────────────
def compute_gamma(S, K, T, sigma, r=0.01):
    """Return Black–Scholes gamma."""
    try:
        if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
            return 0
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        gamma = math.exp(-0.5 * d1 ** 2) / (S * sigma * math.sqrt(2 * math.pi * T))
        return gamma
    except Exception:
        return 0

# ───────────────────────────────────────────────
# Helper to fetch Finnhub option chain
# ───────────────────────────────────────────────
def fetch_chain(symbol):
    url = f"{BASE}?symbol={symbol}&token={FINNHUB_KEY}"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        print(f"❌ Chain fetch failed for {symbol}: {r.status_code}")
        return None
    return r.json()

# ───────────────────────────────────────────────
# Main GEX computation loop
# ───────────────────────────────────────────────
for symbol in TICKERS:
    print(f"\n📈 Processing {symbol}")
    data = fetch_chain(symbol)
    if not data or "data" not in data:
        print(f"⚠️ No data returned for {symbol}")
        continue

    # underlying price
    underlying = data.get("lastPrice") or data.get("underlying_price")
    if not underlying:
        print(f"⚠️ Missing underlying price for {symbol}")
        continue

    rows = []
    today = date.today()

    # iterate expirations
    for exp in data["data"]:
        expiry_str = exp.get("expirationDate")
        if not expiry_str:
            continue
        expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
        T = max((expiry_date - today).days, 1) / 365.0

        # calls + puts
        for side in ["CALL", "PUT"]:
            options = exp.get(side.lower()) or []
            for opt in options:
                strike = opt.get("strike")
                iv = opt.get("impliedVolatility")
                oi = opt.get("openInterest")

                if not all([strike, iv, oi]):
                    continue

                gamma = compute_gamma(underlying, strike, T, iv)
                gex = gamma * oi * 100 * underlying
                rows.append({
                    "strike": strike,
                    "expiration": expiry_str,
                    "type": side,
                    "gamma": gamma,
                    "openInterest": oi,
                    "GEX": gex
                })

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"⚠️ No valid GEX data for {symbol}")
        continue

    df = df.sort_values(["expiration", "strike"])
    fname = f"{symbol}_GEX.csv"
    df.to_csv(fname, index=False)
    print(f"✅ Saved {fname} ({len(df)} rows)")

print("\n🏁 Finished all tickers.")
