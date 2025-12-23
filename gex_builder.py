# ==================================================
# MarketData.app GEX Builder (v8.0)
# Full Dealer Positioning Engine:
# GEX + Vanna + Charm + Flip Zone + Summary
# ==================================================

import os
import time
import requests
import pandas as pd
from datetime import datetime
import numpy as np

API_KEY = os.getenv("MARKETDATA_KEY") or "YOUR_BACKUP_TOKEN"
BASE_URL = "https://api.marketdata.app/v1/options"

# === Load Tickers ===
if os.path.exists("tickers.txt"):
    with open("tickers.txt") as f:
        TICKERS = [t.strip().upper() for t in f if t.strip()]
else:
    TICKERS = ["SPY", "QQQ", "NVDA"]

print("🚀 Starting MarketData GEX Builder (v8.0)")
print(f"✅ Loaded {len(TICKERS)} tickers: {', '.join(TICKERS)}")
print(f"🔑 API key present: {'Yes' if API_KEY else 'No'}\n")


# ==================================================
# Helper Functions
# ==================================================
def get_chain(symbol):
    """Get the list of option contracts for a symbol."""
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
    """Get quote and Greeks for an individual option contract."""
    url = f"{BASE_URL}/quotes/{option_symbol}?token={API_KEY}"
    r = requests.get(url, timeout=15)
    if r.status_code not in (200, 203):
        return None
    data = r.json()
    if data.get("s") != "ok":
        return None
    return data


# ==================================================
# GEX + Vanna + Charm Builder
# ==================================================
def build_dealer_metrics(symbol):
    print(f"📈 Processing {symbol}")
    chain = get_chain(symbol)
    if not chain:
        print(f"⚠️ No option symbols found for {symbol}")
        return None

    rows = []
    for i, opt in enumerate(chain[:400]):
        q = get_quote(opt)
        if not q:
            continue

        try:
            strike = q.get("strike", [None])[0] if isinstance(q.get("strike"), list) else q.get("strike")
            gamma = q.get("gamma", [None])[0] if isinstance(q.get("gamma"), list) else q.get("gamma")
            vanna = q.get("vanna", [None])[0] if isinstance(q.get("vanna"), list) else q.get("vanna")
            charm = q.get("charm", [None])[0] if isinstance(q.get("charm"), list) else q.get("charm")
            oi = q.get("openInterest", [None])[0] if isinstance(q.get("openInterest"), list) else q.get("openInterest")
            underlying = q.get("underlyingPrice", [None])[0] if isinstance(q.get("underlyingPrice"), list) else q.get("underlyingPrice")

            if all(v is not None for v in [strike, gamma, oi, underlying]):
                gex = gamma * oi * 100 * underlying
                vanna_exp = (vanna or 0) * oi * 100 * underlying * 0.01
                charm_exp = (charm or 0) * oi * 100 * underlying * 0.01

                rows.append({
                    "strike": strike,
                    "gamma": gamma,
                    "vanna": vanna or 0,
                    "charm": charm or 0,
                    "oi": oi,
                    "underlying": underlying,
                    "GEX": gex,
                    "VannaExp": vanna_exp,
                    "CharmExp": charm_exp
                })
        except Exception:
            continue

        if i % 25 == 0:
            time.sleep(0.2)

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"⚠️ No valid data for {symbol}")
        return None

    df = df.sort_values("strike").reset_index(drop=True)

    # === Core Analytics ===
    total_gex = df["GEX"].sum()
    total_vanna = df["VannaExp"].sum()
    total_charm = df["CharmExp"].sum()

    # Flip Zone (Zero-Gamma)
    df["cum_gex"] = df["GEX"].cumsum()
    zero_gamma_strike = np.nan
    try:
        sign_change = np.where(np.sign(df["cum_gex"]).diff().fillna(0) != 0)[0]
        if len(sign_change) > 0:
            zero_gamma_strike = df.loc[sign_change[0], "strike"]
    except Exception:
        zero_gamma_strike = np.nan

    gamma_max_strike = df.loc[df["GEX"].idxmax(), "strike"]
    gamma_min_strike = df.loc[df["GEX"].idxmin(), "strike"]

    dealer_regime = "Short Vol (Pos GEX)" if total_gex > 0 else "Long Vol (Neg GEX)"

    # === Save Per-Ticker CSV ===
    date_tag = datetime.now().strftime("%Y%m%d")
    fname = f"{symbol}_GEX_{date_tag}.csv"
    df.to_csv(fname, index=False)

    # === Print Summary ===
    print(f"✅ Saved {fname} ({len(df)} rows)")
    print(f"   ↳ Total GEX: {total_gex:,.0f}")
    print(f"   ↳ Total Vanna: {total_vanna:,.0f}")
    print(f"   ↳ Total Charm: {total_charm:,.0f}")
    print(f"   ↳ Flip Zone: {zero_gamma_strike}")
    print(f"   ↳ Dealer Regime: {dealer_regime}\n")

    return {
        "symbol": symbol,
        "file": fname,
        "total_gex": total_gex,
        "total_vanna": total_vanna,
        "total_charm": total_charm,
        "flip_zone": zero_gamma_strike,
        "gamma_max": gamma_max_strike,
        "gamma_min": gamma_min_strike,
        "dealer_regime": dealer_regime
    }


# ==================================================
# MAIN EXECUTION
# ==================================================
generated_files = []
summaries = []

for ticker in TICKERS:
    result = build_dealer_metrics(ticker)
    if result:
        generated_files.append(result["file"])
        summaries.append(result)

# === Write latest.txt for other scripts ===
if generated_files:
    latest_date = datetime.now().strftime("%Y%m%d")
    with open("latest.txt", "w") as f:
        f.write(latest_date)
    print(f"🕒 Updated latest.txt with {latest_date}")

# === Write gamma_summary.csv ===
if summaries:
    df_sum = pd.DataFrame(summaries)
    df_sum.to_csv("gamma_summary.csv", index=False)
    print(f"📊 Created gamma_summary.csv with {len(df_sum)} entries.")

print("\n🏁 Finished all tickers.")
