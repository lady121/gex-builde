# ===========================================================
# MarketData.app GEX Builder v7.3 — Full Gamma Suite (Stable)
# Author: PulsR | Maintained by Code GPT
# ===========================================================
# Features:
#  ✅ Flip Zone Detection (where net cumulative GEX crosses zero)
#  ✅ Gamma Squeeze Index (GSI)
#  ✅ Call/Put/Net GEX analytics
#  ✅ Pressure Zones & Gamma Neutral Zone
#  ✅ API-based live MarketData.app option data
#  ✅ Safe handling for missing/variant API fields
#  ✅ Skips incomplete quotes (no 'strike' errors)
#  ✅ Produces CSV, TXT, and PNG files (optional)
# ===========================================================

import os
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt

# ===============================================
# Configuration
# ===============================================
API_KEY = os.getenv("MARKETDATA_KEY") or "YOUR_BACKUP_TOKEN"
BASE_URL = "https://api.marketdata.app/v1/options"
ENABLE_PLOTS = True
MAX_OPTIONS = 400

# ===============================================
# Load tickers
# ===============================================
if os.path.exists("tickers.txt"):
    with open("tickers.txt") as f:
        TICKERS = [t.strip().upper() for t in f if t.strip()]
else:
    TICKERS = ["SPY", "QQQ", "NVDA"]

print("🚀 Starting MarketData GEX Builder (v7.3 — Full Gamma Suite)")
print(f"Tickers: {', '.join(TICKERS)}")
print(f"API key present: {'Yes' if API_KEY else 'No'}\n")


# ===============================================
# Helper Functions
# ===============================================
def get_chain(symbol):
    """Fetch the list of option symbols for a ticker."""
    url = f"{BASE_URL}/chain/{symbol}?token={API_KEY}"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code not in (200, 203):
            print(f"❌ Chain fetch failed for {symbol}: {r.status_code}")
            return []
        data = r.json()
        if data.get("s") != "ok":
            print(f"⚠️ No valid chain data for {symbol}")
            return []
        return data.get("optionSymbol", [])
    except Exception as e:
        print(f"❌ Error fetching chain for {symbol}: {e}")
        return []


def get_quote(option_symbol):
    """Fetch individual option quote data."""
    url = f"{BASE_URL}/quotes/{option_symbol}?token={API_KEY}"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code not in (200, 203):
            return None
        data = r.json()
        if data.get("s") != "ok":
            return None
        return data
    except Exception:
        return None


def infer_option_type(symbol_str):
    """Guess if an option is a Call or Put from its symbol."""
    if "C" in symbol_str and "P" not in symbol_str:
        return "C"
    if "P" in symbol_str and "C" not in symbol_str:
        return "P"
    if symbol_str.endswith("C"):
        return "C"
    elif symbol_str.endswith("P"):
        return "P"
    return None


def safe_extract(d, keys, default=None):
    """Try multiple keys safely from a dict."""
    if not isinstance(d, dict):
        return default
    for k in keys:
        val = d.get(k)
        if val is not None:
            if isinstance(val, list) and len(val) > 0:
                return val[0]
            return val
    return default


def compute_flip_zone(df):
    """Find flip zone strike where cumulative GEX crosses zero."""
    df_sorted = df.sort_values("strike").reset_index(drop=True)
    df_sorted["cum_gex"] = df_sorted["net_gex"].cumsum()
    signs = np.sign(df_sorted["cum_gex"])
    flips = np.where(np.diff(signs))[0]
    if len(flips) > 0:
        low = df_sorted.loc[flips[0], "strike"]
        high = df_sorted.loc[flips[0] + 1, "strike"]
        return (low + high) / 2
    return None


# ===============================================
# Main GEX Builder
# ===============================================
def build_gex(symbol):
    print(f"\n📈 Processing {symbol}")
    chain = get_chain(symbol)
    if not chain:
        print(f"⚠️ No option symbols found for {symbol}")
        return None, None

    rows = []
    for i, opt in enumerate(chain[:MAX_OPTIONS]):
        q = get_quote(opt)
        if not q:
            continue
        try:
            strike = safe_extract(q, ["strike", "strikePrice", "strike_price"])
            gamma = safe_extract(q, ["gamma"])
            oi = safe_extract(q, ["openInterest", "open_interest", "oi"])
            underlying = safe_extract(q, ["underlyingPrice", "underlying_price", "underlying"])

            if any(v is None for v in [strike, gamma, oi, underlying]):
                continue  # skip incomplete quote

            gex = gamma * oi * 100 * underlying
            otype = infer_option_type(opt)
            if otype:
                rows.append({
                    "strike": strike,
                    "gamma": gamma,
                    "oi": oi,
                    "underlying": underlying,
                    "GEX": gex,
                    "type": otype
                })
        except Exception as e:
            print(f"⚠️ Skipping {opt} due to parsing error: {e}")
            continue

        if i % 25 == 0:
            time.sleep(0.2)  # throttle to avoid rate limits

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"⚠️ No valid GEX data for {symbol}")
        return None, None

    # ===============================================
    # Aggregation and Computation
    # ===============================================
    grouped = df.groupby(["strike", "type"])["GEX"].sum().unstack(fill_value=0)
    grouped.rename(columns={"C": "call_gex", "P": "put_gex"}, inplace=True)

    # Ensure both columns exist
    if "call_gex" not in grouped.columns:
        grouped["call_gex"] = 0.0
    if "put_gex" not in grouped.columns:
        grouped["put_gex"] = 0.0

    if df["type"].nunique() < 2:
        print(f"⚠️ Only one option type returned for {symbol} — results may be partial.")

    grouped["net_gex"] = grouped["call_gex"] - grouped["put_gex"]
    grouped["abs_net_gex"] = grouped["net_gex"].abs()

    # Flip Zone
    flip_zone = compute_flip_zone(grouped)

    # Gamma Squeeze Index (GSI)
    grouped["GSI"] = np.where(
        (abs(grouped["call_gex"]) + abs(grouped["put_gex"])) > 0,
        abs(grouped["call_gex"]) / (abs(grouped["call_gex"]) + abs(grouped["put_gex"])),
        np.nan,
    )

    # Pressure Zones (top 10%)
    cutoff = grouped["abs_net_gex"].quantile(0.9)
    grouped["pressure_zone"] = grouped["abs_net_gex"] >= cutoff

    grouped["cum_gex"] = grouped["net_gex"].cumsum()

    # ===============================================
    # Save Outputs
    # ===============================================
    date_tag = datetime.now().strftime("%Y%m%d")
    fname = f"{symbol}_GEX_full_{date_tag}.csv"
    grouped.reset_index().to_csv(fname, index=False)
    print(f"✅ Saved {fname} ({len(grouped)} rows)")

    # ===============================================
    # Optional Plot
    # ===============================================
    if ENABLE_PLOTS:
        plt.figure(figsize=(10, 5))
        plt.plot(grouped.index, grouped["net_gex"], label="Net GEX", linewidth=1.8)
        plt.axhline(0, color="black", linestyle="--")
        if flip_zone:
            plt.axvline(flip_zone, color="red", linestyle="--", label=f"Flip Zone ~ {flip_zone:.2f}")
        plt.title(f"{symbol} — Net Gamma Exposure ({date_tag})")
        plt.xlabel("Strike Price")
        plt.ylabel("Net GEX")
        plt.legend()
        plt.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(f"{symbol}_GEX_plot_{date_tag}.png", dpi=150)
        plt.close()

    return fname, flip_zone


# ===============================================
# Main Execution Loop
# ===============================================
generated_files = []
flip_summary = {}

for ticker in TICKERS:
    try:
        result, flip = build_gex(ticker)
        if result:
            generated_files.append(result)
        if flip:
            flip_summary[ticker] = flip
    except Exception as e:
        print(f"❌ Error while processing {ticker}: {e}")

# ===============================================
# Write Summary Files
# ===============================================
if generated_files:
    latest_date = datetime.now().strftime("%Y%m%d")
    with open("latest.txt", "w") as f:
        f.write(latest_date)
    print(f"\n🕒 Updated latest.txt with {latest_date}")

if flip_summary:
    with open("flip_zones.txt", "w") as f:
        for k, v in flip_summary.items():
            f.write(f"{k}: {v:.2f}\n")
    print("📘 Saved flip_zones.txt summary.")

print("\n🏁 Finished all tickers — Full Gamma Suite complete.")
