import requests
import pandas as pd
import io
import re
import os
import subprocess

# === CONFIGURATION ===
REPO_PATH = "lady121/gex-builde"
BASE_GITHUB_URL = "https://raw.githubusercontent.com"
GITHUB_API_URL = f"https://api.github.com/repos/{REPO_PATH}/contents"
OUTPUT_FILE = "GEX_Master_Indicator.pine"

# === AUTO-DETECT TICKERS ===
def get_all_tickers_from_repo(repo_api_url):
    tickers = set()
    try:
        resp = requests.get(repo_api_url)
        resp.raise_for_status()
        files = resp.json()
        for file in files:
            name = file["name"]
            match = re.match(r"([A-Z]+)_GEX_\d{8}\.csv", name)
            if match:
                tickers.add(match.group(1))
    except Exception as e:
        print(f"[!] Failed to auto-detect tickers: {e}")
    return sorted(list(tickers))

def fetch_data_for_symbol(symbol, repo):
    print(f"--- Processing {symbol} ---")
    try:
        latest_url = f"{BASE_GITHUB_URL}/{repo}/main/latest.txt"
        file_date = requests.get(latest_url).text.strip()
    except Exception as e:
        print(f"  [X] Error fetching date for {symbol}: {e}")
        return None, None

    try:
        csv_url = f"{BASE_GITHUB_URL}/{repo}/main/{symbol}_GEX_{file_date}.csv"
        csv_resp = requests.get(csv_url)
        csv_resp.raise_for_status()
        df = pd.read_csv(io.StringIO(csv_resp.text), header=None)
        df_clean = pd.DataFrame({
            "strike": pd.to_numeric(df[0], errors="coerce"),
            "gex": pd.to_numeric(df[4], errors="coerce")
        }).dropna()
        df_clean = df_clean.sort_values(by="strike")
        print(f"  [OK] {symbol}: {len(df_clean)} levels loaded.")
        return file_date, df_clean
    except Exception as e:
        print(f"  [X] Failed to fetch or parse CSV for {symbol}: {e}")
        return None, None

def generate_master_pine_script(repo):
    print("\n=== STARTING AUTO-GEX MASTER GENERATION ===")
    symbols = get_all_tickers_from_repo(GITHUB_API_URL)
    if not symbols:
        print("[!] No tickers found in repo.")
        return

    print(f"Detected tickers: {', '.join(symbols)}")

    pine_data_blocks = ""
    first_date = ""
    valid_syms = []

    for sym in symbols:
        file_date, df = fetch_data_for_symbol(sym, repo)
        if df is not None and not df.empty:
            if not first_date:
                first_date = file_date
            s_list = [str(round(x, 2)) for x in df["strike"].tolist()]
            g_list = [str(int(x)) for x in df["gex"].tolist()]
            block = f"""
    if sym == "{sym}"
        s := array.from({', '.join(s_list)})
        g := array.from({', '.join(g_list)})
"""
            pine_data_blocks += block
            valid_syms.append(sym)

    if not valid_syms:
        print("[!] No valid symbols had data.")
        return

    print(f"Building Pine Script for: {', '.join(valid_syms)}")

    pine_code = f"""//V6
indicator("GEX Master Auto: {first_date}", overlay=true, max_boxes_count=500, max_labels_count=500)

// === SETTINGS ===
var float width_scale = input.float(0.5, "Bar Width Scale", minval=0.1, step=0.1)
var float text_size_threshold = input.float(100000000, "Text Label Threshold (Notional)")
var bool show_dashboard = input.bool(true, "Show Dashboard")

// === DATA LOADING ===
load_data() =>
    string sym = syminfo.ticker
    float[] s = array.new_float(0)
    float[] g = array.new_float(0)
    // -- AUTO-GENERATED DATA BLOCKS START --
    {pine_data_blocks}
    // -- AUTO-GENERATED DATA BLOCKS END --
    [s, g]

// === MAIN LOGIC ===
if barstate.islast
    [strikes, gex_vals] = load_data()
    if array.size(strikes) > 0
        float max_gex = 0.0
        float total_gex = 0.0
        for i = 0 to array.size(gex_vals) - 1
            val = array.get(gex_vals, i)
            total_gex += val
            if math.abs(val) > max_gex
                max_gex := math.abs(val)

        for i = 0 to array.size(strikes) - 1
            s_price = array.get(strikes, i)
            g_val = array.get(gex_vals, i)
            float len_norm = max_gex > 0 ? (math.abs(g_val) / max_gex) * (50 * width_scale) : 0
            col = g_val > 0 ? color.new(color.green, 40) : color.new(color.red, 40)
            border_col = g_val > 0 ? color.green : color.red
            float box_height = (s_price * 0.0005)
            box.new(bar_index + 5, s_price + box_height, bar_index + 5 + math.round(len_norm), s_price - box_height,
                    border_color=border_col, bgcolor=col)
            if math.abs(g_val) > text_size_threshold
                label.new(bar_index + 5 + math.round(len_norm), s_price,
                    str.tostring(math.round(g_val / 1000000)) + "M",
                    style=label.style_none, textcolor=border_col, size=size.small)

        if show_dashboard
            string regime = total_gex > 0 ? "Short Vol (Pos GEX)" : "Long Vol (Neg GEX)"
            color reg_col = total_gex > 0 ? color.green : color.red
            var table dash = table.new(position.top_right, 1, 1)
            table.cell(dash, 0, 0, syminfo.ticker + " GEX: " +
                str.tostring(math.round(total_gex/1000000)) + "M\\n" + regime + "\\nDate: {first_date}",
                bgcolor=color.new(reg_col, 80), text_color=color.white)
    else
        var table err = table.new(position.bottom_right, 1, 1)
        table.cell(err, 0, 0, "No GEX Data for " + syminfo.ticker, bgcolor=color.gray, text_color=color.white)
"""

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(pine_code)

    print(f"\n✅ SUCCESS! File created: {OUTPUT_FILE}")

    if os.getenv("GITHUB_ACTIONS") == "true":
        print("🔄 Running in GitHub Actions, committing file...")
        subprocess.run(["git", "config", "user.name", "github-actions"])
        subprocess.run(["git", "config", "user.email", "actions@github.com"])
        subprocess.run(["git", "add", "-A"])
        result = subprocess.run(["git", "commit", "-m", f"Auto-update Pine script for {first_date}"])
        if result.returncode != 0:
            print("ℹ️ No new changes to commit.")
        subprocess.run(["git", "push"])
        print("✅ Git push complete.")

if __name__ == "__main__":
    generate_master_pine_script(REPO_PATH)
