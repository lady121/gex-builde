import requests
import pandas as pd
import io
import os
import subprocess

# === CONFIGURATION ===
REPO_PATH = "lady121/gex-builde"
BASE_GITHUB_URL = "https://raw.githubusercontent.com"

def load_tickers():
    """Load tickers from tickers.txt dynamically"""
    if not os.path.exists("tickers.txt"):
        print("❌ No tickers.txt found.")
        return []
    with open("tickers.txt", "r") as f:
        tickers = [line.strip().upper() for line in f if line.strip()]
    print(f"✅ Loaded tickers: {', '.join(tickers)}")
    return tickers

def fetch_data_for_symbol(symbol, repo):
    """Fetch GEX data for one ticker"""
    try:
        # Get latest date from repo
        latest_url = f"{BASE_GITHUB_URL}/{repo}/main/latest.txt"
        date_resp = requests.get(latest_url)
        date_resp.raise_for_status()
        file_date = date_resp.text.strip().replace("\n", "")
    except Exception as e:
        print(f"❌ {symbol}: Failed to fetch date → {e}")
        return None, None

    try:
        csv_url = f"{BASE_GITHUB_URL}/{repo}/main/{symbol}_GEX_{file_date}.csv"
        csv_resp = requests.get(csv_url)
        csv_resp.raise_for_status()
        df = pd.read_csv(io.StringIO(csv_resp.text))

        # Clean & prepare data
        df_raw = pd.DataFrame({
            "strike": pd.to_numeric(df.iloc[:, 0], errors="coerce"),
            "gex": pd.to_numeric(df.iloc[:, 4], errors="coerce"),
        }).dropna()

        # === FIX 1: MERGE DUPLICATE STRIKES ===
        # This prevents "labels on top of each other" and halves the object count.
        df_clean = df_raw.groupby("strike", as_index=False)["gex"].sum()
        df_clean = df_clean.sort_values(by="strike")

        print(f"✅ {symbol}: {len(df_clean)} unique strikes loaded (merged).")
        return file_date, df_clean

    except Exception as e:
        print(f"⚠️ {symbol}: Could not fetch or parse CSV ({e})")
        return None, None

def generate_master_pine_script(repo):
    print("=== STARTING AUTO-GEX MASTER GENERATION (V5) ===")
    symbols = load_tickers()
    if not symbols:
        print("❌ No tickers found. Exiting.")
        return

    pine_data_blocks = ""
    first_date = ""
    successful_symbols = []

    for sym in symbols:
        file_date, df = fetch_data_for_symbol(sym, repo)
        if df is not None and not df.empty:
            if not first_date:
                first_date = file_date

            s_list = [str(round(x, 2)) for x in df["strike"].tolist()]
            g_list = [str(int(x)) for x in df["gex"].tolist()]

            s_str = ", ".join(s_list)
            g_str = ", ".join(g_list)

            block = f"""
    if sym == "{sym}"
        s := array.from({s_str})
        g := array.from({g_str})
"""
            pine_data_blocks += block
            successful_symbols.append(sym)

    if not successful_symbols:
        print("❌ No valid GEX data found for any symbols. Aborting.")
        return

    print(f"✅ Building Pine Script for: {', '.join(successful_symbols)}")

    # === Build Final Pine Script (V5 Fixes) ===
    # 1. 'strike_range_pct' input added to filter lines outside visible area.
    # 2. This drastic reduction in objects prevents the "following/floating" glitches.
    # 3. Uses extend.both (Infinite Horizontal Lines) which are grid-like and stable.
    
    pine_code = f"""//@version=6
indicator("GEX Master Auto V5: {first_date}", overlay=true, max_lines_count=500, max_labels_count=500, max_boxes_count=500)

// === SETTINGS ===
var float width_scale = input.float(0.5, "Bar Width Scale", minval=0.1, step=0.1)
var float text_size_threshold = input.float(1000000, "Text Label Threshold (Notional)", tooltip="Only show labels where GEX > this value")
var float strike_range_pct = input.float(15.0, "Visible Range %", minval=1.0, step=1.0, tooltip="Only draw lines within this % of the current price. Reduces lag and clutter.")
var bool show_dashboard = input.bool(true, "Show Dashboard")

// === STORAGE FOR DRAWING OBJECTS ===
var line[] drawn_lines = array.new_line()
var label[] drawn_labels = array.new_label()

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
    // 1. CLEAR OLD DRAWINGS
    // Essential for keeping chart clean when data updates
    while array.size(drawn_lines) > 0
        line.delete(array.pop(drawn_lines))
    while array.size(drawn_labels) > 0
        label.delete(array.pop(drawn_labels))

    [strikes, gex_vals] = load_data()
    
    // === V5 RANGE FILTER ===
    // We calculate a "Safe Zone" around the current price.
    // Lines outside this zone are NOT drawn. This keeps us under the 500 line limit.
    float upper_bound = close * (1 + strike_range_pct / 100)
    float lower_bound = close * (1 - strike_range_pct / 100)
    
    if array.size(strikes) > 0
        float max_gex = 0.0
        float total_gex = 0.0

        // Calculate Max/Total first
        for i = 0 to array.size(gex_vals) - 1
            val = array.get(gex_vals, i)
            total_gex += val
            if math.abs(val) > max_gex
                max_gex := math.abs(val)

        // === Improved Gamma Visualization ===
        for i = 0 to array.size(strikes) - 1
            s_price = array.get(strikes, i)
            g_val = array.get(gex_vals, i)

            // === V5 FILTER LOGIC ===
            // Only proceed if the strike is relevant to current price action
            if s_price >= lower_bound and s_price <= upper_bound

                // Draw line for each strike
                color bar_color = g_val > 0 ? color.new(color.green, 0) : color.new(color.red, 0)
                color label_color = g_val > 0 ? color.green : color.red
                
                // Infinite Grid Lines (extend.both) anchored to TIME (safer than bar_index)
                line l = line.new(time, s_price, time + 1, s_price, 
                                  xloc=xloc.bar_time, extend=extend.both, 
                                  color=bar_color, width=2)
                array.push(drawn_lines, l)

                // Label Logic
                if math.abs(g_val) >= text_size_threshold
                    // 3-Way Stagger to prevent overlap
                    // We use time-based offset (approx 12 hours * multiplier)
                    int x_offset_ms = 1000 * 60 * 60 * 12
                    if i % 3 == 1
                        x_offset_ms := x_offset_ms * 2
                    else if i % 3 == 2
                        x_offset_ms := x_offset_ms * 3
                    
                    string txt = str.tostring(s_price) + "\\n" + str.tostring(math.round(g_val / 1000000)) + "M"
                    
                    // Locked to Price (yloc.price)
                    label lbl = label.new(time + x_offset_ms, s_price, txt, 
                                          xloc=xloc.bar_time, yloc=yloc.price, 
                                          style=label.style_label_left,
                                          textcolor=color.white, color=color.new(label_color, 40), size=size.normal)
                    array.push(drawn_labels, lbl)

        // === Dashboard Summary ===
        if show_dashboard
            string regime = total_gex > 0 ? "Short Vol (Pos GEX)" : "Long Vol (Neg GEX)"
            color reg_col = total_gex > 0 ? color.green : color.red
            var table dash = table.new(position.top_right, 1, 1)
            table.cell(dash, 0, 0,
                syminfo.ticker + " GEX: " + str.tostring(math.round(total_gex/1000000)) +
                "M\\n" + regime + "\\nDate: {first_date}",
                text_color=color.white, bgcolor=color.new(reg_col, 80))
    else
        var table err = table.new(position.bottom_right, 1, 1)
        table.cell(err, 0, 0, "No GEX Data for " + syminfo.ticker,
                   text_color=color.white, bgcolor=color.gray)
"""

    filename = "GEX_Master_Indicator.pine"
    with open(filename, "w") as f:
        f.write(pine_code)

    print(f"✅ SUCCESS! File created: {filename}")

    # Auto commit if running in GitHub Actions
    if os.getenv("GITHUB_ACTIONS"):
        try:
            print("🔄 Running in GitHub Actions, committing file...")
            subprocess.run(["git", "add", filename], check=False)
            subprocess.run(["git", "commit", "-m", f"Auto-update Pine script for {first_date}"], check=False)
            subprocess.run(["git", "push"], check=False)
        except Exception as e:
            print(f"⚠️ Commit skipped: {e}")

if __name__ == "__main__":
    generate_master_pine_script(REPO_PATH)
