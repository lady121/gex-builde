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

        df_clean = pd.DataFrame({
            "strike": pd.to_numeric(df.iloc[:, 0], errors="coerce"),
            "gex": pd.to_numeric(df.iloc[:, 4], errors="coerce"),
        }).dropna()
        df_clean = df_clean.sort_values(by="strike")

        print(f"✅ {symbol}: {len(df_clean)} GEX records loaded.")
        return file_date, df_clean

    except Exception as e:
        print(f"⚠️ {symbol}: Could not fetch or parse CSV ({e})")
        return None, None

def generate_master_pine_script(repo):
    print("=== STARTING AUTO-GEX MASTER GENERATION ===")
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

    # === Pine Script Output ===
    pine_code = f"""//@version=6
indicator("GEX Master Auto: {first_date}", overlay=true, max_boxes_count=500, max_labels_count=500)

// === USER SETTINGS ===
var float width_scale = input.float(0.5, "Bar Width Scale", minval=0.1, step=0.1)
var bool show_dashboard = input.bool(true, "Show Dashboard")

// Custom styling options
max_labels = input.int(20, "Max Labels", minval=5, maxval=100)
line_thickness = input.float(2.0, "Line Thickness", minval=0.5, maxval=10)
label_spacing_mult = input.float(1.0, "Label Spacing (Multiplier)", minval=0.1, maxval=10)
label_size_opt = input.string("normal", "Label Size", options=["tiny", "small", "normal", "large"])

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

        // === Sort by Gamma magnitude and only show top levels ===
        float[] sorted_idx = array.new_int()
        for i = 0 to array.size(gex_vals) - 1
            array.push(sorted_idx, i)

        // ✅ FINAL FIX: Pine v6-compliant one-liner
        array.sort(sorted_idx, (a, b) => (math.abs(array.get(gex_vals, a)) > math.abs(array.get(gex_vals, b)) ? -1 : 1))

        // === Draw Lines and Labels with user control ===
        float spacing = syminfo.mintick * 40 * label_spacing_mult

        for i = 0 to math.min(array.size(sorted_idx), max_labels) - 1
            idx = array.get(sorted_idx, i)
            s_price = array.get(strikes, idx)
            g_val = array.get(gex_vals, idx)

            color bar_color = g_val > 0 ? color.new(color.green, 0) : color.new(color.red, 0)
            color label_color = g_val > 0 ? color.green : color.red

            // Horizontal gamma wall line
            line.new(bar_index - 5, s_price, bar_index + 20, s_price, color=bar_color, width=line_thickness, extend=extend.none)

            // Vertical offset to prevent overlap
            float y_offset = s_price + (spacing * (i % 15))

            // Label showing strike + notional
            string txt = "Strike: " + str.tostring(s_price) + "\\n" + str.tostring(math.round(g_val / 1000000)) + "M"
            label.new(bar_index + 30, y_offset, txt,
                      style=label.style_label_left,
                      textcolor=color.white,
                      color=color.new(label_color, 40),
                      size=label_size_opt)

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
