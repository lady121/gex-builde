# ===========================================================
# GEX to Pine Script Converter v3.8 (Option 3: Compression)
# ===========================================================
# Features:
#   ✅ OPTION 3 IMPLEMENTATION: Compressed String Data
#   ✅ FIX: Solves "Too many tokens" by packing arrays into text
#   ✅ FIX: Uses integer 'switch' for fast date lookups
#   ✅ Runtime Decoding: Pine Script parses the string on the fly
#   ✅ Configuration: Auto-truncates history if needed
# ===========================================================

import os
import json
import sys
import pandas as pd
import numpy as np
from datetime import datetime

print("🌲 Starting Historical GEX Converter (v3.8 - Compressed Mode)...")

# ===============================================
# Configuration
# ===============================================
# To ensure we stay under 100k tokens, we limit history depth.
# 250 trading days is approx 1 year of data per ticker.
MAX_DAYS_PER_TICKER = 250 

# ===============================================
# Command-line flags
# ===============================================
force_rebuild = "--force" in sys.argv
if force_rebuild:
    print("⚙️ Force rebuild enabled — ignoring cache and rebuilding everything.")

# ===============================================
# Helper Functions
# ===============================================
def compute_flip_zone(df):
    try:
        df_sorted = df.sort_values("strike").reset_index(drop=True)
        df_sorted["cum_gex"] = df_sorted["net_gex"].cumsum()
        signs = np.sign(df_sorted["cum_gex"])
        flips = np.where(np.diff(signs))[0]
        if len(flips) > 0:
            idx = flips[0]
            low = df_sorted.loc[idx, "strike"]
            high = df_sorted.loc[idx + 1, "strike"]
            return (low + high) / 2
    except:
        return None
    return None

def process_file_data(filepath):
    try:
        df = pd.read_csv(filepath)
        required_cols = ['strike', 'call_gex', 'put_gex', 'net_gex']
        if not all(col in df.columns for col in required_cols):
            return None

        # 1️⃣ Compute metrics
        flip_zone = compute_flip_zone(df)
        
        # Walls
        cw_idx = df['call_gex'].idxmax()
        call_wall = df.loc[cw_idx, 'strike']

        pw_idx = df['put_gex'].abs().idxmax()
        put_wall = df.loc[pw_idx, 'strike']

        # 2️⃣ Prepare Histogram Data (Compressed)
        # We store raw Strike:NetGex pairs and let Pine calc the rest
        df['abs_net_gex'] = df['net_gex'].abs()
        top_strikes = df.sort_values('abs_net_gex', ascending=False).head(40) # Keep top 40 significant levels
        max_val = top_strikes['abs_net_gex'].max()
        if max_val == 0: max_val = 1.0

        # Build Compact String: "strike:net_gex;strike:net_gex"
        # Example: "4000:500000;4010:-25000"
        data_pairs = []
        for _, row in top_strikes.iterrows():
            s = float(row['strike'])
            g = float(row['net_gex'])
            # Rounding to 2 decimal places saves string space tokens
            data_pairs.append(f"{s:.2f}:{g:.0f}")
        
        compact_string = ";".join(data_pairs)

        return {
            "flip": float(flip_zone) if flip_zone else None,
            "c_wall": float(call_wall),
            "p_wall": float(put_wall),
            "max_val": float(max_val),
            "data_str": compact_string
        }

    except Exception as e:
        print(f"   ⚠️ Error processing {filepath}: {e}")
        return None

# ===============================================
# Smart Rebuild System
# ===============================================
cache_file = ".gex_cache.json"
if os.path.exists(cache_file):
    try:
        with open(cache_file, "r") as f:
            cache = json.load(f)
    except:
        cache = {}
else:
    cache = {}

files = [f for f in os.listdir('.') if f.endswith('.csv') and "GEX" in f]
files.sort()

if not files:
    print("⚠️ No GEX CSV files found.")
    exit()

# Build current state map
current_state = {}
for f in files:
    parts = f.split('_')
    if len(parts) < 4:
        continue
    symbol = parts[0].upper()
    current_state.setdefault(symbol, []).append(f)
for sym in current_state:
    current_state[sym].sort()

changed_symbols = []
if not force_rebuild:
    for sym, file_list in current_state.items():
        if sym not in cache or cache[sym] != file_list:
            changed_symbols.append(sym)
    if not changed_symbols:
        print("✅ No new data detected. Skipping rebuild.")
        exit()
else:
    changed_symbols = list(current_state.keys())

# ===============================================
# Process Data
# ===============================================
history_map = {}

print("⏳ Processing CSV files (compressing data)...")
for f in files:
    parts = f.split('_')
    if len(parts) < 4:
        continue
    symbol = parts[0].upper()
    date_str = parts[-1].replace('.csv', '')
    if len(date_str) != 8:
        continue
    
    data = process_file_data(f)
    if data:
        # Create integer date: 20250102
        date_int = int(date_str)
        
        history_map.setdefault(symbol, []).append({
            "date_int": date_int,
            "data": data
        })

# Sort by date
for sym in history_map:
    history_map[sym].sort(key=lambda x: x['date_int'])
    # Optional: Truncate very old history to save tokens if necessary
    if len(history_map[sym]) > MAX_DAYS_PER_TICKER:
        history_map[sym] = history_map[sym][-MAX_DAYS_PER_TICKER:]

# ===============================================
# Generate PineScript Output (Highly Optimized)
# ===============================================
output_filename = "Universal_GEX_History.pine"

pine_code = f"""//@version=6
indicator("Universal GEX History (Bar Replay)", overlay=true, max_lines_count=500, max_labels_count=500)

// --- Generated {datetime.now().strftime('%Y-%m-%d')} ---
// Mode: Option 3 (Compressed String Data)

var string current_ticker = syminfo.ticker

// --- Helper: Format large numbers ($B, $M, $K) ---
f_fmt(float v) =>
    float av = math.abs(v)
    string s = ""
    if av >= 1000000000
        s := str.format("${{0,number,#.##}}B", v / 1000000000)
    else if av >= 1000000
        s := str.format("${{0,number,#.##}}M", v / 1000000)
    else if av >= 1000
        s := str.format("${{0,number,#.##}}K", v / 1000)
    else
        s := str.format("${{0,number,#}}", v)
    s

// --- Helper: Parse and Draw Histogram from Compressed String ---
f_draw_gex(string d_str, float max_v) =>
    if d_str != ""
        // Split pairs by semicolon
        string[] pairs = str.split(d_str, ";")
        if array.size(pairs) > 0
            for i = 0 to array.size(pairs) - 1
                string p = array.get(pairs, i)
                // Split strike:gex by colon
                string[] parts = str.split(p, ":")
                if array.size(parts) == 2
                    float strike = str.tonumber(array.get(parts, 0))
                    float net_gex = str.tonumber(array.get(parts, 1))
                    
                    // Calc Length
                    int length = int((math.abs(net_gex) / max_v) * 40)
                    if length < 2 
                        length := 2
                    
                    // Style
                    color c = net_gex >= 0 ? color.new(color.green, 40) : color.new(color.red, 40)
                    color tc = net_gex >= 0 ? color.green : color.red
                    string txt = f_fmt(net_gex)
                    
                    // Draw
                    line.new(bar_index, strike, bar_index + length, strike, color=c, width=2)
                    label.new(bar_index + length, strike, txt, style=label.style_label_left, textcolor=tc, color=color.new(color.white, 100), size=size.small)

"""

# --- Symbol Functions (Using Switch on Date Int) ---
for symbol, records in history_map.items():
    func_name = f"f_symbol_{symbol}"
    
    # Function Definition
    # Returns [c_wall, p_wall, flip, data_str, max_val]
    pine_code += f"""
// {symbol}
{func_name}(int ymd) =>
    float cw = na, float pw = na, float fl = na, string d = "", float mv = na
    [cw, pw, fl, d, mv] = switch ymd
"""
    
    # Generate switch cases
    for rec in records:
        d_int = rec['date_int']
        d_dat = rec['data']
        flip_val = d_dat['flip'] if d_dat['flip'] is not None else "na"
        
        # This one-liner is the key to token savings
        pine_code += f'        {d_int} => [{d_dat["c_wall"]}, {d_dat["p_wall"]}, {flip_val}, "{d_dat["data_str"]}", {d_dat["max_val"]}]\n'
    
    # Default return for unknown dates
    pine_code += f'        => [float(na), float(na), float(na), "", float(na)]\n'

# --- Main Logic ---
pine_code += """
// ===== MAIN EXECUTION =====
int ymd = year * 10000 + month * 100 + dayofmonth

// Declare vars using tuple unpacking from switch
[plot_c_wall, plot_p_wall, plot_flip, data_str, max_val] = switch current_ticker
"""

for symbol in history_map.keys():
    func_name = f"f_symbol_{symbol}"
    pine_code += f'    "{symbol}" => {func_name}(ymd)\n'

# Default case
pine_code += '    => [float(na), float(na), float(na), "", float(na)]\n'

# --- Plotting ---
pine_code += """
plot(plot_c_wall, "Call Wall", color=color.new(color.green, 20), linewidth=2, style=plot.style_stepline)
plot(plot_p_wall, "Put Wall",  color=color.new(color.red, 20),   linewidth=2, style=plot.style_stepline)
plot(plot_flip,   "Flip Zone", color=color.blue, linewidth=1, style=plot.style_circles)

// Defer heavy drawing to runtime function
f_draw_gex(data_str, max_val)
"""

# ===============================================
# Write Files
# ===============================================
with open(output_filename, "w") as f:
    f.write(pine_code)

with open(cache_file, "w") as f:
    json.dump(current_state, f, indent=2)

print(f"✅ Created {output_filename} (v3.8 - Compressed)")
print(f"📊 Processed {len(history_map)} tickers.")
