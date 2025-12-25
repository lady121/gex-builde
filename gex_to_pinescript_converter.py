# ===========================================================
# GEX to Pine Script Converter (Universal Edition)
# ===========================================================
# Usage:
# 1. Scans folder for ALL GEX CSV files (e.g., SPY_GEX_....csv).
# 2. Generates a SINGLE "Universal_GEX_Suite.pine" file.
# 3. The Pine Script automatically switches data based on
#    the chart's ticker (syminfo.ticker).
# ===========================================================

import os
import pandas as pd
import numpy as np
from datetime import datetime

print("🌲 Starting Universal GEX Converter...")

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
    """
    Extracts the formatted string data for Pine Script arrays from a single CSV.
    Returns a dict of data strings.
    """
    try:
        df = pd.read_csv(filepath)
        required_cols = ['strike', 'call_gex', 'put_gex', 'net_gex']
        if not all(col in df.columns for col in required_cols):
            return None

        # 1. Calc Metrics
        flip_zone = compute_flip_zone(df)
        call_wall = df.loc[df['call_gex'].idxmax(), 'strike']
        put_wall = df.loc[df['put_gex'].idxmax(), 'strike']

        # 2. Top 50 Active Strikes
        df['abs_net_gex'] = df['net_gex'].abs()
        top_strikes = df.sort_values('abs_net_gex', ascending=False).head(50)
        
        # Normalize
        max_val = top_strikes['abs_net_gex'].max()
        if max_val == 0: max_val = 1
        
        p_strikes = []
        p_lengths = []
        p_signs = []
        
        for _, row in top_strikes.iterrows():
            length = int((row['abs_net_gex'] / max_val) * 30)
            if length < 2: length = 2
            
            p_strikes.append(str(float(row['strike'])))
            p_lengths.append(str(length))
            p_signs.append("1" if row['net_gex'] >= 0 else "-1")

        return {
            "flip": str(flip_zone) if flip_zone else 'na',
            "c_wall": str(float(call_wall)),
            "p_wall": str(float(put_wall)),
            "strikes": ', '.join(p_strikes),
            "lengths": ', '.join(p_lengths),
            "signs": ', '.join(p_signs)
        }
    except Exception as e:
        print(f"   ⚠️ Error processing {filepath}: {e}")
        return None

# ===============================================
# Main Execution
# ===============================================
# Look for CSVs in the current directory (Root of repo when running in Action)
files = [f for f in os.listdir('.') if f.endswith('.csv') and "GEX" in f]
data_map = {} # Key: Symbol, Value: Data Dict

if not files:
    print("⚠️ No GEX CSV files found.")
    exit()

print(f"📂 Found {len(files)} CSV files. Aggregating...")

for f in files:
    # Extract Symbol from filename (e.g. "SPY_GEX_..." -> "SPY")
    symbol = f.split('_')[0].upper()
    print(f"   Processing {symbol}...")
    
    data = process_file_data(f)
    if data:
        data_map[symbol] = data

# ===============================================
# Write Single Universal Pine Script
# ===============================================
date_tag = datetime.now().strftime("%Y-%m-%d")
output_filename = f"Universal_GEX_Suite_{datetime.now().strftime('%Y%m%d')}.pine"

pine_code = f"""//@version=5
indicator("Universal GEX Suite", overlay=true, max_lines_count=500, max_labels_count=500)

// --- Universal GEX Suite ---
// Generated: {date_tag}
// Contains Data for: {', '.join(data_map.keys())}
// Logic: Automatically detects the ticker on your chart and loads the correct GEX profile.

// --- Variables ---
var float[] strikes = array.new_float()
var int[] lengths   = array.new_int()
var int[] signs     = array.new_int()
var float flip_level = na
var float c_wall_price = na
var float p_wall_price = na
var string current_ticker = syminfo.ticker

// --- Data Injection ---
"""

# Inject data blocks for each ticker
for symbol, d in data_map.items():
    pine_code += f"""
if current_ticker == "{symbol}"
    strikes := array.from({d['strikes']})
    lengths := array.from({d['lengths']})
    signs   := array.from({d['signs']})
    flip_level := {d['flip']}
    c_wall_price := {d['c_wall']}
    p_wall_price := {d['p_wall']}
"""

pine_code += """
// --- Plotting Logic ---
if barstate.islast
    // 1. Draw Flip Zone
    if not na(flip_level)
        line.new(bar_index, flip_level, bar_index + 10, flip_level, color=color.blue, width=2, style=line.style_dashed)
        label.new(bar_index + 10, flip_level, "Flip: " + str.tostring(flip_level, "#.##"), style=label.style_label_left, textcolor=color.white, color=color.blue)

    // 2. Draw Walls
    if not na(c_wall_price)
        label.new(bar_index + 5, c_wall_price, "Call Wall: " + str.tostring(c_wall_price), style=label.style_label_left, textcolor=color.white, color=color.green)
    if not na(p_wall_price)
        label.new(bar_index + 5, p_wall_price, "Put Wall: " + str.tostring(p_wall_price), style=label.style_label_left, textcolor=color.white, color=color.red)

    // 3. Draw Histogram
    if array.size(strikes) > 0
        for i = 0 to array.size(strikes) - 1
            float s = array.get(strikes, i)
            int l   = array.get(lengths, i)
            int sg  = array.get(signs, i)
            
            col = sg > 0 ? color.new(color.green, 40) : color.new(color.red, 40)
            line.new(bar_index, s, bar_index + l, s, color=col, width=2)
"""

with open(output_filename, "w") as f:
    f.write(pine_code)

print(f"✅ Successfully created: {output_filename}")
