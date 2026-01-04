# ===========================================================
# GEX to Pine Script Converter v2.1 (GEX Values Added)
# ===========================================================
# Usage:
# 1. Scans folder for ALL historical GEX CSV files.
# 2. Generates "Universal_GEX_History.pine" (Version 6).
# 3. Features:
#    - Shows GEX $ Amount on Labels (in Billions).
#    - COMBINED WALL labels.
#    - Smart Flip Zones.
# ===========================================================

import os
import pandas as pd
import numpy as np
from datetime import datetime

print("🌲 Starting Historical GEX Converter (v2.1)...")

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

        # 1. Key Metrics & Wall Strength
        flip_zone = compute_flip_zone(df)
        
        # Find Call Wall (Max Call GEX)
        cw_idx = df['call_gex'].idxmax()
        call_wall = df.loc[cw_idx, 'strike']
        call_wall_gex = df.loc[cw_idx, 'call_gex'] / 1_000_000_000 # Convert to Billions

        # Find Put Wall (Max Put GEX) - usually Put GEX is negative in raw data, but usually stored as positive in call_gex/put_gex columns?
        # In builder: grouped["P"] becomes "put_gex". It's usually positive magnitude. 
        # But net_gex = call - put.
        # We want the magnitude.
        pw_idx = df['put_gex'].abs().idxmax()
        put_wall = df.loc[pw_idx, 'strike']
        put_wall_gex = df.loc[pw_idx, 'put_gex'] / 1_000_000_000 # Convert to Billions (Magnitude)

        # 2. Histogram Data (Top 40 Strikes)
        df['abs_net_gex'] = df['net_gex'].abs()
        top_strikes = df.sort_values('abs_net_gex', ascending=False).head(40)
        
        max_val = top_strikes['abs_net_gex'].max()
        if max_val == 0: max_val = 1
        
        p_strikes = []
        p_lengths = []
        p_signs = []
        
        for _, row in top_strikes.iterrows():
            # Scale length: Min 2, Max 40
            length = int((row['abs_net_gex'] / max_val) * 40)
            if length < 2: length = 2
            
            p_strikes.append(str(float(row['strike'])))
            p_lengths.append(str(length))
            p_signs.append("1" if row['net_gex'] >= 0 else "-1")

        return {
            "flip": float(flip_zone) if flip_zone else None,
            "c_wall": float(call_wall),
            "p_wall": float(put_wall),
            "c_gex": float(call_wall_gex),
            "p_gex": float(put_wall_gex),
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
files = [f for f in os.listdir('.') if f.endswith('.csv') and "GEX" in f and "summary" not in f]
files.sort() 

if not files:
    print("⚠️ No GEX CSV files found.")
    exit()

print(f"📂 Found {len(files)} CSV files. Building History...")

history_map = {} 

for f in files:
    # Expected format: Ticker_GEX_robust_YYYYMMDD.csv
    parts = f.split('_')
    if len(parts) < 4: continue
    
    symbol = parts[0].upper()
    date_str = parts[-1].replace('.csv', '')
    
    if len(date_str) != 8: continue
    
    data = process_file_data(f)
    if data:
        if symbol not in history_map: history_map[symbol] = []
        history_map[symbol].append({
            "year": int(date_str[:4]),
            "month": int(date_str[4:6]),
            "day": int(date_str[6:8]),
            "data": data
        })

# ===============================================
# Write Pine Script (Version 6)
# ===============================================
output_filename = "Universal_GEX_History.pine"

pine_code = f"""//@version=6
indicator("Universal GEX History (Bar Replay)", overlay=true, max_lines_count=500, max_labels_count=500)

// --- Universal GEX History (Auto-Generated) ---
// Generated: {datetime.now().strftime('%Y-%m-%d')}

var string current_ticker = syminfo.ticker

// --- Plot Variables ---
var float plot_c_wall = na
var float plot_p_wall = na
var float plot_flip   = na
var float plot_c_gex  = na
var float plot_p_gex  = na

// --- Arrays for Current Day Histogram ---
var float[] cur_strikes = array.new<float>()
var int[]   cur_lengths = array.new<int>()
var int[]   cur_signs   = array.new<int>()
"""

# INJECT DATA
for symbol, records in history_map.items():
    last_record = records[-1]
    
    pine_code += f"""
// ===== {symbol} DATA =====
if current_ticker == "{symbol}"
"""
    for rec in records:
        y, m, d = rec['year'], rec['month'], rec['day']
        d_dat = rec['data']
        
        # Pine Script: use 'na' literal for missing flips
        flip_val = d_dat['flip'] if d_dat['flip'] is not None else "na"
        
        pine_code += f"""    if year == {y} and month == {m} and dayofmonth == {d}
        plot_c_wall := {d_dat['c_wall']}
        plot_p_wall := {d_dat['p_wall']}
        plot_flip   := {flip_val}
        plot_c_gex  := {d_dat['c_gex']:.4f}
        plot_p_gex  := {d_dat['p_gex']:.4f}
"""

    ld = last_record['data']
    pine_code += f"""
    if barstate.islast
        cur_strikes := array.from({ld['strikes']})
        cur_lengths := array.from({ld['lengths']})
        cur_signs   := array.from({ld['signs']})
"""

# PLOTTING LOGIC
pine_code += """
// --- Plotting History (Lines) ---
plot(plot_c_wall, "Call Wall", color=color.new(color.green, 20), linewidth=2, style=plot.style_stepline)
plot(plot_p_wall, "Put Wall",  color=color.new(color.red, 20),   linewidth=2, style=plot.style_stepline)
plot(plot_flip,   "Flip Zone", color=color.blue,  linewidth=1, style=plot.style_circles)

// --- Plotting Profile (Histogram - Last Bar Only) ---
if barstate.islast
    // 1. Draw Smart Labels (Fix Overlap)
    if not na(plot_c_wall) and not na(plot_p_wall)
        if plot_c_wall == plot_p_wall
            // COMBINED WALL
            string gex_str = "C: $" + str.tostring(plot_c_gex, "#.##") + "B / P: $" + str.tostring(plot_p_gex, "#.##") + "B"
            label.new(bar_index + 5, plot_c_wall, "COMBINED WALL\\n" + gex_str, style=label.style_label_left, textcolor=color.white, color=color.purple)
        else
            // SEPARATE WALLS
            label.new(bar_index + 5, plot_c_wall, "Call Wall ($" + str.tostring(plot_c_wall) + ")\\nGEX: $" + str.tostring(plot_c_gex, "#.##") + "B", style=label.style_label_left, textcolor=color.white, color=color.green)
            label.new(bar_index + 5, plot_p_wall, "Put Wall ($" + str.tostring(plot_p_wall) + ")\\nGEX: $" + str.tostring(plot_p_gex, "#.##") + "B",  style=label.style_label_left, textcolor=color.white, color=color.red)

    // 2. Draw Flip Label (Only if exists)
    if not na(plot_flip)
        label.new(bar_index + 10, plot_flip, "Flip: " + str.tostring(plot_flip, "#.##"), style=label.style_label_left, textcolor=color.white, color=color.blue)

    // 3. Draw Histogram Bars
    if array.size(cur_strikes) > 0
        for i = 0 to array.size(cur_strikes) - 1
            float s = array.get(cur_strikes, i)
            int l   = array.get(cur_lengths, i)
            int sg  = array.get(cur_signs, i)
            
            // Greener/Redder based on sign
            col = sg > 0 ? color.new(color.green, 40) : color.new(color.red, 40)
            
            // Draw the line
            line.new(bar_index, s, bar_index + l, s, color=col, width=2)
"""

with open(output_filename, "w") as f:
    f.write(pine_code)

print(f"✅ Created Universal_GEX_History.pine (Version 6) with GEX values")
