# ===========================================================
# GEX to Pine Script Converter (Historical / Bar Replay Edition)
# ===========================================================
# Usage:
# 1. Scans folder for ALL historical GEX CSV files.
# 2. Generates "Universal_GEX_History.pine".
# 3. Enables BAR REPLAY by plotting historical Walls & Flip Zones.
# 4. Shows the Full Histogram ONLY for the *current* live bar.
# ===========================================================

import os
import pandas as pd
import numpy as np
from datetime import datetime

print("🌲 Starting Historical GEX Converter...")

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

        # 1. Key Metrics (For History Lines)
        flip_zone = compute_flip_zone(df)
        call_wall = df.loc[df['call_gex'].idxmax(), 'strike']
        put_wall = df.loc[df['put_gex'].idxmax(), 'strike']

        # 2. Histogram Data (For Current Day Only - Compressed)
        # We only save this full data for the *latest* file to save Pine Script size
        df['abs_net_gex'] = df['net_gex'].abs()
        top_strikes = df.sort_values('abs_net_gex', ascending=False).head(40)
        
        max_val = top_strikes['abs_net_gex'].max()
        if max_val == 0: max_val = 1
        
        p_strikes = []
        p_lengths = []
        p_signs = []
        
        for _, row in top_strikes.iterrows():
            length = int((row['abs_net_gex'] / max_val) * 25) # Max 25 bars
            if length < 2: length = 2
            
            p_strikes.append(str(float(row['strike'])))
            p_lengths.append(str(length))
            p_signs.append("1" if row['net_gex'] >= 0 else "-1")

        return {
            "flip": float(flip_zone) if flip_zone else 0.0,
            "c_wall": float(call_wall),
            "p_wall": float(put_wall),
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
files = [f for f in os.listdir('.') if f.endswith('.csv') and "GEX" in f]
files.sort() # Sort by date usually works if naming is YYYYMMDD

if not files:
    print("⚠️ No GEX CSV files found.")
    exit()

print(f"📂 Found {len(files)} CSV files. Building History...")

# Structure: history_map[symbol] = [ {date: '20251224', data: {...}}, ... ]
history_map = {} 

for f in files:
    parts = f.split('_')
    if len(parts) < 4: continue # Skip malformed filenames
    
    symbol = parts[0].upper()
    date_str = parts[-1].replace('.csv', '') # e.g., 20251224
    
    # Check date format
    if len(date_str) != 8: continue
    
    data = process_file_data(f)
    if data:
        if symbol not in history_map:
            history_map[symbol] = []
        
        history_map[symbol].append({
            "year": int(date_str[:4]),
            "month": int(date_str[4:6]),
            "day": int(date_str[6:8]),
            "data": data
        })

# ===============================================
# Write Pine Script
# ===============================================
output_filename = f"Universal_GEX_History_{datetime.now().strftime('%Y%m%d')}.pine"

pine_code = f"""//@version=5
indicator("Universal GEX History (Bar Replay)", overlay=true, max_lines_count=500, max_labels_count=500)

// --- Universal GEX History ---
// Generated: {datetime.now().strftime('%Y-%m-%d')}
// Features: 
// 1. Historical Lines for Walls/Flip (Works with Bar Replay)
// 2. Full Histogram for the LAST BAR only.

var string current_ticker = syminfo.ticker

// --- Plot Variables ---
var float plot_c_wall = na
var float plot_p_wall = na
var float plot_flip   = na

// --- Arrays for Current Day Histogram ---
var float[] cur_strikes = array.new_float()
var int[]   cur_lengths = array.new_int()
var int[]   cur_signs   = array.new_int()
"""

# ---------------------------------------------------------
# INJECT DATA: Ticker by Ticker
# ---------------------------------------------------------
for symbol, records in history_map.items():
    # Only keep the last record for the histogram (Profile)
    last_record = records[-1]
    
    pine_code += f"""
// ===== {symbol} DATA =====
if current_ticker == "{symbol}"
"""
    # 1. Historical Data Injection (Series of If statements is most efficient for Pine Limits)
    # We check the bar's date to assign the correct historical levels
    for rec in records:
        y, m, d = rec['year'], rec['month'], rec['day']
        d_dat = rec['data']
        
        # Logic: If current bar is on or after this date, update the "Wall" variables.
        # This creates a "Step" line effect.
        pine_code += f"""    if year == {y} and month == {m} and dayofmonth == {d}
        plot_c_wall := {d_dat['c_wall']}
        plot_p_wall := {d_dat['p_wall']}
        plot_flip   := {d_dat['flip'] > 0 and d_dat['flip'] or 'na'}
"""

    # 2. Current Day Histogram Data (Only load if it's the very last bar to save memory)
    # Note: We use the MOST RECENT file for the histogram
    ld = last_record['data']
    pine_code += f"""
    if barstate.islast
        cur_strikes := array.from({ld['strikes']})
        cur_lengths := array.from({ld['lengths']})
        cur_signs   := array.from({ld['signs']})
"""

pine_code += """
// --- Plotting History (Lines) ---
plot(plot_c_wall, "Call Wall", color=color.green, linewidth=2, style=plot.style_stepline)
plot(plot_p_wall, "Put Wall",  color=color.red,   linewidth=2, style=plot.style_stepline)
plot(plot_flip,   "Flip Zone", color=color.blue,  linewidth=1, style=plot.style_circles)

// --- Plotting Profile (Histogram - Last Bar Only) ---
if barstate.islast and array.size(cur_strikes) > 0
    // Draw Flip Label
    if not na(plot_flip)
        label.new(bar_index + 10, plot_flip, "Flip: " + str.tostring(plot_flip, "#.##"), style=label.style_label_left, textcolor=color.white, color=color.blue)
    
    // Draw Wall Labels
    label.new(bar_index + 5, plot_c_wall, "Call Wall", style=label.style_label_left, textcolor=color.white, color=color.green)
    label.new(bar_index + 5, plot_p_wall, "Put Wall",  style=label.style_label_left, textcolor=color.white, color=color.red)

    // Draw Histogram Bars
    for i = 0 to array.size(cur_strikes) - 1
        float s = array.get(cur_strikes, i)
        int l   = array.get(cur_lengths, i)
        int sg  = array.get(cur_signs, i)
        
        col = sg > 0 ? color.new(color.green, 50) : color.new(color.red, 50)
        line.new(bar_index, s, bar_index + l, s, color=col, width=2)
"""

with open(output_filename, "w") as f:
    f.write(pine_code)

print(f"✅ Created Historical Script: {output_filename}")
