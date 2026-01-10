# ===========================================================
# GEX to Pine Script Converter v3.3 (Histogram Labels Added)
# ===========================================================
# Features:
#  ✅ Smart Rebuild: Caches results to run faster.
#  ✅ Visual Precision: Version 6, Combined Walls, GEX+Delta Labels.
#  ✅ Histogram Data: Now includes text labels for Net GEX on bars.
# ===========================================================

import os
import json
import sys
import pandas as pd
import numpy as np
from datetime import datetime

print("🌲 Starting Historical GEX Converter (v3.3 - Histogram Labels)...")

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
        has_dex = 'call_dex' in df.columns and 'put_dex' in df.columns
        
        required_cols = ['strike', 'call_gex', 'put_gex', 'net_gex']
        if not all(col in df.columns for col in required_cols):
            return None

        # 1. Key Metrics
        flip_zone = compute_flip_zone(df)
        
        # Call Wall
        cw_idx = df['call_gex'].idxmax()
        call_wall = df.loc[cw_idx, 'strike']
        call_wall_gex = df.loc[cw_idx, 'call_gex'] / 1_000_000_000
        call_wall_dex = (df.loc[cw_idx, 'call_dex'] / 1_000_000_000) if has_dex else 0.0

        # Put Wall
        pw_idx = df['put_gex'].abs().idxmax()
        put_wall = df.loc[pw_idx, 'strike']
        put_wall_gex = df.loc[pw_idx, 'put_gex'] / 1_000_000_000
        put_wall_dex = (df.loc[pw_idx, 'put_dex'] / 1_000_000_000) if has_dex else 0.0

        # 2. Histogram Data
        df['abs_net_gex'] = df['net_gex'].abs()
        top_strikes = df.sort_values('abs_net_gex', ascending=False).head(40)
        max_val = top_strikes['abs_net_gex'].max()
        if max_val == 0: max_val = 1
        
        p_strikes = []
        p_lengths = []
        p_signs = []
        p_labels = [] # New list for GEX values
        
        for _, row in top_strikes.iterrows():
            length = int((row['abs_net_gex'] / max_val) * 40)
            if length < 2: length = 2
            
            # Format the label (e.g., "$2.5B")
            gex_val = row['net_gex'] / 1_000_000_000
            label_txt = f"${abs(gex_val):.2f}B"
            
            p_strikes.append(str(float(row['strike'])))
            p_lengths.append(str(length))
            p_signs.append("1" if row['net_gex'] >= 0 else "-1")
            p_labels.append(f"'{label_txt}'") # Wrap in quotes for Pine string array

        return {
            "flip": float(flip_zone) if flip_zone else None,
            "c_wall": float(call_wall),
            "p_wall": float(put_wall),
            "c_gex": float(call_wall_gex),
            "p_gex": float(put_wall_gex),
            "c_dex": float(call_wall_dex),
            "p_dex": float(put_wall_dex),
            "strikes": ', '.join(p_strikes),
            "lengths": ', '.join(p_lengths),
            "signs": ', '.join(p_signs),
            "labels": ', '.join(p_labels)
        }
    except Exception as e:
        print(f"   ⚠️ Error processing {filepath}: {e}")
        return None

# ===============================================
# Smart Rebuild Logic
# ===============================================
cache_file = ".gex_cache.json"
if os.path.exists(cache_file):
    try:
        with open(cache_file, "r") as f: cache = json.load(f)
    except: cache = {}
else:
    cache = {}

files = [f for f in os.listdir('.') if f.endswith('.csv') and "GEX" in f and "summary" not in f]
files.sort()

if not files:
    print("⚠️ No GEX CSV files found.")
    exit()

current_state = {}
for f in files:
    parts = f.split('_')
    if len(parts) < 4: continue
    symbol = parts[0].upper()
    current_state.setdefault(symbol, []).append(f)

for sym in current_state: current_state[sym].sort()

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
# Process Files
# ===============================================
history_map = {} 

for f in files:
    parts = f.split('_')
    if len(parts) < 4: continue
    symbol = parts[0].upper()
    
    date_str = parts[-1].replace('.csv', '')
    if len(date_str) != 8: continue
    
    data = process_file_data(f)
    if data:
        history_map.setdefault(symbol, []).append({
            "year": int(date_str[:4]),
            "month": int(date_str[4:6]),
            "day": int(date_str[6:8]),
            "data": data
        })

# ===============================================
# Write Pine Script (Version 6 + Visuals)
# ===============================================
output_filename = "Universal_GEX_History.pine"

pine_code = f"""//@version=6
indicator("Universal GEX History (Bar Replay)", overlay=true, max_lines_count=500, max_labels_count=500)

// --- Generated {datetime.now().strftime('%Y-%m-%d')} ---
// Smart Rebuild: {'Active' if not force_rebuild else 'Forced'}

var string current_ticker = syminfo.ticker
var float plot_c_wall = na
var float plot_p_wall = na
var float plot_flip   = na
var float plot_c_gex  = na
var float plot_p_gex  = na
var float plot_c_dex  = na
var float plot_p_dex  = na
var float[] cur_strikes = array.new<float>()
var int[]   cur_lengths = array.new<int>()
var int[]   cur_signs   = array.new<int>()
var string[] cur_labels = array.new<string>() // New array for GEX text
"""

for symbol, records in history_map.items():
    last_record = records[-1]
    pine_code += f"""
// ===== {symbol} DATA =====
if current_ticker == "{symbol}"
"""
    for rec in records:
        y, m, d = rec['year'], rec['month'], rec['day']
        d_dat = rec['data']
        flip_val = d_dat['flip'] if d_dat['flip'] is not None else "na"
        
        pine_code += f"""    if year == {y} and month == {m} and dayofmonth == {d}
        plot_c_wall := {d_dat['c_wall']}
        plot_p_wall := {d_dat['p_wall']}
        plot_flip   := {flip_val}
        plot_c_gex  := {d_dat['c_gex']:.4f}
        plot_p_gex  := {d_dat['p_gex']:.4f}
        plot_c_dex  := {d_dat['c_dex']:.4f}
        plot_p_dex  := {d_dat['p_dex']:.4f}
"""

    ld = last_record['data']
    pine_code += f"""
    if barstate.islast
        cur_strikes := array.from({ld['strikes']})
        cur_lengths := array.from({ld['lengths']})
        cur_signs   := array.from({ld['signs']})
        cur_labels  := array.from({ld['labels']})
"""

# PLOTTING LOGIC
pine_code += """
plot(plot_c_wall, "Call Wall", color=color.new(color.green, 20), linewidth=2, style=plot.style_stepline)
plot(plot_p_wall, "Put Wall",  color=color.new(color.red, 20),   linewidth=2, style=plot.style_stepline)
plot(plot_flip,   "Flip Zone", color=color.blue,  linewidth=1, style=plot.style_circles)

if barstate.islast
    // 1. Draw Smart Labels (Fix Overlap)
    if not na(plot_c_wall) and not na(plot_p_wall)
        if plot_c_wall == plot_p_wall
            // COMBINED WALL
            string gex_str = "C: $" + str.tostring(plot_c_gex, "#.##") + "B | Delta: $" + str.tostring(plot_c_dex, "#.##") + "B"
            string pex_str = "P: $" + str.tostring(plot_p_gex, "#.##") + "B | Delta: $" + str.tostring(plot_p_dex, "#.##") + "B"
            label.new(bar_index + 5, plot_c_wall, "COMBINED WALL\\n" + gex_str + "\\n" + pex_str, style=label.style_label_left, textcolor=color.white, color=color.purple)
        else
            // SEPARATE WALLS
            string c_txt = "Call Wall ($" + str.tostring(plot_c_wall) + ")\\nGEX: $" + str.tostring(plot_c_gex, "#.##") + "B\\nDelta: $" + str.tostring(plot_c_dex, "#.##") + "B"
            label.new(bar_index + 5, plot_c_wall, c_txt, style=label.style_label_left, textcolor=color.white, color=color.green)
            
            string p_txt = "Put Wall ($" + str.tostring(plot_p_wall) + ")\\nGEX: $" + str.tostring(plot_p_gex, "#.##") + "B\\nDelta: $" + str.tostring(plot_p_dex, "#.##") + "B"
            label.new(bar_index + 5, plot_p_wall, p_txt, style=label.style_label_left, textcolor=color.white, color=color.red)

    // 2. Draw Flip Label
    if not na(plot_flip)
        label.new(bar_index + 10, plot_flip, "Flip: " + str.tostring(plot_flip, "#.##"), style=label.style_label_left, textcolor=color.white, color=color.blue)

    // 3. Draw Histogram Bars with GEX Labels
    if array.size(cur_strikes) > 0
        for i = 0 to array.size(cur_strikes) - 1
            float s = array.get(cur_strikes, i)
            int l   = array.get(cur_lengths, i)
            int sg  = array.get(cur_signs, i)
            string txt = array.get(cur_labels, i)
            
            col = sg > 0 ? color.new(color.green, 40) : color.new(color.red, 40)
            txt_col = sg > 0 ? color.green : color.red
            
            // Draw Line
            line.new(bar_index, s, bar_index + l, s, color=col, width=2)
            
            // Draw Label at end of line
            label.new(bar_index + l, s, txt, style=label.style_label_left, textcolor=txt_col, color=color.new(color.white, 100), size=size.small)
"""

with open(output_filename, "w") as f:
    f.write(pine_code)

with open(cache_file, "w") as f:
    json.dump(current_state, f, indent=2)

print(f"✅ Created Universal_GEX_History.pine (Version 6) with Histogram Labels")
print("🧠 Smart cache updated.")
