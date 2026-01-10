# ===========================================================
# GEX to Pine Script Converter (Historical / Bar Replay Edition)
# with Smart Rebuild System 🧠
# ===========================================================

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime

print("🌲 Starting Historical GEX Converter (Smart Mode)...")

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

        flip_zone = compute_flip_zone(df)
        call_wall = df.loc[df['call_gex'].idxmax(), 'strike']
        put_wall = df.loc[df['put_gex'].idxmax(), 'strike']

        df['abs_net_gex'] = df['net_gex'].abs()
        top_strikes = df.sort_values('abs_net_gex', ascending=False).head(40)

        max_val = top_strikes['abs_net_gex'].max()
        if max_val == 0: max_val = 1

        p_strikes, p_lengths, p_signs = [], [], []

        for _, row in top_strikes.iterrows():
            length = int((row['abs_net_gex'] / max_val) * 25)
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
# Smart Rebuild System
# ===============================================
cache_file = ".gex_cache.json"
if os.path.exists(cache_file):
    with open(cache_file, "r") as f:
        cache = json.load(f)
else:
    cache = {}

files = [f for f in os.listdir('.') if f.endswith('.csv') and "GEX" in f]
files.sort()

if not files:
    print("⚠️ No GEX CSV files found.")
    exit()

# Build current state of CSVs per symbol
current_state = {}
for f in files:
    parts = f.split('_')
    if len(parts) < 4:
        continue
    symbol = parts[0].upper()
    current_state.setdefault(symbol, []).append(f)

for sym in current_state:
    current_state[sym].sort()

# Detect changes
changed_symbols = []
for sym, file_list in current_state.items():
    if sym not in cache or cache[sym] != file_list:
        changed_symbols.append(sym)
        print(f"🔁 Change detected for {sym}: {len(file_list)} files")

if not changed_symbols:
    print("✅ No new data detected. Skipping rebuild.")
    exit()

print(f"📂 Found {len(files)} CSV files across {len(current_state)} symbols. Building History...")

# ===============================================
# Build Data Only for Changed Symbols
# ===============================================
history_map = {}

for f in files:
    parts = f.split('_')
    if len(parts) < 4: 
        continue

    symbol = parts[0].upper()
    if symbol not in changed_symbols:
        continue  # skip unchanged symbols

    date_str = parts[-1].replace('.csv', '')
    if len(date_str) != 8: 
        continue

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

// --- Generated {datetime.now().strftime('%Y-%m-%d')} ---
// Smart rebuild enabled. Only changed symbols reprocessed.

var string current_ticker = syminfo.ticker
var float plot_c_wall = na
var float plot_p_wall = na
var float plot_flip   = na
var float[] cur_strikes = array.new_float()
var int[]   cur_lengths = array.new_int()
var int[]   cur_signs   = array.new_int()
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
        pine_code += f"""    if year == {y} and month == {m} and dayofmonth == {d}
        plot_c_wall := {d_dat['c_wall']}
        plot_p_wall := {d_dat['p_wall']}
        plot_flip   := {d_dat['flip'] > 0 and d_dat['flip'] or 'na'}
"""

    ld = last_record['data']
    pine_code += f"""
    if barstate.islast
        cur_strikes := array.from({ld['strikes']})
        cur_lengths := array.from({ld['lengths']})
        cur_signs   := array.from({ld['signs']})
"""

pine_code += """
plot(plot_c_wall, "Call Wall", color=color.green, linewidth=2, style=plot.style_stepline)
plot(plot_p_wall, "Put Wall",  color=color.red,   linewidth=2, style=plot.style_stepline)
plot(plot_flip,   "Flip Zone", color=color.blue,  linewidth=1, style=plot.style_circles)

if barstate.islast and array.size(cur_strikes) > 0
    if not na(plot_flip)
        label.new(bar_index + 10, plot_flip, "Flip: " + str.tostring(plot_flip, "#.##"), style=label.style_label_left, textcolor=color.white, color=color.blue)
    label.new(bar_index + 5, plot_c_wall, "Call Wall", style=label.style_label_left, textcolor=color.white, color=color.green)
    label.new(bar_index + 5, plot_p_wall, "Put Wall",  style=label.style_label_left, textcolor=color.white, color=color.red)
    for i = 0 to array.size(cur_strikes) - 1
        float s = array.get(cur_strikes, i)
        int l   = array.get(cur_lengths, i)
        int sg  = array.get(cur_signs, i)
        col = sg > 0 ? color.new(color.green, 50) : color.new(color.red, 50)
        line.new(bar_index, s, bar_index + l, s, color=col, width=2)
"""

with open(output_filename, "w") as f:
    f.write(pine_code)

# Save cache for next run
with open(cache_file, "w") as f:
    json.dump(current_state, f, indent=2)

print(f"✅ Created Historical Script: {output_filename}")
print("🧠 Smart cache updated — next run will skip unchanged symbols.")
