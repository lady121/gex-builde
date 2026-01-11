# ===========================================================
# GEX to Pine Script Converter v3.5 (Function-Based Compression)
# ===========================================================
# Features:
#  ✅ Smart Rebuild System with Force Flag (--force)
#  ✅ DEX (Delta Exposure) Support
#  ✅ Intelligent Net GEX Label Scaling ($K/$M/$B)
#  ✅ Per-Date Replay Support
#  ✅ Function Wrapping for Each Symbol (fixes “main body too long” error)
# ===========================================================

import os
import json
import sys
import pandas as pd
import numpy as np
from datetime import datetime

print("🌲 Starting Historical GEX Converter (v3.5 - Function Wrapped Output)...")

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

        # 1️⃣ Compute metrics
        flip_zone = compute_flip_zone(df)
        cw_idx = df['call_gex'].idxmax()
        call_wall = df.loc[cw_idx, 'strike']
        call_wall_gex = df.loc[cw_idx, 'call_gex'] / 1_000_000_000
        call_wall_dex = (df.loc[cw_idx, 'call_dex'] / 1_000_000_000) if has_dex else 0.0

        pw_idx = df['put_gex'].abs().idxmax()
        put_wall = df.loc[pw_idx, 'strike']
        put_wall_gex = df.loc[pw_idx, 'put_gex'] / 1_000_000_000
        put_wall_dex = (df.loc[pw_idx, 'put_dex'] / 1_000_000_000) if has_dex else 0.0

        # 2️⃣ Build histogram arrays with dynamic label formatting
        df['abs_net_gex'] = df['net_gex'].abs()
        top_strikes = df.sort_values('abs_net_gex', ascending=False).head(40)
        max_val = top_strikes['abs_net_gex'].max()
        if max_val == 0:
            max_val = 1

        p_strikes, p_lengths, p_signs, p_labels = [], [], [], []
        for _, row in top_strikes.iterrows():
            length = int((row['abs_net_gex'] / max_val) * 40)
            if length < 2:
                length = 2

            raw_val = float(row['net_gex'])
            abs_val = abs(raw_val)

            # Intelligent number scaling
            if abs_val >= 1_000_000_000:
                label_txt = f"${raw_val / 1_000_000_000:.2f}B"
            elif abs_val >= 1_000_000:
                label_txt = f"${raw_val / 1_000_000:.2f}M"
            elif abs_val >= 1_000:
                label_txt = f"${raw_val / 1_000:.0f}K"
            else:
                label_txt = f"${raw_val:.0f}"

            p_strikes.append(str(float(row['strike'])))
            p_lengths.append(str(length))
            p_signs.append("1" if raw_val >= 0 else "-1")
            p_labels.append(f"'{label_txt}'")

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
        history_map.setdefault(symbol, []).append({
            "year": int(date_str[:4]),
            "month": int(date_str[4:6]),
            "day": int(date_str[6:8]),
            "data": data
        })

# ===============================================
# Generate PineScript Output (Function Wrapped)
# ===============================================
output_filename = "Universal_GEX_History.pine"

pine_code = f"""//@version=6
indicator("Universal GEX History (Bar Replay)", overlay=true, max_lines_count=500, max_labels_count=500)

// --- Generated {datetime.now().strftime('%Y-%m-%d')} ---
// Smart Rebuild: {'Forced' if force_rebuild else 'Active'}

var string current_ticker = syminfo.ticker
var float plot_c_wall = na
var float plot_p_wall = na
var float plot_flip   = na
var float plot_c_gex  = na
var float plot_p_gex  = na
var float plot_c_dex  = na
var float plot_p_dex  = na
var float[] cur_strikes = array.new_float()
var int[]   cur_lengths = array.new_int()
var int[]   cur_signs   = array.new_int()
var string[] cur_labels = array.new_string()
"""

# --- Function Blocks ---
for symbol, records in history_map.items():
    func_name = f"f_symbol_{symbol}"
    pine_code += f"""
// ===== {symbol} FUNCTION =====
{func_name}() =>
    var float plot_c_wall = na
    var float plot_p_wall = na
    var float plot_flip   = na
    var float[] cur_strikes = array.new_float()
    var int[]   cur_lengths = array.new_int()
    var int[]   cur_signs   = array.new_int()
    var string[] cur_labels = array.new_string()
"""
    for rec in records:
        y, m, d = rec['year'], rec['month'], rec['day']
        d_dat = rec['data']
        flip_val = d_dat['flip'] if d_dat['flip'] is not None else "na"
        pine_code += f"""    if year == {y} and month == {m} and dayofmonth == {d}
        plot_c_wall := {d_dat['c_wall']}
        plot_p_wall := {d_dat['p_wall']}
        plot_flip   := {flip_val}
        cur_strikes := array.from({d_dat['strikes']})
        cur_lengths := array.from({d_dat['lengths']})
        cur_signs   := array.from({d_dat['signs']})
        cur_labels  := array.from({d_dat['labels']})
"""
    pine_code += f"    [plot_c_wall, plot_p_wall, plot_flip, cur_strikes, cur_lengths, cur_signs, cur_labels]\n"

# --- Main Calls ---
pine_code += "\n// ===== MAIN CALLS =====\n"
for symbol in history_map.keys():
    func_name = f"f_symbol_{symbol}"
    pine_code += f"""if current_ticker == "{symbol}"
    [plot_c_wall, plot_p_wall, plot_flip, cur_strikes, cur_lengths, cur_signs, cur_labels] := {func_name}()
"""

# --- Plot Logic ---
pine_code += """
plot(plot_c_wall, "Call Wall", color=color.new(color.green, 20), linewidth=2, style=plot.style_stepline)
plot(plot_p_wall, "Put Wall",  color=color.new(color.red, 20),   linewidth=2, style=plot.style_stepline)
plot(plot_flip,   "Flip Zone", color=color.blue, linewidth=1, style=plot.style_circles)

if array.size(cur_strikes) > 0
    for i = 0 to array.size(cur_strikes) - 1
        float s = array.get(cur_strikes, i)
        int l   = array.get(cur_lengths, i)
        int sg  = array.get(cur_signs, i)
        string txt = array.get(cur_labels, i)

        col = sg > 0 ? color.new(color.green, 40) : color.new(color.red, 40)
        txt_col = sg > 0 ? color.green : color.red

        line.new(bar_index, s, bar_index + l, s, color=col, width=2)
        label.new(bar_index + l, s, txt, style=label.style_label_left, textcolor=txt_col, color=color.new(color.white, 100), size=size.small)
"""

# ===============================================
# Write Files
# ===============================================
with open(output_filename, "w") as f:
    f.write(pine_code)

with open(cache_file, "w") as f:
    json.dump(current_state, f, indent=2)

print(f"✅ Created {output_filename} (v3.5 - Function Wrapped)")
print("🧠 Smart cache updated.")
