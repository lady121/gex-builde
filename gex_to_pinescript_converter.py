# ===========================================================
# GEX to Pine Script Converter v4.2 (Ultra-Compression)
# ===========================================================
# Features:
#   ✅ ULTRA-COMPRESSION: Groups data by month (1 line per month instead of 20)
#   ✅ ADAPTIVE HISTORY: Automatically calculates safe history depth based on symbol count
#   ✅ VISUALS: Keeps v4.1's clean labels, right-aligned text, and flip crosses
#   ✅ PERFORMANCE: Massive token reduction allowing for more symbols/history
# ===========================================================

import os
import json
import sys
import pandas as pd
import numpy as np
from datetime import datetime

print("🌲 Starting Historical GEX Converter (v4.2 - Ultra-Compressed)...")

# ===============================================
# Configuration
# ===============================================
# We dynamically adjust this later based on symbol count
BASE_HISTORY_DAYS = 300 

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
        df['abs_net_gex'] = df['net_gex'].abs()
        top_strikes = df.sort_values('abs_net_gex', ascending=False).head(40)
        max_val = top_strikes['abs_net_gex'].max()
        if max_val == 0: max_val = 1.0

        # Build Inner Data String: "strike:net_gex;strike:net_gex"
        data_pairs = []
        for _, row in top_strikes.iterrows():
            s = float(row['strike'])
            g = float(row['net_gex'])
            # Rounding saves string space
            data_pairs.append(f"{s:.2f}:{g:.0f}")
        
        inner_data = ";".join(data_pairs)

        # 3️⃣ Format Day Blob for Monthly String
        # Format: CW~PW~Flip~Max~Data
        # We use '~' as delimiter
        flip_val = f"{float(flip_zone):.2f}" if flip_zone else "n"
        day_blob = f"{float(call_wall):.2f}~{float(put_wall):.2f}~{flip_val}~{float(max_val):.0f}~{inner_data}"
        
        return day_blob

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
# Adaptive Limits
# ===============================================
# Simple heuristic: The more symbols, the less history per symbol to save tokens.
num_symbols = len(current_state)
# Base logic: 2000 total "monthly blocks" allowed roughly across all symbols
# but since months compress well, we can be generous.
# Let's target ~1.5 years (approx 400 trading days) if 1 symbol, scaling down.
max_history_days = int(max(100, 5000 / max(1, num_symbols))) 
print(f"⚖️  Adaptive Logic: Found {num_symbols} symbols. Limiting to {max_history_days} days history per symbol.")

# ===============================================
# Process Data (Monthly Grouping)
# ===============================================
# Structure: history_map[symbol][year_month_int] = "day=blob|day=blob..."
history_map = {}

print("⏳ Processing CSV files (Ultra-Compression Mode)...")
for f in files:
    parts = f.split('_')
    if len(parts) < 4: continue
    symbol = parts[0].upper()
    date_str = parts[-1].replace('.csv', '')
    if len(date_str) != 8: continue
    
    data_blob = process_file_data(f)
    if data_blob:
        d_int = int(date_str)
        ym_int = int(date_str[:6]) # 202501
        day_part = int(date_str[6:8]) # 02
        
        # Structure: history_map[symbol] = list of {date_int, blob} first
        history_map.setdefault(symbol, []).append({
            "date_int": d_int,
            "ym_int": ym_int,
            "day": day_part,
            "blob": data_blob
        })

# Sort, Truncate, and Compress into Months
final_map = {}

for sym, records in history_map.items():
    records.sort(key=lambda x: x['date_int'])
    
    # Truncate
    if len(records) > max_history_days:
        records = records[-max_history_days:]
    
    # Group by Month
    monthly_data = {}
    for rec in records:
        ym = rec['ym_int']
        d = rec['day']
        blob = rec['blob']
        # Format: "Day=Blob"
        entry = f"{d}={blob}"
        monthly_data.setdefault(ym, []).append(entry)
    
    # Join months
    final_map[sym] = {}
    for ym, entries in monthly_data.items():
        # Pipe separated days
        final_map[sym][ym] = "|".join(entries)

# ===============================================
# Generate PineScript Output (Ultra-Compressed)
# ===============================================
output_filename = "Universal_GEX_History.pine"

pine_code = f"""//@version=6
indicator("Universal GEX History (Bar Replay)", overlay=true, max_lines_count=500, max_labels_count=500)

// --- Generated {datetime.now().strftime('%Y-%m-%d')} ---
// Mode: V4.2 (Ultra-Compressed Monthly Blobs)

// --- Settings ---
show_labels = input.bool(true, "Show Histogram Values", group="Visuals")
line_width  = input.int(1, "GEX Line Width", minval=1, maxval=4, group="Visuals")

var string current_ticker = syminfo.ticker

// --- Helper: Format numbers ---
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

// --- Helper: Parse Monthly Blob for Specific Day ---
// Blob format: "1=CW~PW~Flip~Max~Data|2=..."
f_parse_day_data(string m_blob, int d_target) =>
    float cw = na, float pw = na, float fl = na, float mv = na, string d_str = ""
    
    if m_blob != ""
        // Split month into days
        string[] days = str.split(m_blob, "|")
        int cnt = array.size(days)
        
        // Find today
        string target_prefix = str.tostring(d_target) + "="
        
        // We iterate to find the day (max 22 days per month, fast enough)
        for i = 0 to cnt - 1
            string day_entry = array.get(days, i)
            if str.startswith(day_entry, target_prefix)
                // Remove "Day=" prefix
                string content = str.replace(day_entry, target_prefix, "")
                
                // Split components: CW~PW~Flip~Max~Data
                string[] comps = str.split(content, "~")
                if array.size(comps) >= 5
                    cw := str.tonumber(array.get(comps, 0))
                    pw := str.tonumber(array.get(comps, 1))
                    string fl_str = array.get(comps, 2)
                    fl := fl_str == "n" ? na : str.tonumber(fl_str)
                    mv := str.tonumber(array.get(comps, 3))
                    d_str := array.get(comps, 4)
                break
    [cw, pw, fl, mv, d_str]

// --- Helper: Draw Histogram ---
f_draw_gex(string d_str, float max_v) =>
    if d_str != ""
        string[] pairs = str.split(d_str, ";")
        if array.size(pairs) > 0
            for i = 0 to array.size(pairs) - 1
                string p = array.get(pairs, i)
                string[] parts = str.split(p, ":")
                if array.size(parts) == 2
                    float strike = str.tonumber(array.get(parts, 0))
                    float net_gex = str.tonumber(array.get(parts, 1))
                    
                    int length = int((math.abs(net_gex) / max_v) * 25) 
                    if length < 2 
                        length := 2
                    
                    color c = net_gex >= 0 ? color.new(color.green, 40) : color.new(color.red, 40)
                    
                    line.new(bar_index, strike, bar_index + length, strike, color=c, width=line_width)
                    
                    if show_labels and math.abs(net_gex) > (max_v * 0.25)
                        color tc = net_gex >= 0 ? color.green : color.red
                        string txt = f_fmt(net_gex)
                        label.new(bar_index + length, strike, txt, style=label.style_label_left, textcolor=tc, color=color.new(color.white, 100), size=size.tiny)

"""

# --- Symbol Functions (Monthly Switching) ---
for symbol, month_map in final_map.items():
    func_name = f"f_symbol_{symbol}"
    pine_code += f"""
// {symbol} - Monthly Lookup
{func_name}(int ym) =>
    string b = switch ym
"""
    # Sort months for cleaner code
    sorted_months = sorted(month_map.keys())
    for ym in sorted_months:
        blob = month_map[ym]
        pine_code += f'        {ym} => "{blob}"\n'
    
    pine_code += f'        => ""\n'

# --- Main Logic ---
pine_code += """
// ===== MAIN EXECUTION =====
int ym = year * 100 + month
int d  = dayofmonth

// 1. Get Monthly Blob
string month_blob = switch current_ticker
"""

for symbol in final_map.keys():
    func_name = f"f_symbol_{symbol}"
    pine_code += f'    "{symbol}" => {func_name}(ym)\n'

pine_code += '    => ""\n'

pine_code += """
// 2. Parse Daily Data from Blob
[plot_c_wall, plot_p_wall, plot_flip, max_val, data_str] = f_parse_day_data(month_blob, d)

// 3. Draw Lines & Labels
if not na(plot_c_wall)
    // Draw Lines
    plot(plot_c_wall, "Call Wall", color=color.new(color.green, 20), linewidth=2, style=plot.style_stepline)
    plot(plot_p_wall, "Put Wall",  color=color.new(color.red, 20),   linewidth=2, style=plot.style_stepline)
    plot(plot_flip,   "Flip Zone", color=color.new(color.purple, 0), linewidth=2, style=plot.style_cross)

    // Draw Labels (Right Aligned)
    var label l_cw = label.new(na, na, "CW", style=label.style_label_left, textcolor=color.green, color=color.new(color.white, 100), size=size.small)
    var label l_pw = label.new(na, na, "PW", style=label.style_label_left, textcolor=color.red, color=color.new(color.white, 100), size=size.small)
    var label l_fp = label.new(na, na, "Flip", style=label.style_label_left, textcolor=color.purple, color=color.new(color.white, 100), size=size.small)

    label.set_xy(l_cw, bar_index + 3, plot_c_wall)
    label.set_text(l_cw, "CW: " + str.tostring(plot_c_wall))

    label.set_xy(l_pw, bar_index + 3, plot_p_wall)
    label.set_text(l_pw, "PW: " + str.tostring(plot_p_wall))

    if not na(plot_flip)
        label.set_xy(l_fp, bar_index + 3, plot_flip)
        label.set_text(l_fp, "Flip: " + str.tostring(plot_flip))
    else
        label.set_xy(l_fp, na, na)

    // 4. Draw Profile
    f_draw_gex(data_str, max_val)
"""

# ===============================================
# Write Files
# ===============================================
with open(output_filename, "w") as f:
    f.write(pine_code)

with open(cache_file, "w") as f:
    json.dump(current_state, f, indent=2)

print(f"✅ Created {output_filename} (v4.2 - Ultra-Compressed)")
print(f"📊 Processed {len(final_map)} symbols with adaptive history.")
