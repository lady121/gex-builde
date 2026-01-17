# ===========================================================
# GEX to Pine Script Converter v6.1 (Clean Box Labels)
# ===========================================================
# Features:
#   ✅ STRUCTURE: Classic "If Ticker / If Date" logic
#   ✅ VISUALS: STRICT Box Labels for all data points
#   ✅ CLEANUP: Consolidates GEX/Delta into single labels per strike
#   ✅ DATA: Includes GEX and Delta (DEX) with K/M/B formatting
# ===========================================================

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

print("🌲 Starting Historical GEX Converter (v6.1 - Clean Box Labels)...")

# ===============================================
# Configuration
# ===============================================
# Limit history to prevent Pine Script size errors
MAX_DAYS_PER_TICKER = 150 

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

def format_value(val):
    """Formats large numbers into K, M, B strings."""
    abs_val = abs(val)
    if abs_val >= 1_000_000_000:
        return f"${val/1_000_000_000:.2f}B"
    elif abs_val >= 1_000_000:
        return f"${val/1_000_000:.1f}M"
    elif abs_val >= 1_000:
        return f"${val/1_000:.0f}K"
    else:
        return f"${val:.0f}"

def process_file_data(filepath):
    try:
        df = pd.read_csv(filepath)
        has_dex = 'call_dex' in df.columns and 'put_dex' in df.columns
        
        required_cols = ['strike', 'call_gex', 'put_gex', 'net_gex']
        if not all(col in df.columns for col in required_cols):
            return None

        # 1️⃣ Compute metrics
        flip_zone = compute_flip_zone(df)
        
        # Walls
        cw_idx = df['call_gex'].idxmax()
        call_wall = df.loc[cw_idx, 'strike']
        cw_gex = df.loc[cw_idx, 'call_gex']
        cw_dex = df.loc[cw_idx, 'call_dex'] if has_dex else 0.0

        pw_idx = df['put_gex'].abs().idxmax()
        put_wall = df.loc[pw_idx, 'strike']
        pw_gex = df.loc[pw_idx, 'put_gex']
        pw_dex = df.loc[pw_idx, 'put_dex'] if has_dex else 0.0

        # 2️⃣ Prepare Histogram Data (Top 40)
        df['abs_net_gex'] = df['net_gex'].abs()
        top_strikes = df.sort_values('abs_net_gex', ascending=False).head(40)
        max_val = top_strikes['abs_net_gex'].max()
        if max_val == 0: max_val = 1.0

        p_strikes = []
        p_lengths = []
        p_signs = []
        p_labels = [] 
        
        for _, row in top_strikes.iterrows():
            length = int((row['abs_net_gex'] / max_val) * 40)
            if length < 2: length = 2
            
            # Create a clean, consolidated label for this strike
            gex_str = format_value(row['net_gex'])
            label_text = f"{gex_str}"
            
            # Add Delta if significant and available
            if has_dex:
                # We approximate Net DEX for the strike (Call Dex - Put Dex)
                net_dex = row.get('call_dex', 0) - row.get('put_dex', 0)
                if abs(net_dex) > 1000: # Only show if not trivial
                    dex_str = format_value(net_dex)
                    label_text += f" | D: {dex_str}"

            p_strikes.append(str(float(row['strike'])))
            p_lengths.append(str(length))
            p_signs.append("1" if row['net_gex'] >= 0 else "-1")
            p_labels.append(f"'{label_text}'")

        return {
            "flip": float(flip_zone) if flip_zone else None,
            "c_wall": float(call_wall),
            "p_wall": float(put_wall),
            "c_gex": float(cw_gex),
            "p_gex": float(pw_gex),
            "c_dex": float(cw_dex),
            "p_dex": float(pw_dex),
            "strikes": ", ".join(p_strikes),
            "lengths": ", ".join(p_lengths),
            "signs": ", ".join(p_signs),
            "labels": ", ".join(p_labels)
        }

    except Exception as e:
        print(f"   ⚠️ Error processing {filepath}: {e}")
        return None

# ===============================================
# Main Logic
# ===============================================
files = [f for f in os.listdir('.') if f.endswith('.csv') and "GEX" in f]
files.sort()

if not files:
    print("⚠️ No GEX CSV files found.")
    exit()

# Group by Ticker
history_map = {}
print("⏳ Processing CSV files...")

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

# Truncate
for sym in history_map:
    history_map[sym].sort(key=lambda x: (x['year'], x['month'], x['day']))
    if len(history_map[sym]) > MAX_DAYS_PER_TICKER:
        history_map[sym] = history_map[sym][-MAX_DAYS_PER_TICKER:]

# ===============================================
# Generate PineScript Output
# ===============================================
output_filename = "Universal_GEX_History.pine"

pine_code = f"""//@version=6
indicator("Universal GEX History (Bar Replay)", overlay=true, max_lines_count=500, max_labels_count=500)

// --- Generated {datetime.now().strftime('%Y-%m-%d')} ---
// Mode: V6.1 (Clean Box Labels)

// --- Settings ---
sz_label = input.string("normal", "Label Size", options=["auto", "tiny", "small", "normal", "large", "huge"], group="Visuals")

var string current_ticker = syminfo.ticker

// --- Plot Variables ---
var float plot_c_wall = na
var float plot_p_wall = na
var float plot_flip   = na
var float plot_c_gex  = na
var float plot_p_gex  = na
var float plot_c_dex  = na
var float plot_p_dex  = na

// --- Arrays for Current Day Histogram ---
var float[] cur_strikes = array.new<float>()
var int[]   cur_lengths = array.new<int>()
var int[]   cur_signs   = array.new<int>()
var string[] cur_labels = array.new<string>()

// --- Helper: Format numbers (K, M, B) ---
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
"""

# --- INJECT DATA ---
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
        plot_c_gex  := {d_dat['c_gex']}
        plot_p_gex  := {d_dat['p_gex']}
        plot_c_dex  := {d_dat['c_dex']}
        plot_p_dex  := {d_dat['p_dex']}
"""

    ld = last_record['data']
    pine_code += f"""
    if barstate.islast
        cur_strikes := array.from({ld['strikes']})
        cur_lengths := array.from({ld['lengths']})
        cur_signs   := array.from({ld['signs']})
        cur_labels  := array.from({ld['labels']})
"""

# --- Plotting Logic ---
pine_code += """
// --- Plotting History (Lines) ---
plot(plot_c_wall, "Call Wall", color=color.new(color.green, 20), linewidth=2, style=plot.style_stepline)
plot(plot_p_wall, "Put Wall",  color=color.new(color.red, 20),   linewidth=2, style=plot.style_stepline)
plot(plot_flip,   "Flip Zone", color=color.new(color.purple, 0), linewidth=1, style=plot.style_circles)

// --- Visual Logic (Last Bar Only) ---
if barstate.islast
    // 1. WALL LABELS (Box Style)
    
    // Check Overlap
    bool overlap = not na(plot_c_wall) and not na(plot_p_wall) and plot_c_wall == plot_p_wall
    
    if overlap
        // Combined Label (Purple Box)
        string txt = "COMBINED WALL ($" + str.tostring(plot_c_wall) + ")\\n" +
                     "C_GEX: " + f_fmt(plot_c_gex) + " | D: " + f_fmt(plot_c_dex) + "\\n" +
                     "P_GEX: " + f_fmt(plot_p_gex) + " | D: " + f_fmt(plot_p_dex)
                     
        label.new(bar_index + 8, plot_c_wall, txt, style=label.style_label_left, textcolor=color.white, color=color.purple, size=sz_label)
    
    else
        // Call Wall (Green Box)
        if not na(plot_c_wall)
            string c_txt = "CW ($" + str.tostring(plot_c_wall) + ")\\nGEX: " + f_fmt(plot_c_gex) + "\\nDEX: " + f_fmt(plot_c_dex)
            label.new(bar_index + 5, plot_c_wall, c_txt, style=label.style_label_left, textcolor=color.white, color=color.green, size=sz_label)
            
        // Put Wall (Red Box)
        if not na(plot_p_wall)
            string p_txt = "PW ($" + str.tostring(plot_p_wall) + ")\\nGEX: " + f_fmt(plot_p_gex) + "\\nDEX: " + f_fmt(plot_p_dex)
            label.new(bar_index + 12, plot_p_wall, p_txt, style=label.style_label_left, textcolor=color.white, color=color.red, size=sz_label)

    // Flip Zone Label
    if not na(plot_flip)
        label.new(bar_index + 20, plot_flip, "Flip: " + str.tostring(plot_flip, "#.##"), style=label.style_label_left, textcolor=color.white, color=color.blue, size=sz_label)

    // 2. HISTOGRAM LABELS (Consolidated Boxes)
    if array.size(cur_strikes) > 0
        for i = 0 to array.size(cur_strikes) - 1
            float s = array.get(cur_strikes, i)
            int l   = array.get(cur_lengths, i)
            int sg  = array.get(cur_signs, i)
            string t = array.get(cur_labels, i)
            
            col = sg > 0 ? color.new(color.green, 40) : color.new(color.red, 40)
            lbl_col = sg > 0 ? color.green : color.red
            
            // Draw Line
            line.new(bar_index, s, bar_index + l, s, color=col, width=2)
            
            // Draw Box Label for Histogram Data
            label.new(bar_index + l, s, t, style=label.style_label_left, textcolor=color.white, color=lbl_col, size=size.tiny)
"""

# ===============================================
# Write Files
# ===============================================
with open(output_filename, "w") as f:
    f.write(pine_code)

print(f"✅ Created {output_filename} (v6.1 - Clean Box Labels)")
