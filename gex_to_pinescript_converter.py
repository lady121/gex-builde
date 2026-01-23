# ===========================================================
# GEX to Pine Script Converter v7.3 (Walls with GEX+Delta)
# ===========================================================
# FEATURES:
#   ✅ VISUALS: Call/Put Walls now show GEX & Delta (e.g., "$5B | Δ0.50")
#   ✅ VISUALS: Flip Zone separated (Blue label)
#   ✅ DATA: Efficient string packing for Wall stats
#   ✅ FIX: Maintained type safety and structure
# ===========================================================

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

print("🌲 Starting GEX Converter v7.3 (Walls with GEX/Delta)...")

# ===============================================
# Configuration
# ===============================================
MAX_DAYS_PER_TICKER = 250
MIN_PCT_THRESHOLD = 0.25  # Only show levels > 25% of the day's max GEX

# ===============================================
# Command-line flags
# ===============================================
force_rebuild = "--force" in sys.argv
if force_rebuild:
    print("⚙️ Force rebuild enabled — ignoring cache.")

# ===============================================
# Helper Functions
# ===============================================
def format_value(val):
    """Compact K/M/B formatting for strings."""
    abs_val = abs(val)
    if abs_val >= 1_000_000_000:
        return f"${val/1_000_000_000:.1f}B"
    elif abs_val >= 1_000_000:
        return f"${val/1_000_000:.0f}M"
    elif abs_val >= 1_000:
        return f"${val/1_000:.0f}K"
    else:
        return f"${val:.0f}"

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

def get_delta_str(row, has_greek, has_dex):
    """Helper to format delta string based on available columns"""
    if has_greek:
        d = row.get('net_greek_delta', 0)
        return f"|Δ{d:.2f}"
    elif has_dex:
        net_dex = row.get('call_dex', 0) - row.get('put_dex', 0)
        if abs(net_dex) > 1000:
            return f"|D{format_value(net_dex)}"
    return ""

def process_file_data(filepath):
    try:
        df = pd.read_csv(filepath)
        required_cols = ['strike', 'call_gex', 'put_gex', 'net_gex']
        if not all(col in df.columns for col in required_cols):
            return None

        # Check for Greek Delta or Legacy Dex
        has_greek = 'net_greek_delta' in df.columns
        has_dex = 'call_dex' in df.columns and 'put_dex' in df.columns

        # 1. Flip Zone Logic
        flip_zone = compute_flip_zone(df)
        flip_delta = 0.0
        if flip_zone is not None:
            # Find strike closest to flip
            closest_idx = (df['strike'] - flip_zone).abs().idxmin()
            if has_greek:
                flip_delta = df.loc[closest_idx, 'net_greek_delta']

        # 2. Wall Logic (Extract GEX & Delta strings)
        cw_idx = df['call_gex'].idxmax()
        call_wall = df.loc[cw_idx, 'strike']
        cw_row = df.loc[cw_idx]
        cw_str = f"{format_value(cw_row['call_gex'])}{get_delta_str(cw_row, has_greek, has_dex)}"
        
        pw_idx = df['put_gex'].abs().idxmax()
        put_wall = df.loc[pw_idx, 'strike']
        pw_row = df.loc[pw_idx]
        pw_str = f"{format_value(pw_row['put_gex'])}{get_delta_str(pw_row, has_greek, has_dex)}"

        # 3. Histogram Data Compression
        df['abs_net_gex'] = df['net_gex'].abs()
        max_gex = df['abs_net_gex'].max()
        if max_gex == 0: max_gex = 1.0

        significant_df = df[df['abs_net_gex'] > (max_gex * MIN_PCT_THRESHOLD)].copy()
        significant_df = significant_df.sort_values('abs_net_gex', ascending=False).head(15)

        data_str_parts = []
        for _, row in significant_df.iterrows():
            strike = float(row['strike'])
            val_str = format_value(row['net_gex'])
            d_str = get_delta_str(row, has_greek, has_dex)
            line = f"{strike:.1f}:{val_str}{d_str}"
            data_str_parts.append(line)
        
        compact_str = "\\n".join(data_str_parts)

        return {
            "cw": float(call_wall),
            "cw_info": cw_str,
            "pw": float(put_wall),
            "pw_info": pw_str,
            "flip": float(flip_zone) if flip_zone else None,
            "flip_delta": float(flip_delta),
            "max": float(max_gex),
            "data_str": compact_str
        }

    except Exception as e:
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
print(f"⏳ Processing {len(files)} files...")

for f in files:
    parts = f.split('_')
    if len(parts) < 4: continue
    symbol = parts[0].upper()
    date_str = parts[-1].replace('.csv', '')
    if len(date_str) != 8: continue
    
    ymd_int = int(date_str) 
    
    data = process_file_data(f)
    if data:
        history_map.setdefault(symbol, []).append({
            "ymd": ymd_int,
            "data": data
        })

# Truncate History
for sym in history_map:
    history_map[sym].sort(key=lambda x: x['ymd'])
    if len(history_map[sym]) > MAX_DAYS_PER_TICKER:
        history_map[sym] = history_map[sym][-MAX_DAYS_PER_TICKER:]

print(f"✅ Data ready for {len(history_map)} tickers.")

# ===============================================
# Generate PineScript Output
# ===============================================
output_filename = "Universal_GEX_History.pine"

pine_code = f"""//@version=6
indicator("Universal GEX History (Optimized)", overlay=true, max_labels_count=500, max_lines_count=500)

// --- Generated {datetime.now().strftime('%Y-%m-%d')} ---
// Architecture: Date-Keyed Lookup with Full GEX/Delta Details

// --- Settings ---
sz_label = input.string("normal", "Label Size", options=["auto", "tiny", "small", "normal", "large", "huge"])
show_hist = input.bool(true, "Show Historic Labels")

// --- Helper: Get Date Integer ---
f_ymd() =>
    year * 10000 + month * 100 + dayofmonth

// ==========================================
// DATA FUNCTIONS (One per Ticker)
// ==========================================
// Returns: [CW, CW_Info, PW, PW_Info, Flip, FlipDelta, DataStr, Max]
"""

# Generate Ticker Functions
for symbol, records in history_map.items():
    safe_sym = symbol.replace("-", "_").replace(".", "")
    
    pine_code += f"""
f_data_{safe_sym}(int ymd) =>
    switch ymd
"""
    for rec in records:
        d = rec['data']
        f_val = d['flip'] if d['flip'] is not None else "float(na)"
        f_del = d['flip_delta'] if d['flip_delta'] is not None else 0.0
        
        # Escape strings
        d_str_safe = d['data_str'].replace('"', '\\"')
        cw_safe = d['cw_info']
        pw_safe = d['pw_info']
        
        pine_code += f"        {rec['ymd']} => [{d['cw']}, \"{cw_safe}\", {d['pw']}, \"{pw_safe}\", {f_val}, {f_del}, \"{d_str_safe}\", {d['max']}]\n"

    # Default Case
    pine_code += "        => [float(na), \"\", float(na), \"\", float(na), 0.0, \"\", float(na)]\n"

# Main Dispatcher
pine_code += """
// ==========================================
// MAIN LOGIC
// ==========================================
int cur_ymd = f_ymd()

// Dispatch based on Ticker
[cw, cw_info, pw, pw_info, flip, flip_d, d_str, d_max] = switch syminfo.ticker
"""

for symbol in history_map:
    safe_sym = symbol.replace("-", "_").replace(".", "")
    pine_code += f'    "{symbol}" => f_data_{safe_sym}(cur_ymd)\n'

# Default dispatch
pine_code += '    => [float(na), "", float(na), "", float(na), 0.0, "", float(na)]\n'

pine_code += """
// ==========================================
// VISUALIZATION
// ==========================================

// 1. Plot Walls & Flip (Lines)
plot(cw, "Call Wall", color=color.new(color.green, 30), linewidth=2, style=plot.style_stepline)
plot(pw, "Put Wall",  color=color.new(color.red, 30),   linewidth=2, style=plot.style_stepline)
plot(flip, "Flip Zone", color=color.new(color.blue, 0), linewidth=1, style=plot.style_circles)

// 2. Labels (History & Current)
if show_hist
    
    // A. Main Data Label (Walls + Histogram)
    if not na(cw)
        float anchor_price = cw
        
        // Build Header with GEX/Delta info: CW: 4500 ($5B | Δ0.50)
        string txt = "CW: " + str.tostring(cw) + " (" + cw_info + ")" + "\\n" + 
                     "PW: " + str.tostring(pw) + " (" + pw_info + ")"
        
        if d_str != ""
            txt := txt + "\\n----------------\\n" + d_str

        color lab_col = color.gray
        if not na(flip)
            lab_col := close > flip ? color.green : color.red
        else
            lab_col := close > cw ? color.green : (close < pw ? color.red : color.gray)

        label.new(bar_index, anchor_price, txt, 
                  color=lab_col, 
                  textcolor=color.white, 
                  style=label.style_label_left, 
                  size=sz_label)

    // B. SEPARATE FLIP LABEL
    if not na(flip)
        string f_txt = "Flip: " + str.tostring(flip, "#.##")
        if flip_d != 0.0
            f_txt := f_txt + "\\nΔ: " + str.tostring(flip_d, "#.2f")
            
        label.new(bar_index, flip, f_txt, 
                  color=color.blue, 
                  textcolor=color.white, 
                  style=label.style_label_left, 
                  size=sz_label)
"""

with open(output_filename, "w") as f:
    f.write(pine_code)

print(f"✅ Created {output_filename} (v7.3 Walls + Delta)")
