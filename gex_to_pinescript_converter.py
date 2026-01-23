# ===========================================================
# GEX to Pine Script Converter v7.1 (Type Safety Fix)
# ===========================================================
# FEATURES:
#   ✅ FIX: "Value with NA type" error resolved using float(na)
#   ✅ Auto-detect current chart symbol (syminfo.ticker)
#   ✅ Compressed daily data (ymd => [CW, PW, Flip, DataStr, Max])
#   ✅ Bar replay support (Date-keyed lookup)
#   ✅ Token-safe compression (Top levels only)
#   ✅ Configurable history length (Max 250 days)
# ===========================================================

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

print("🌲 Starting GEX Converter v7.1 (Type Safety Fix)...")

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
        return f"${val/1_000_000:.0f}M" # Removed decimal for compactness
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

def process_file_data(filepath):
    try:
        df = pd.read_csv(filepath)
        required_cols = ['strike', 'call_gex', 'put_gex', 'net_gex']
        if not all(col in df.columns for col in required_cols):
            return None

        # Check for Greek Delta or Legacy Dex
        has_greek = 'net_greek_delta' in df.columns
        has_dex = 'call_dex' in df.columns and 'put_dex' in df.columns

        # 1. Key Metrics
        flip_zone = compute_flip_zone(df)
        
        cw_idx = df['call_gex'].idxmax()
        call_wall = df.loc[cw_idx, 'strike']
        
        pw_idx = df['put_gex'].abs().idxmax()
        put_wall = df.loc[pw_idx, 'strike']

        # 2. Histogram Data Compression
        # Filter for significant levels only (> 25% of max) to save tokens
        df['abs_net_gex'] = df['net_gex'].abs()
        max_gex = df['abs_net_gex'].max()
        if max_gex == 0: max_gex = 1.0

        # Filter: Top 40 AND > Threshold
        significant_df = df[df['abs_net_gex'] > (max_gex * MIN_PCT_THRESHOLD)].copy()
        significant_df = significant_df.sort_values('abs_net_gex', ascending=False).head(15) # Cap at top 15 lines per day for safety

        data_str_parts = []
        for _, row in significant_df.iterrows():
            strike = float(row['strike'])
            val_str = format_value(row['net_gex'])
            
            # Delta formatting
            delta_str = ""
            if has_greek:
                d = row['net_greek_delta']
                delta_str = f"|Δ{d:.2f}"
            elif has_dex:
                net_dex = row.get('call_dex', 0) - row.get('put_dex', 0)
                if abs(net_dex) > 1000:
                    delta_str = f"|D{format_value(net_dex)}"
            
            # Compact Line: "4500:$5B|Δ0.5"
            line = f"{strike:.1f}:{val_str}{delta_str}"
            data_str_parts.append(line)
        
        # Join with specific delimiter for display
        # We use newline so it stacks in the label automatically
        compact_str = "\\n".join(data_str_parts)

        return {
            "cw": float(call_wall),
            "pw": float(put_wall),
            "flip": float(flip_zone) if flip_zone else None,
            "max": float(max_gex),
            "data_str": compact_str
        }

    except Exception as e:
        # print(f"   ⚠️ Error processing {filepath}: {e}")
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
    
    # YMD Integer for Switch Case (e.g., 20231025)
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
// Architecture: Date-Keyed Lookup (YMD) with Type Safety

// --- Settings ---
sz_label = input.string("normal", "Label Size", options=["auto", "tiny", "small", "normal", "large", "huge"])
show_hist = input.bool(true, "Show Historic Labels")

// --- Helper: Get Date Integer ---
// Converts current bar time to YYYYMMDD integer
f_ymd() =>
    year * 10000 + month * 100 + dayofmonth

// ==========================================
// DATA FUNCTIONS (One per Ticker)
// ==========================================
// Returns: [CallWall, PutWall, Flip, DataString, MaxGex]
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
        # Escape string for Pine
        safe_str = d['data_str'].replace('"', '\\"')
        
        # Pine Switch Line: case => [return tuple]
        pine_code += f"        {rec['ymd']} => [{d['cw']}, {d['pw']}, {f_val}, \"{safe_str}\", {d['max']}]\n"

    # Default Case (Fixed: Explicit casting)
    pine_code += "        => [float(na), float(na), float(na), \"\", float(na)]\n"

# Main Dispatcher
pine_code += """
// ==========================================
// MAIN LOGIC
// ==========================================
int cur_ymd = f_ymd()

// Dispatch based on Ticker
[cw, pw, flip, d_str, d_max] = switch syminfo.ticker
"""

for symbol in history_map:
    safe_sym = symbol.replace("-", "_").replace(".", "")
    pine_code += f'    "{symbol}" => f_data_{safe_sym}(cur_ymd)\n'

# Default dispatch (Fixed: Explicit casting)
pine_code += '    => [float(na), float(na), float(na), "", float(na)]\n'

pine_code += """
// ==========================================
// VISUALIZATION
// ==========================================

// 1. Plot Walls & Flip (Lines)
plot(cw, "Call Wall", color=color.new(color.green, 30), linewidth=2, style=plot.style_stepline)
plot(pw, "Put Wall",  color=color.new(color.red, 30),   linewidth=2, style=plot.style_stepline)
plot(flip, "Flip Zone", color=color.new(color.purple, 0), linewidth=1, style=plot.style_circles)

// 2. Labels (History & Current)
// Only draw if we have data
if not na(cw) and show_hist
    
    // Combined Data Label (Off-Candle)
    // We position this at the Call Wall or Max GEX level
    float anchor_price = cw
    
    // Construct Label Text
    string txt = "CW: " + str.tostring(cw) + "\\n" + "PW: " + str.tostring(pw)
    
    if not na(flip)
        txt := txt + "\\nFlip: " + str.tostring(flip, "#.##")
        
    // Add the compressed histogram data
    if d_str != ""
        txt := txt + "\\n----------------\\n" + d_str

    // Color based on regime (simple check)
    color lab_col = color.gray
    if not na(flip)
        lab_col := close > flip ? color.green : color.red
    else
        lab_col := close > cw ? color.green : (close < pw ? color.red : color.gray)

    // Draw Label
    // style_label_left places it to the right of the bar (off-candle)
    label.new(bar_index, anchor_price, txt, 
              color=lab_col, 
              textcolor=color.white, 
              style=label.style_label_left, 
              size=sz_label)
"""

with open(output_filename, "w") as f:
    f.write(pine_code)

print(f"✅ Created {output_filename} (v7.1 Type Safe)")
