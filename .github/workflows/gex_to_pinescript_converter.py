# ===========================================================
# GEX to Pine Script Converter (Standalone)
# ===========================================================
# Usage:
# 1. Place this script in the same folder as your GEX CSV files.
# 2. Run it: python gex_to_pinescript_converter.py
# 3. It will generate a .pine file for every .csv found.
# ===========================================================

import os
import pandas as pd
import numpy as np
from datetime import datetime

print("🌲 Starting GEX to Pine Script Converter...")

# ===============================================
# Helper Functions
# ===============================================
def compute_flip_zone(df):
    """
    Recalculates the Flip Zone from CSV data.
    Assumes CSV has 'strike' and 'net_gex' columns.
    """
    try:
        # Sort by strike to ensure cumulative sum is correct
        df_sorted = df.sort_values("strike").reset_index(drop=True)
        
        # Calculate Cumulative GEX
        df_sorted["cum_gex"] = df_sorted["net_gex"].cumsum()
        
        # Find where sign flips from neg to pos or pos to neg
        signs = np.sign(df_sorted["cum_gex"])
        flips = np.where(np.diff(signs))[0]
        
        if len(flips) > 0:
            # Linear interpolation for the flip point
            idx = flips[0]
            low_strike = df_sorted.loc[idx, "strike"]
            high_strike = df_sorted.loc[idx + 1, "strike"]
            return (low_strike + high_strike) / 2
    except Exception as e:
        print(f"   ⚠️ Flip calculation error: {e}")
    return None

def generate_pine_file(filepath):
    filename = os.path.basename(filepath)
    
    # Try to parse Symbol and Date from filename (e.g., SPY_GEX_robust_20251224.csv)
    parts = filename.split('_')
    symbol = parts[0] if len(parts) > 0 else "UNKNOWN"
    date_tag = parts[-1].replace('.csv', '') if len(parts) > 0 else datetime.now().strftime("%Y%m%d")

    try:
        df = pd.read_csv(filepath)
        required_cols = ['strike', 'call_gex', 'put_gex', 'net_gex']
        
        if not all(col in df.columns for col in required_cols):
            print(f"   ❌ Skipping {filename}: Missing columns.")
            return

        # 1. Calc Metrics
        flip_zone = compute_flip_zone(df)
        call_wall = df.loc[df['call_gex'].idxmax(), 'strike']
        put_wall = df.loc[df['put_gex'].idxmax(), 'strike']

        # 2. Top 50 Active Strikes (for histogram)
        df['abs_net_gex'] = df['net_gex'].abs()
        top_strikes = df.sort_values('abs_net_gex', ascending=False).head(50)
        
        # Normalize for bar height (max 30 lines high)
        max_val = top_strikes['abs_net_gex'].max()
        if max_val == 0: max_val = 1
        
        # Prepare arrays for Pine Script
        p_strikes = []
        p_lengths = []
        p_signs = [] # 1=Green, -1=Red
        
        for _, row in top_strikes.iterrows():
            length = int((row['abs_net_gex'] / max_val) * 30)
            if length < 2: length = 2 # Min visibility
            
            p_strikes.append(str(float(row['strike'])))
            p_lengths.append(str(length))
            p_signs.append("1" if row['net_gex'] >= 0 else "-1")

        # 3. Write Pine Script
        pine_content = f"""//@version=5
indicator("GEX Profile: {symbol}", overlay=true, max_lines_count=100, max_labels_count=100)

// Source: {filename}
// Flip: {flip_zone if flip_zone else 'N/A'} | Call Wall: {call_wall} | Put Wall: {put_wall}

var line[] gex_lines = array.new_line()
var label[] gex_labels = array.new_label()

// --- Data Arrays ---
float[] strikes = array.from({', '.join(p_strikes)})
int[] lengths   = array.from({', '.join(p_lengths)})
int[] signs     = array.from({', '.join(p_signs)})

if barstate.islast
    // Cleanup
    for l in gex_lines
        line.delete(l)
    for lb in gex_labels
        label.delete(lb)

    // 1. Plot Flip Zone
    float flip = {flip_zone if flip_zone else 'na'}
    if not na(flip)
        line.new(bar_index, flip, bar_index + 10, flip, color=color.blue, width=2, style=line.style_dashed)
        label.new(bar_index + 10, flip, "Flip: " + str.tostring(flip, "#.##"), style=label.style_label_left, textcolor=color.white, color=color.blue)

    // 2. Plot Walls
    float c_wall = {call_wall}
    float p_wall = {put_wall}
    label.new(bar_index + 5, c_wall, "Call Wall: " + str.tostring(c_wall), style=label.style_label_left, textcolor=color.white, color=color.green)
    label.new(bar_index + 5, p_wall, "Put Wall: " + str.tostring(p_wall), style=label.style_label_left, textcolor=color.white, color=color.red)

    // 3. Plot Histogram Bars
    if array.size(strikes) > 0
        for i = 0 to array.size(strikes) - 1
            float s = array.get(strikes, i)
            int l   = array.get(lengths, i)
            int sg  = array.get(signs, i)
            
            col = sg > 0 ? color.new(color.green, 40) : color.new(color.red, 40)
            ln = line.new(bar_index, s, bar_index + l, s, color=col, width=2)
            array.push(gex_lines, ln)
"""
        
        out_name = f"{symbol}_GEX_{date_tag}.pine"
        with open(out_name, "w") as f:
            f.write(pine_content)
        
        print(f"✅ Generated {out_name}")

    except Exception as e:
        print(f"❌ Error processing {filename}: {e}")

# ===============================================
# Main Loop
# ===============================================
files = [f for f in os.listdir('.') if f.endswith('.csv') and "GEX" in f]

if not files:
    print("⚠️ No GEX CSV files found in this folder.")
else:
    print(f"found {len(files)} files to convert.")
    for f in files:
        generate_pine_file(f)

print("\n🏁 Conversion Complete.")
