# ===========================================================
# GEX to Pine Script Converter v7.7 (Flip Zone Trailing Line)
# ===========================================================
# FEATURES:
#   ✅ FIX: Flip Zone now uses a STEPLINE (Trailing Line) instead of dots.
#   ✅ LOGIC: Flip Label only draws when the level shifts.
#   ✅ VISUALS: Consistent "Smart Label" behavior for Walls & Flip.
# ===========================================================

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

print("🌲 Starting GEX Converter v7.7 (Flip Zone Line)...")

# ===============================================
# Configuration
# ===============================================
MAX_DAYS_PER_TICKER = 250
MIN_PCT_THRESHOLD = 0.25  # Only show levels > 25% of max
HIST_BAR_LENGTH = 30      # Max length of histogram bars in bars

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
    """Compact K/M/B formatting."""
    abs_val = abs(val)
    if abs_val >= 1_000_000_000:
        return f"${val/1_000_000_000:.1f}B"
    elif abs_val >= 1_000_000:
        return f"${val/1_000_000:.1f}M"
    elif abs_val >= 1_000:
        return f"${val/1_000:.0f}K"
    else:
        return f"${val:.0f}"

def get_delta_str(row, has_greek, has_dex):
    """Returns ' | D: Δ0.50' or ' | D: D$5M'"""
    if has_greek:
        d = row.get('net_greek_delta', 0)
        return f" | D: Δ{d:.2f}"
    elif has_dex:
        net_dex = row.get('call_dex', 0) - row.get('put_dex', 0)
        if abs(net_dex) > 1000:
            return f" | D: D{format_value(net_dex)}"
    return ""

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

        has_greek = 'net_greek_delta' in df.columns
        has_dex = 'call_dex' in df.columns and 'put_dex' in df.columns

        # 1. Flip Zone
        flip_zone = compute_flip_zone(df)
        flip_delta = 0.0
        if flip_zone is not None:
            closest_idx = (df['strike'] - flip_zone).abs().idxmin()
            if has_greek:
                flip_delta = df.loc[closest_idx, 'net_greek_delta']

        # 2. Walls
        cw_idx = df['call_gex'].idxmax()
        call_wall = df.loc[cw_idx, 'strike']
        cw_row = df.loc[cw_idx]
        cw_info = f"GEX: {format_value(cw_row['call_gex'])}{get_delta_str(cw_row, has_greek, has_dex)}"
        
        pw_idx = df['put_gex'].abs().idxmax()
        put_wall = df.loc[pw_idx, 'strike']
        pw_row = df.loc[pw_idx]
        pw_info = f"GEX: {format_value(pw_row['put_gex'])}{get_delta_str(pw_row, has_greek, has_dex)}"

        # 3. Histogram Data
        df['abs_net_gex'] = df['net_gex'].abs()
        max_gex = df['abs_net_gex'].max()
        if max_gex == 0: max_gex = 1.0

        significant_df = df[df['abs_net_gex'] > (max_gex * MIN_PCT_THRESHOLD)].copy()
        significant_df = significant_df.sort_values('abs_net_gex', ascending=False).head(15)

        hist_lines = []
        
        array_strikes = []
        array_lengths = []
        array_signs = []
        array_labels = []

        for _, row in significant_df.iterrows():
            strike = float(row['strike'])
            val_str = format_value(row['net_gex'])
            d_str = get_delta_str(row, has_greek, has_dex).replace(" | D: ", ", ")
            
            hist_lines.append(f"{strike:.1f}: {val_str}{d_str}")

            length = int((row['abs_net_gex'] / max_gex) * HIST_BAR_LENGTH)
            if length < 2: length = 2
            
            array_strikes.append(str(strike))
            array_lengths.append(str(length))
            array_signs.append("1" if row['net_gex'] >= 0 else "-1")
            array_labels.append(f"'[{val_str}{d_str}]'")

        compact_str = "\\n".join(hist_lines)

        return {
            "cw": float(call_wall),
            "cw_info": cw_info,
            "pw": float(put_wall),
            "pw_info": pw_info,
            "flip": float(flip_zone) if flip_zone else None,
            "flip_delta": float(flip_delta),
            "max": float(max_gex),
            "data_str": compact_str,
            "arrays": {
                "strikes": ", ".join(array_strikes),
                "lengths": ", ".join(array_lengths),
                "signs": ", ".join(array_signs),
                "labels": ", ".join(array_labels)
            }
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
indicator("Universal GEX History (Smart Labels)", overlay=true, max_labels_count=500, max_lines_count=500)

// --- Generated {datetime.now().strftime('%Y-%m-%d')} ---
// Architecture: Date-Keyed Lookup + Smart Label Persistence

// --- Settings ---
sz_label = input.string("normal", "Label Size", options=["auto", "tiny", "small", "normal", "large", "huge"])
show_hist = input.bool(true, "Show Historic Labels")

// --- Helper: Get Date Integer ---
f_ymd() =>
    year * 10000 + month * 100 + dayofmonth

// ==========================================
// DATA FUNCTIONS (One per Ticker)
// ==========================================
"""

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
        
        d_str_safe = d['data_str'].replace('"', '\\"')
        cw_safe = d['cw_info']
        pw_safe = d['pw_info']
        
        pine_code += f"        {rec['ymd']} => [{d['cw']}, \"{cw_safe}\", {d['pw']}, \"{pw_safe}\", {f_val}, {f_del}, \"{d_str_safe}\"]\n"

    pine_code += "        => [float(na), \"\", float(na), \"\", float(na), 0.0, \"\"]\n"

# Arrays Function
pine_code += "\n// --- Array Functions ---\n"
for symbol, records in history_map.items():
    safe_sym = symbol.replace("-", "_").replace(".", "")
    if not records: continue
    last_arrays = records[-1]['data']['arrays']
    
    pine_code += f"""
f_arrays_{safe_sym}() =>
    [array.from({last_arrays['strikes']}), array.from({last_arrays['lengths']}), array.from({last_arrays['signs']}), array.from({last_arrays['labels']})]
"""

pine_code += """
// ==========================================
// MAIN LOGIC
// ==========================================
int cur_ymd = f_ymd()

// Dispatch Data
[cw, cw_info, pw, pw_info, flip, flip_d, d_str] = switch syminfo.ticker
"""

for symbol in history_map:
    safe_sym = symbol.replace("-", "_").replace(".", "")
    pine_code += f'    "{symbol}" => f_data_{safe_sym}(cur_ymd)\n'
pine_code += '    => [float(na), "", float(na), "", float(na), 0.0, ""]\n'

pine_code += """
// ==========================================
// VISUALIZATION
// ==========================================

// 1. Plot Walls & Flip
bool is_combined = not na(cw) and not na(pw) and cw == pw

// Plot Walls (Stepline)
plot(cw, "Call Wall", color=is_combined ? color.purple : color.green, linewidth=2, style=plot.style_stepline)
plot(pw, "Put Wall",  color=is_combined ? color.purple : color.red,   linewidth=2, style=plot.style_stepline)

// Plot Flip Zone (Stepline) - Changed from circles to line
plot(flip, "Flip Zone", color=color.new(color.blue, 0), linewidth=1, style=plot.style_stepline)

// 2. Smart Label Logic
if show_hist
    // Detect Changes: only true if current value != previous value
    bool cw_changed = (cw != cw[1])
    bool pw_changed = (pw != pw[1])
    bool flip_changed = (flip != flip[1])
    
    // Also check if we just started having data
    bool new_data_cw = not na(cw) and na(cw[1])
    bool new_data_pw = not na(pw) and na(pw[1])
    bool new_data_flip = not na(flip) and na(flip[1])

    // --- FLIP LABEL ---
    // Only draw label if Flip Zone changes or starts new
    if not na(flip) and (flip_changed or new_data_flip)
        string f_txt = "Flip: " + str.tostring(flip, "#.##")
        if flip_d != 0.0
            f_txt := f_txt + "\\nΔ: " + str.tostring(flip_d, "#.2f")
        label.new(bar_index, flip, f_txt, color=color.blue, textcolor=color.white, style=label.style_label_left, size=sz_label)

    // --- WALL LABELS ---
    if not na(cw) and (cw_changed or pw_changed or new_data_cw or new_data_pw)
        if is_combined
            string txt = "COMBINED: " + str.tostring(cw) + "\\n" + "C: " + cw_info + "\\n" + "P: " + pw_info
            if not barstate.islast and d_str != ""
                txt := txt + "\\n----------------\\n" + d_str
            label.new(bar_index, cw, txt, color=color.purple, textcolor=color.white, style=label.style_label_left, size=sz_label)
        else
            // Call Wall
            if not na(cw)
                string c_txt = "CW: " + str.tostring(cw) + " | " + cw_info
                if not barstate.islast and d_str != ""
                    c_txt := c_txt + "\\n----------------\\n" + d_str
                label.new(bar_index, cw, c_txt, color=color.green, textcolor=color.white, style=label.style_label_left, size=sz_label)
            
            // Put Wall
            if not na(pw)
                string p_txt = "PW: " + str.tostring(pw) + " | " + pw_info
                label.new(bar_index, pw, p_txt, color=color.red, textcolor=color.white, style=label.style_label_left, size=sz_label)

// 3. SCENARIO 3: VISUAL HISTOGRAM (Last Bar Only)
if barstate.islast
    float[] a_str = array.new<float>()
    int[]   a_len = array.new<int>()
    int[]   a_sgn = array.new<int>()
    string[] a_lbl = array.new<string>()
    
    [t1, t2, t3, t4] = switch syminfo.ticker
"""

for symbol in history_map:
    safe_sym = symbol.replace("-", "_").replace(".", "")
    pine_code += f'        "{symbol}" => f_arrays_{safe_sym}()\n'
pine_code += '        => [array.new<float>(), array.new<int>(), array.new<int>(), array.new<string>()]\n'

pine_code += """
    a_str := t1, a_len := t2, a_sgn := t3, a_lbl := t4

    if array.size(a_str) > 0
        for i = 0 to array.size(a_str) - 1
            float s = array.get(a_str, i)
            int l   = array.get(a_len, i)
            int sg  = array.get(a_sgn, i)
            string txt = array.get(a_lbl, i)
            
            col_line = sg > 0 ? color.new(color.green, 40) : color.new(color.red, 40)
            col_lbl  = sg > 0 ? color.green : color.red
            
            line.new(bar_index, s, bar_index + l, s, color=col_line, width=2)
            label.new(bar_index + l, s, txt, style=label.style_label_left, textcolor=color.white, color=col_lbl, size=size.tiny)
"""

with open(output_filename, "w") as f:
    f.write(pine_code)

print(f"✅ Created {output_filename} (v7.7 Flip Zone Line)")
