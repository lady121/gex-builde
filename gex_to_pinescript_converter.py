# ===========================================================
# GEX to Pine Script Converter v4.9 (Dynamic K/M/B Formatting)
# ===========================================================
# Features:
#   ✅ FORMAT: Smart scaling (1K, 100K, 2M, 1.5B) matching your request
#   ✅ CLEANUP: Removed "$" sign for cleaner look (e.g., "2M" instead of "$2M")
#   ✅ DATA: Includes DEX (Delta Exposure) data
#   ✅ VISUALS: Smart "COMBINED" labels + Rich separate labels
#   ✅ CORE: Ultra-compression & auto-limits active
# ===========================================================

import os
import json
import sys
import time
import pandas as pd
import numpy as np
from datetime import datetime

# Try importing clipboard library
try:
    import pyperclip
    CLIPBOARD_AVAILABLE = True
except ImportError:
    CLIPBOARD_AVAILABLE = False

print("🌲 Starting Historical GEX Converter (v4.9 - Dynamic K/M/B)...")

# ===============================================
# Configuration
# ===============================================
BASE_HISTORY_DAYS = 300 
OUTPUT_FILENAME = "Universal_GEX_History.pine"
CACHE_FILE = ".gex_cache.json"

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
        
        has_dex = 'call_dex' in df.columns and 'put_dex' in df.columns

        # 1️⃣ Compute metrics
        flip_zone = compute_flip_zone(df)
        
        # Walls & Associated Values
        cw_idx = df['call_gex'].idxmax()
        call_wall = df.loc[cw_idx, 'strike']
        cw_gex = df.loc[cw_idx, 'call_gex']
        cw_dex = df.loc[cw_idx, 'call_dex'] if has_dex else 0.0

        pw_idx = df['put_gex'].abs().idxmax()
        put_wall = df.loc[pw_idx, 'strike']
        pw_gex = df.loc[pw_idx, 'put_gex']
        pw_dex = df.loc[pw_idx, 'put_dex'] if has_dex else 0.0

        # 2️⃣ Prepare Histogram Data (Compressed)
        df['abs_net_gex'] = df['net_gex'].abs()
        top_strikes = df.sort_values('abs_net_gex', ascending=False).head(40)
        max_val = top_strikes['abs_net_gex'].max()
        if max_val == 0: max_val = 1.0

        # Build Inner Data String
        data_pairs = []
        for _, row in top_strikes.iterrows():
            s = float(row['strike'])
            g = float(row['net_gex'])
            data_pairs.append(f"{s:.2f}:{g:.0f}")
        
        inner_data = ";".join(data_pairs)

        # 3️⃣ Format Day Blob for Monthly String
        # Format: CW~PW~Flip~Max~Data~CW_GEX~PW_GEX~CW_DEX~PW_DEX
        flip_val = f"{float(flip_zone):.2f}" if flip_zone else "n"
        
        # Store GEX/DEX as integers (rounded) to save space, formatted in Pine later
        day_blob = (f"{float(call_wall):.2f}~{float(put_wall):.2f}~{flip_val}~{float(max_val):.0f}~{inner_data}~"
                    f"{float(cw_gex):.0f}~{float(pw_gex):.0f}~{float(cw_dex):.0f}~{float(pw_dex):.0f}")
        
        return day_blob

    except Exception as e:
        print(f"   ⚠️ Error processing {filepath}: {e}")
        return None

def generate_pinescript(current_files):
    # Process Data
    history_map = {}
    
    # Adaptive Limits
    num_symbols = 0
    symbols_set = set()
    for f in current_files:
        parts = f.split('_')
        if len(parts) >= 4:
            symbols_set.add(parts[0].upper())
    num_symbols = len(symbols_set)
    max_history_days = int(max(100, 5000 / max(1, num_symbols)))

    print(f"   ⏳ Processing {len(current_files)} files for {num_symbols} symbols...")

    for f in current_files:
        parts = f.split('_')
        if len(parts) < 4: continue
        symbol = parts[0].upper()
        date_str = parts[-1].replace('.csv', '')
        if len(date_str) != 8: continue
        
        data_blob = process_file_data(f)
        if data_blob:
            d_int = int(date_str)
            ym_int = int(date_str[:6])
            day_part = int(date_str[6:8])
            
            history_map.setdefault(symbol, []).append({
                "date_int": d_int,
                "ym_int": ym_int,
                "day": day_part,
                "blob": data_blob
            })

    # Sort & Compress
    final_map = {}
    for sym, records in history_map.items():
        records.sort(key=lambda x: x['date_int'])
        if len(records) > max_history_days:
            records = records[-max_history_days:]
        
        monthly_data = {}
        for rec in records:
            ym = rec['ym_int']
            d = rec['day']
            blob = rec['blob']
            entry = f"{d}={blob}"
            monthly_data.setdefault(ym, []).append(entry)
        
        final_map[sym] = {}
        for ym, entries in monthly_data.items():
            final_map[sym][ym] = "|".join(entries)

    # Generate Pine Code
    pine_code = f"""//@version=6
indicator("Universal GEX History (Bar Replay)", overlay=true, max_lines_count=500, max_labels_count=500)

// --- Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---
// Mode: V4.9 (Dynamic K/M/B)

// --- Settings ---
show_labels    = input.bool(true, "Show Histogram Values", group="Visuals")
line_width     = input.int(1, "GEX Line Width", minval=1, maxval=4, group="Visuals")
min_pct        = input.int(30, "Label Threshold %", minval=0, maxval=100, tooltip="Only show labels for bars larger than this % of the day's max.", group="Visuals")
sz_hist_txt    = input.string("tiny", "Histogram Text Size", options=["auto", "tiny", "small", "normal", "large", "huge"], group="Visuals")
sz_wall_txt    = input.string("small", "Wall Label Size", options=["auto", "tiny", "small", "normal", "large", "huge"], group="Visuals")

var string current_ticker = syminfo.ticker

// --- Helper: Format numbers (K, M, B) ---
f_fmt(float v) =>
    float av = math.abs(v)
    string s = ""
    if av >= 1000000000
        s := str.format("{{0,number,#.##}}B", v / 1000000000)
    else if av >= 1000000
        s := str.format("{{0,number,#.##}}M", v / 1000000)
    else if av >= 1000
        s := str.format("{{0,number,#.##}}K", v / 1000)
    else
        s := str.format("{{0,number,#.##}}", v)
    s

// --- Helper: Parse Monthly Blob ---
// Returns: [cw, pw, fl, mv, d_str, cw_g, pw_g, cw_d, pw_d]
f_parse_day_data(string m_blob, int d_target) =>
    float cw = na, float pw = na, float fl = na, float mv = na, string d_str = ""
    float cwg = na, float pwg = na, float cwd = na, float pwd = na
    
    if m_blob != ""
        string[] days = str.split(m_blob, "|")
        int cnt = array.size(days)
        string target_prefix = str.tostring(d_target) + "="
        
        for i = 0 to cnt - 1
            string day_entry = array.get(days, i)
            if str.startswith(day_entry, target_prefix)
                string content = str.replace(day_entry, target_prefix, "")
                string[] comps = str.split(content, "~")
                // Expecting 9 components now
                if array.size(comps) >= 9
                    cw := str.tonumber(array.get(comps, 0))
                    pw := str.tonumber(array.get(comps, 1))
                    string fl_str = array.get(comps, 2)
                    fl := fl_str == "n" ? na : str.tonumber(fl_str)
                    mv := str.tonumber(array.get(comps, 3))
                    d_str := array.get(comps, 4)
                    cwg := str.tonumber(array.get(comps, 5))
                    pwg := str.tonumber(array.get(comps, 6))
                    cwd := str.tonumber(array.get(comps, 7))
                    pwd := str.tonumber(array.get(comps, 8))
                break
    [cw, pw, fl, mv, d_str, cwg, pwg, cwd, pwd]

// --- Helper: Label Formatter ---
f_lbl_txt(string title, float price, float gex, float dex) =>
    str.format("{{0}}: {{1}} (GEX: {{2}}, DEX: {{3}})", title, str.tostring(price), f_fmt(gex), f_fmt(dex))

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
                    
                    if show_labels and math.abs(net_gex) > (max_v * (min_pct / 100.0))
                        color tc = net_gex >= 0 ? color.green : color.red
                        string txt = f_fmt(net_gex)
                        label.new(bar_index + length, strike, txt, style=label.style_label_left, textcolor=tc, color=color.new(color.white, 100), size=sz_hist_txt)

"""

    # --- Symbol Functions ---
    for symbol, month_map in final_map.items():
        func_name = f"f_symbol_{symbol}"
        pine_code += f"""
// {symbol}
{func_name}(int ym) =>
    string b = switch ym
"""
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
// 2. Parse Daily Data
[plot_c_wall, plot_p_wall, plot_flip, max_val, data_str, cw_g, pw_g, cw_d, pw_d] = f_parse_day_data(month_blob, d)

// 3. Draw Lines
plot(plot_c_wall, "Call Wall", color=color.new(color.green, 20), linewidth=2, style=plot.style_stepline)
plot(plot_p_wall, "Put Wall",  color=color.new(color.red, 20),   linewidth=2, style=plot.style_stepline)
plot(plot_flip,   "Flip Zone", color=color.new(color.purple, 0), linewidth=2, style=plot.style_cross)

// 4. Draw Rich Labels
var label l_cw = label.new(na, na, "", style=label.style_label_left, textcolor=color.green, color=color.new(color.white, 100), size=sz_wall_txt)
var label l_pw = label.new(na, na, "", style=label.style_label_left, textcolor=color.red, color=color.new(color.white, 100), size=sz_wall_txt)
var label l_fp = label.new(na, na, "Flip", style=label.style_label_left, textcolor=color.purple, color=color.new(color.white, 100), size=sz_wall_txt)

if not na(plot_c_wall) and not na(plot_p_wall)
    
    // Check for overlap (Combined Label)
    if plot_c_wall == plot_p_wall
        label.set_xy(l_cw, bar_index + 3, plot_c_wall)
        // Clean Combined Format
        string cmb_txt = "COMBINED: " + str.tostring(plot_c_wall) + "\\n" + f_fmt(cw_g) + " / " + f_fmt(pw_g)
        label.set_text(l_cw, cmb_txt)
        label.set_textcolor(l_cw, color.purple)
        
        // Hide separate PW label
        label.set_xy(l_pw, na, na)
    else
        // Separate Labels
        label.set_xy(l_cw, bar_index + 3, plot_c_wall)
        label.set_text(l_cw, f_lbl_txt("CW", plot_c_wall, cw_g, cw_d))
        label.set_textcolor(l_cw, color.green)

        label.set_xy(l_pw, bar_index + 3, plot_p_wall)
        label.set_text(l_pw, f_lbl_txt("PW", plot_p_wall, pw_g, pw_d))
        label.set_textcolor(l_pw, color.red)
else
    // Fallback if data missing
    label.set_xy(l_cw, na, na)
    label.set_xy(l_pw, na, na)

// Flip Label
if not na(plot_flip)
    label.set_xy(l_fp, bar_index + 3, plot_flip)
    label.set_text(l_fp, "Flip: " + str.tostring(plot_flip))
else
    label.set_xy(l_fp, na, na)

// 5. Draw Profile (Triggered only on New Day)
bool new_day = ta.change(time("D")) != 0
if new_day or barstate.isfirst
    f_draw_gex(data_str, max_val)
"""
    return pine_code

# ===============================================
# Watch / Rebuild Loop
# ===============================================
def rebuild():
    files = [f for f in os.listdir('.') if f.endswith('.csv') and "GEX" in f]
    files.sort()
    
    if not files:
        print("⚠️ No GEX CSV files found.")
        return

    pine_code = generate_pinescript(files)
    
    with open(OUTPUT_FILENAME, "w") as f:
        f.write(pine_code)
    
    print(f"✅ UPDATED: {OUTPUT_FILENAME} at {datetime.now().strftime('%H:%M:%S')}")
    
    if CLIPBOARD_AVAILABLE:
        try:
            pyperclip.copy(pine_code)
            print("📋 COPIED: Pine Script code is in your clipboard!")
        except Exception as e:
            print(f"⚠️ Clipboard error: {e}")
    else:
        print("ℹ️  Install 'pyperclip' to enable auto-copy (pip install pyperclip)")

def main_loop():
    print("👀 Monitoring folder for changes... (Press Ctrl+C to stop)")
    last_state = {}
    rebuild()
    
    files = [f for f in os.listdir('.') if f.endswith('.csv') and "GEX" in f]
    for f in files:
        last_state[f] = os.path.getmtime(f)
        
    while True:
        try:
            time.sleep(2)
            current_files = [f for f in os.listdir('.') if f.endswith('.csv') and "GEX" in f]
            current_state = {}
            needs_update = False
            
            for f in current_files:
                mtime = os.path.getmtime(f)
                current_state[f] = mtime
                if f not in last_state or last_state[f] != mtime:
                    print(f"🔄 Detected change in: {f}")
                    needs_update = True
            
            if len(current_files) != len(last_state):
                needs_update = True
                
            if needs_update:
                rebuild()
                last_state = current_state
                
        except KeyboardInterrupt:
            print("\n🛑 Watch mode stopped.")
            break
        except Exception as e:
            print(f"⚠️ Error in watch loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main_loop()
