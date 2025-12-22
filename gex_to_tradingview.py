# =========================================================
#  GEX Master Auto Generator (Final Stable Build)
#  Generates Pine Script v6 indicators for TradingView
#  Author: Code GPT (custom PulsR edition)
# =========================================================

import os
from datetime import datetime

# --- CONFIG ---
OUTPUT_FILE = "GEX_Master_Auto.pine"
TITLE = "GEX Master Auto"
VERSION = 6

# --- DATA LOADING PLACEHOLDER ---
# (Assumes you have your ticker-specific strike/gamma data arrays defined elsewhere)
def generate_data_block():
    # Placeholder – your real data loader writes these
    return """
    if sym == "RR"
        s := array.from(1, 1, 2, 2, 3, 3, 4, 4)
        g := array.from(0, 0, 341, 2766, 77254, 175022, 136026, 433208)
    if sym == "MSTU"
        s := array.from(1, 1, 2, 2, 3, 3, 4, 4)
        g := array.from(0, 0, 0, 0, 178775, 40517, 6442, 794)
    """

# --- GENERATE PINE SCRIPT ---
def generate_pine():
    today = datetime.now().strftime("%Y%m%d")
    pine = f"""//@version={VERSION}
indicator("{TITLE}: {today}", overlay=true, max_lines_count=500, max_labels_count=500)

// === SETTINGS ===
var float width_scale = input.float(0.5, "Bar Width Scale", minval=0.1, step=0.1)
var float text_size_threshold = input.float(100000000, "Text Label Threshold (Notional)")
var bool show_dashboard = input.bool(true, "Show Dashboard")

// === DATA LOADING ===
load_data() =>
    string sym = syminfo.ticker
    float[] s = array.new_float(0)
    float[] g = array.new_float(0)
{generate_data_block()}
    [s, g]

// === MAIN LOGIC ===
var bool gex_drawn = false
var line[] gex_lines = array.new_line()
var label[] gex_labels = array.new_label()

if barstate.islast and not gex_drawn
    [strikes, gex_vals] = load_data()
    if array.size(strikes) > 0
        float total_gex = 0.0
        for i = 0 to array.size(gex_vals) - 1
            total_gex += array.get(gex_vals, i)

        // delete any old drawings
        for l in gex_lines
            line.delete(l)
        for lb in gex_labels
            label.delete(lb)
        array.clear(gex_lines)
        array.clear(gex_labels)

        // create persistent, price-locked gamma levels
        for i = 0 to array.size(strikes) - 1
            s_price = array.get(strikes, i)
            g_val = array.get(gex_vals, i)
            color g_col = g_val > 0 ? color.new(color.green, 0) : color.new(color.red, 0)

            // horizontal support/resistance-style line
            l = line.new(
                x1=bar_index - 2000,
                y1=s_price,
                x2=bar_index + 2000,
                y2=s_price,
                xloc=xloc.bar_index,
                extend=extend.right,
                color=g_col,
                width=2)
            array.push(gex_lines, l)

            // label at each strike
            lb = label.new(
                x=bar_index,
                y=s_price,
                text="Strike " + str.tostring(s_price) + "\\n" + str.tostring(math.round(g_val / 1e6)) + "M",
                xloc=xloc.bar_index,
                yloc=yloc.price,
                style=label.style_label_left,
                textcolor=color.white,
                color=color.new(g_col, 60),
                size=size.small)
            array.push(gex_labels, lb)

        // === Dashboard ===
        if show_dashboard
            string regime = total_gex > 0 ? "Short Vol (Pos GEX)" : "Long Vol (Neg GEX)"
            color reg_col = total_gex > 0 ? color.green : color.red
            var table dash = table.new(position.top_right, 1, 1)
            table.cell(dash, 0, 0,
                syminfo.ticker + " GEX: " + str.tostring(math.round(total_gex / 1e6)) +
                "M\\n" + regime + "\\nDate: {today}",
                text_color=color.white, bgcolor=color.new(reg_col, 80))

        gex_drawn := true
    else
        var table err = table.new(position.bottom_right, 1, 1)
        table.cell(err, 0, 0, "No GEX Data for " + syminfo.ticker,
                  text_color=color.white, bgcolor=color.gray)
"""
    return pine

# --- WRITE FILE ---
if __name__ == "__main__":
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(generate_pine())
    print(f"[✓] Generated {OUTPUT_FILE} successfully.")
