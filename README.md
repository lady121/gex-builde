🚀 MarketData GEX Automation Suite

Automated Gamma Exposure (GEX) Analysis & TradingView Integration

This repository hosts a fully automated system that calculates Daily Gamma Exposure (GEX) levels, generates visual charts, and compiles a ready-to-use Pine Script indicator for TradingView.

🔄 System Overview (The Workflow)

The system operates on a Twice-Daily Schedule (1:00 AM UTC & 1:00 PM UTC) via GitHub Actions.

Step 1: The Builder runs first. It fetches data, calculates levels, and generates the CSV and PNG files.

Step 2: The Converter runs automatically after the Builder. It compiles all data into a single Pine Script file.

📂 The Output Files (What You Will See)

Every time the system runs, it produces the following files in your repository. Here is how to use them:

1. 🖼️ Visual Charts (.png)

File Pattern: {Ticker}_GEX_robust_{Date}.png (e.g., SPY_GEX_robust_20251226.png)

These are for a quick visual check before you trade. You don't need to open any data files; just look at the image.

How to Read the Chart Title:
The title bar at the top of the image contains the most critical trading levels:

Spot: The price of the stock when the data was processed.

Flip: The precise Flip Zone price.

Market Regimes (Important):
If the market is extremely one-sided, the Flip Zone might disappear. The chart title will tell you why:

Normal: Spot: $450.00 | Flip: $455.00 -> Trade the Flip level as a Pivot.

Bullish Regime: Spot: $450.00 | Flip: ALL_CALLS -> No Put Gamma exists. The market is in a "Trend Up" mode.

Bearish Regime: Spot: $450.00 | Flip: ALL_PUTS -> No Call Gamma exists. The market is in a "Trend Down" mode.

2. 📊 The Master Summary (gamma_summary.csv)

File Name: gamma_summary.csv

This is a spreadsheet-ready file containing a snapshot of all tickers configured in your system. It is overwritten on every run to show the latest data.

Columns:

Ticker: Symbol (e.g., SPY, NVDA).

Spot Price: Current price.

Flip Zone: The pivot level (or "ALL_CALLS"/"ALL_PUTS").

Call Wall: Major Resistance level.

Put Wall: Major Support level.

Net GEX ($B): Total Net Gamma in Billions.

Regime: Normal, All Calls, or All Puts.

3. 🌲 TradingView Indicator (.pine)

File Name: Universal_GEX_History.pine

This is the final product. It contains the logic to plot these levels directly on your TradingView charts.

Features:

Version 6: Uses the latest Pine Script standards.

Historical Replay: The script stores data by date. If you use TradingView's "Bar Replay" mode, the lines will adjust to show what the GEX levels were on that specific day.

Visual Strength: The histogram lines on the right side of the chart change length based on how much Gamma is at that strike (Longer Line = Stronger Level).

Smart Labels:

Green Line: Call Wall (Resistance).

Red Line: Put Wall (Support).

Purple Label: "COMBINED WALL" (If Put and Call walls are at the same strike).

Blue Line: Flip Zone (Hidden automatically if the regime is "ALL CALLS" or "ALL PUTS").

🚀 How to Use the Pine Script

Open the file Universal_GEX_History.pine in this repository.

Copy all the code (Ctrl+A, Ctrl+C).

Open TradingView.

At the bottom, open the Pine Editor.

Paste the code and click "Add to Chart".

Note: The levels update automatically based on the Ticker you are viewing (e.g., if you switch from SPY to QQQ, the lines update instantly).

⚙️ Configuration

Adding New Stocks

To analyze different stocks, edit the tickers.txt file in the root of the repository:

Open tickers.txt.

Add one symbol per line (e.g., TSLA, MSFT, AMZN).

Commit the changes. The next run will include these stocks.

Manual Trigger

If you don't want to wait for the scheduled run:

Go to the "Actions" tab in GitHub.

Select "Build GEX Twice Daily".

Click "Run workflow".

⚠️ Troubleshooting

Q: Why is the Flip Zone missing on my TradingView chart?
A: This is intentional. If the CSV data reported "ALL_CALLS" or "ALL_PUTS" (an extreme regime), the script hides the blue line to prevent false signals. Check the PNG title to confirm the regime.

Q: Why are the lines "stepped"?
A: The indicator plots daily levels. The lines remain flat for the entire trading day and only move when the new daily data is loaded.

Q: I don't see the histogram bars.
A: The histogram profile is only drawn on the Last Bar (the current live candle) to keep the chart clean.
