import requests, math, pandas as pd
from scipy.stats import norm
from datetime import datetime

API_KEY = "kAOv_au6_NbqPynik5gp74t9a8R4zFbz"
SYMBOL  = "AAPL"

url = f"https://api.massive.com/v3/snapshot/options/{SYMBOL}?apiKey={API_KEY}"
r   = requests.get(url)
data = r.json().get("results", [])

rows = []
for opt in data:
    d = opt.get("details", {})
    greeks = opt.get("greeks", {})
    strike = d.get("strike_price")
    oi     = opt.get("open_interest")
    gamma  = greeks.get("gamma")
    under  = opt["underlying_asset"]["price"]
    if all([strike, oi, gamma, under]):
        gex = gamma * oi * 100 * under
        rows.append({"strike": strike, "GEX": gex})

df = pd.DataFrame(rows).sort_values("strike")
df.to_csv(f"{SYMBOL}_GEX.csv", index=False)
print(f"{datetime.now()}  →  Saved {SYMBOL}_GEX.csv ({len(df)} strikes)")
