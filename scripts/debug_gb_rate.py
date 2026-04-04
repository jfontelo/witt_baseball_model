### debug_gb_rate.py — inspect MLB Stats API pitcher game log fields
### Run from your project root: python debug_gb_rate.py

import requests

# Shane Bieber — first pitcher in your table
PITCHER_ID = 669456
SEASON = 2023

url = (
    f"https://statsapi.mlb.com/api/v1/people/{PITCHER_ID}/stats"
    f"?stats=gameLog&group=pitching&season={SEASON}"
)

response = requests.get(url)
data = response.json()
splits = data.get("stats", [{}])[0].get("splits", [])

if not splits:
    print("No splits returned")
else:
    first = splits[0].get("stat", {})
    print(f"Available stat keys ({len(first)} total):")
    for k, v in sorted(first.items()):
        print(f"  {k}: {v}")
