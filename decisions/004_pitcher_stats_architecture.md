# Pitcher Stats Data Architecture

## Problem

The current `pitcher_game_logs` table stores cumulative season stats computed from game logs up to each game date. This works well for historical modeling but creates two problems for prediction:

1. **Early season noise** — A pitcher with one bad opening day start (e.g. ERA 18.0 from 2 innings) has misleading cumulative stats that corrupt predictions
2. **Unknown pitchers** — If Witt has never faced a pitcher, they have no entry in the database at all

Both cases surfaced on April 3, 2026 when predicting against Chad Patrick. His only DB entry was from his first career start in 2025 (ERA 18), not his actual full season performance (ERA 3.53).

---

## Current Workaround

`predict.py` has a fallback that checks current season sample size and falls back to prior season if fewer than 5 starts exist. If neither season has clean data, league averages are used.

For the Chad Patrick game a separate one-off script (`predict_chad_patrick.py`) was used with manually hardcoded 2025 season stats.

---

## Decision

Add a `fetch_pitcher_season_stats(pitcher_id, season)` function to `data_collection.py` that:

1. Pulls a pitcher's **season-level cumulative stats** directly from the MLB Stats API
2. Stores them in a new `pitcher_season_stats` table (one row per pitcher per season)
3. Is called lazily from `predict.py` — only when a pitcher isn't found or has bad data

This is **lazy loading** — don't pre-populate stats for all 150 MLB starters. Only fetch and cache stats for pitchers Witt is actually scheduled to face.

---

## Architecture

```
predict.py
    │
    ├── needs pitcher stats for tonight
    │
    ├── checks pitcher_season_stats table
    │       ├── found + season sample >= 5 starts → use it
    │       └── not found or bad data →
    │               calls fetch_pitcher_season_stats() from data_collection.py
    │                       └── hits MLB Stats API
    │                       └── stores in pitcher_season_stats
    │                       └── returns clean stats
    │
    └── proceeds with prediction
```

---

## Why Scripts Folder

`data_collection.py` lives in `scripts/` because it's a reusable module — a library of functions that any other script can call. `predict.py` already imports `engine` from it:

```python
sys.path.append("../scripts")
from data_collection import engine
```

The same pattern applies to any new function added to `data_collection.py`. Scripts in the `scripts/` folder are not one-off tools — they're shared modules with single responsibilities that other scripts call when needed.

---

## New Table Schema

```sql
CREATE TABLE IF NOT EXISTS pitcher_season_stats (
    pitcher_id INTEGER NOT NULL,
    pitcher_name TEXT,
    season INTEGER NOT NULL,
    throws TEXT,
    era FLOAT,
    whip FLOAT,
    k_per_9 FLOAT,
    era_vs_rhb FLOAT,
    whip_vs_rhb FLOAT,
    games INTEGER,
    innings_pitched FLOAT,
    UNIQUE (pitcher_id, season)
);
```

---

## Next Steps

- [ ] Add `pitcher_season_stats` table to `create_tables()` in `data_collection.py`
- [ ] Build `fetch_pitcher_season_stats(pitcher_id, season)` function
- [ ] Update `predict.py` to call it lazily when pitcher data is missing or bad
- [ ] Remove manual override scripts once this is in place
