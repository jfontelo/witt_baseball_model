# Decision 002 — Data Pipeline Architecture

**Date:** April 1, 2026
**Status:** Decided

---

## Context

After locking the V1 methodology in Decision 001, we refactored `data_collection.py` to align with it. Several key decisions were made about how to collect, store, and compute the features the model will use.

---

## Decisions Made

### 1. Remove opponent team stats entirely

**Decision:** Removed `royals_game_logs`, `opponent_offense_game_logs`, and `opponent_defense_game_logs` tables and their fetch functions.

**Reasoning:** Opponent team-level offense and defense stats have no causal relationship to Witt's individual TB output. What matters is the specific starting pitcher Witt faces, not how the opposing team hits as a unit. Including team stats would add noise and complexity with no predictive benefit.

---

### 2. Replace opponent stats with pitcher game logs

**Decision:** Added `pitcher_game_logs` table storing the opposing starting pitcher's cumulative ERA, WHIP, K/9, and handedness (R/L) entering each game.

**Reasoning:** The starting pitcher is the primary determinant of offensive output for any given game. ERA, WHIP, and K/9 are well-established, always-available signals of pitcher quality. Handedness is included because platoon splits are one of the most consistent and meaningful splits in baseball.

**Implementation:** For each game in Witt's log, the script fetches the boxscore to identify the opposing starter, then fetches that pitcher's game-by-game pitching log for the season.

---

### 3. Use cumulative-to-date pitcher stats, not full-season stats

**Decision:** Pitcher ERA, WHIP, and K/9 are computed from games pitched **before** the date of each Witt game, not from the full season summary.

**Reasoning:** Using full-season stats would introduce data leakage — the model would be trained on information that didn't exist at game time. Cumulative-to-date stats represent exactly what was knowable before first pitch, which is also what will be available when making real predictions. This makes the training pipeline and the prediction pipeline use identical logic.

**Fallback:** Pitchers with zero prior appearances (Opening Day starters, true debuts) fall back to league averages (ERA 4.20, WHIP 1.30, K/9 8.8) rather than being excluded.

---

### 4. Add park factors with hit-type breakdown

**Decision:** Added a `park_factors` table with five columns per park: overall park factor, plus individual factors for 1B, 2B, 3B, and HR.

**Reasoning:** Since we are predicting Total Bases specifically, the distribution of hit types matters more than just the overall run environment. A single = 1 TB, double = 2, triple = 3, homer = 4. A park that suppresses home runs but elevates doubles (like Fenway) has a very different effect on TB than a park that boosts everything uniformly. Breaking out the factors by hit type gives the model richer, more accurate input.

**Source:** Baseball Savant Statcast Park Factors, 2025 single-year data. Keyed to MLB team IDs already present in `witt_game_logs`.

**Notable findings:**
- Kauffman Stadium has a triples factor of 212 — highly relevant for Witt who is an above-average triples hitter
- T-Mobile Park (Mariners) has a HR factor of 31 — extreme suppression
- Chase Field (D-backs) has a HR factor of 218 — extreme elevation

---

### 5. Park factor applied based on home/away

**Decision:** When Witt plays at home, Kauffman's factors are used. When Witt plays away, the opposing team's park factors are used.

**Reasoning:** This is the correct directional logic — the park factor reflects the environment the game is played in, regardless of who's batting.

---

## What Was Explicitly Ruled Out

| Feature | Reason |
|---|---|
| wOBAcon, xwOBAcon, BACON | Derived metrics — redundant with raw hit type factors |
| Park factor for BB and SO | Indirect relationship to TB, adds complexity |
| Multi-year rolling park factors | 2025 single-year used for simplicity; revisit if model shows instability |

---

## Updated V1 Feature Set

| Feature | Source | Table |
|---|---|---|
| Witt's TB per game (last N games) | MLB Stats API | witt_game_logs |
| Opposing pitcher ERA (cumulative to date) | MLB Stats API | pitcher_game_logs |
| Opposing pitcher WHIP (cumulative to date) | MLB Stats API | pitcher_game_logs |
| Opposing pitcher K/9 (cumulative to date) | MLB Stats API | pitcher_game_logs |
| Pitcher handedness (R/L) | MLB Stats API | pitcher_game_logs |
| Home or Away | MLB Stats API | witt_game_logs |
| Park factor (overall) | Baseball Savant 2025 | park_factors |
| Park factor 1B | Baseball Savant 2025 | park_factors |
| Park factor 2B | Baseball Savant 2025 | park_factors |
| Park factor 3B | Baseball Savant 2025 | park_factors |
| Park factor HR | Baseball Savant 2025 | park_factors |
