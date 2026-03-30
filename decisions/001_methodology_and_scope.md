# Decision 001 — Methodology & V1 Scope

**Date:** March 30, 2026
**Status:** Decided

---

## Context

This project aims to find +EV betting opportunities on DraftKings by predicting Bobby Witt Jr.'s Total Bases (TB) and Stolen Bases (SB) in a given game. Before writing any model code, we needed to define the methodology and scope clearly to avoid building on a bad foundation.

---

## Decisions Made

### 1. Use Poisson Regression for the model

**Decision:** Use Poisson regression, not a simple average or linear regression.

**Reasoning:** TB and SB are count outcomes — non-negative integers. Poisson regression is specifically designed for this type of data and outputs a full probability distribution (P(TB=0), P(TB=1), P(TB=2)...) rather than just a single predicted number. That distribution is exactly what we need to compare against DraftKings' implied probabilities.

---

### 2. Use pitcher-level stats instead of Witt vs. specific pitcher matchups

**Decision:** Model inputs will use the opposing starter's aggregate stats (ERA, WHIP, handedness) rather than Witt's historical stats against that specific pitcher.

**Reasoning:** Witt has been in MLB since 2022. Many individual matchup samples are 2–5 at-bats — statistically meaningless. A model built on tiny samples would be unreliable and potentially misleading. Pitcher-level aggregate stats are always available, always meaningful, and more generalizable.

---

### 3. Drop opponent team offensive stats from the model

**Decision:** Remove `opponent_offense_game_logs` from the model feature set.

**Reasoning:** How well the opposing team hits as a whole has no direct causal relationship to Witt's TB or SB in a given game. This data was collected in the original pipeline but adds noise without signal. Keeping the feature set lean and causally justified is more important than having more features.

---

### 4. Include ballpark factor as a feature

**Decision:** Include a static ballpark factor lookup table as a model input.

**Reasoning:** Park effects on offense are well-documented and statistically significant. Coors Field vs. Petco Park represents a real difference in expected offensive output. This is a static table (doesn't require API calls) and is easy to maintain. High signal, low cost.

---

### 5. Include RHP/LHP splits

**Decision:** Encode pitcher handedness as a binary feature.

**Reasoning:** Platoon splits (performance vs. right-handed vs. left-handed pitchers) are one of the most consistent and meaningful splits in baseball. Witt's splits are significant enough to include. This is a single data point per game and always available from the MLB API.

---

## V1 Feature Set (Locked)

| Feature | Source |
|---|---|
| Witt's TB average (last 10–15 games) | PostgreSQL DB |
| Tonight's starting pitcher ERA | MLB Stats API |
| Tonight's starting pitcher WHIP | MLB Stats API |
| Pitcher handedness (RHP/LHP) | MLB Stats API |
| Home or Away | PostgreSQL DB |
| Ballpark factor | Static lookup table |

---

## What We Explicitly Ruled Out of V1

| Feature | Reason deferred |
|---|---|
| Witt vs. specific pitcher history | Sample too small |
| Opponent team offense stats | No causal relationship to Witt's output |
| Weather | Adds complexity, revisit in V2 |
| Pitcher recent form (last 3 starts) | V2 — good signal but adds complexity |
| Witt rest/days off | V2 |

---

## How We Measure Success

The model is useful if it identifies lines where our predicted probability meaningfully differs from DraftKings' implied probability. A well-calibrated model that wins 53–55% of +EV bets over a large sample is considered successful. We are not optimizing for perfect prediction — we are optimizing for edge over the market.
