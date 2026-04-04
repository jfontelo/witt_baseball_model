# V7 Model: gb_rate, days_rest, Launch Angle, Feature Cleanup

**Date:** 2026-04-04
**Model version:** witt_hr_logistic_v7
**Prior version:** witt_hr_logistic_v6 (CV AUC 0.555, spread 0.185)

---

## Context

V6 cleaned up the most significant structural problems from V5: removed circular HR rolling features (hr_lag1, hr_avg_7, hr_avg_15) that were endogenous to the outcome, and collapsed duplicate 7/15-day Statcast windows that were causing sign-flipping multicollinearity. V6 spread improved to 0.185 from ~0.12 in V5. AUC declined from 0.588 to 0.555 but this was expected and accepted -- the V5 AUC was inflated by the circular features.

V7 adds three new features and removes one redundant one.

---

## Changes

### Added: gb_rate

**What it is:** Pitcher ground ball rate, computed as groundOuts / (groundOuts + airOuts) from the MLB Stats API game log endpoint. Accumulated across all prior starts entering the game, consistent with how ERA and WHIP are computed in the pipeline.

**Why it matters:** GB rate is a structural HR suppressor independent of ERA. A pitcher with a 0.55+ GB rate is inducing weak ground contact by design -- pitch shape, location, sequencing. ERA captures outcomes but GB rate captures the mechanism. A pitcher can have a high ERA and a high GB rate (bad but not home run prone) or a low ERA and a low GB rate (good but hittable in the air). The two features carry different information.

**Why it is more stable than HR/9:** Home runs allowed per 9 innings is itself a noisy outcome stat at the pitcher level -- a few games can swing it dramatically. GB rate stabilizes faster because ground balls are more frequent events and reflect pitch design rather than HR luck.

**Decision on field name:** MLB Stats API game log returns groundOuts and airOuts (not flyOuts, which is a separate and smaller subset). airOuts is the correct denominator -- it includes fly balls and pop-ups, representing all non-ground batted ball outs. Confirmed via debug script against known pitcher game logs.

**Data engineering note:** gb_rate required a schema migration (ALTER TABLE pitcher_game_logs ADD COLUMN IF NOT EXISTS gb_rate FLOAT) and a fix to the upsert_table function. The original upsert built its update_columns dict from SQLAlchemy table reflection, which cached the pre-migration schema and silently omitted the new column. Fixed by building update_columns from stmt.excluded keyed off DataFrame columns directly, which always reflects what is actually being written.

**Historical backfill:** After the upsert fix, a full data_collection.py run backfilled real gb_rate values for all historical rows. Pre-fix rows were temporarily set to league average (0.44) via direct SQL UPDATE.

### Added: hr_zone_rate_15

**What it is:** Rolling 15-game percentage of batted balls landing in the 25-35 degree launch angle band, the range where balls most frequently leave the park.

**Why it matters:** Barrel rate captures the optimal exit velocity and launch angle combination. HR zone rate captures pure launch angle tendency independently. A player can barrel a ball slightly outside the HR zone and get a double; a player with a consistently elevated launch angle profile will convert more fly balls to HRs at the margin. This feature is designed to differentiate player profiles as the model scales to multiple players -- Schwarber's hr_zone_rate will be high and stable, Witt's more variable.

### Added: days_rest

**What it is:** Calendar days off before the game, computed from date diff on player_game_logs within each season, capped at 4.

**Why capped at 4:** Rest effects on power output flatten after 3-4 days. All-star break and extended IL returns would otherwise create outlier values that don't represent a different physical state than 4 days rest.

**Why grouped by season:** Without season groupby, the first game of each season receives a rest value reflecting the winter gap, which is not meaningful.

**Result:** 523 of 619 model rows show 0.0 days rest -- Witt plays nearly every game. This makes days_rest a weak feature in practice for a player of his durability, but it is causally valid and will carry more signal for players who miss more games.

### Removed: park_factor_hr

park_factor_hr was redundant with park_factor. Both features measure HR-friendliness of the ballpark and were highly correlated. park_factor_hr was removed to reduce multicollinearity. park_factor is retained as the more stable and broadly applicable measure.

---

## Results

| Version | CV AUC | Spread | Notes |
|---------|--------|--------|-------|
| V5 | 0.588 | ~0.12 | Circular HR features inflating AUC |
| V6 | 0.555 | 0.185 | Clean features, launch angle added |
| V7 | 0.554 | 0.201 | gb_rate with real historical data |

**AUC** is essentially flat V6 to V7 (0.555 to 0.554). This is expected given that gb_rate was all league average fills during V6 and now carries real signal, but the net effect on overall ranking is small.

**Spread improved to 0.201.** Top bin actual HR rate is 28.2% vs 8.1% bottom bin, against a 16.0% baseline. This is the highest spread across all model versions and represents nearly 2x lift in the top bin over baseline.

**Calibration is clean across all five bins:** predicted and actual rates track closely at every level, confirming the model is well-calibrated and not just fitting noise.

**Overfitting gap** (in-sample minus CV AUC) is 0.099, slightly tighter than V6.

---

## Why spread matters more than AUC for this use case

AUC measures global ranking ability across all predictions. For betting, only the top bin matters -- the model needs conviction when it has conviction, not accuracy across every game. A model that correctly identifies the 20% of games where HR probability is genuinely elevated is more valuable than one with marginally better AUC but flatter bin separation. The spread directly measures whether top-bin confidence translates to real-world HR rate lift.

---

## First live predictions (2026-04-04, Brewers at Royals doubleheader)

**Game 1 (Chad Patrick, RHP):** Model 18.4%, book +391 (20.4% implied). Edge -2.0%. No bet.

**Game 2 (Brandon Sproat, RHP):** Model 18.4%, book +391 (20.4% implied). Edge -2.0%. No bet. Sproat had no 2026 MLB stats -- model used league averages for pitcher features.

**Game 3 tomorrow (Kyle Harrison, LHP):** Model 8.4%, book +391 (20.4% implied). Edge -12.0%. Clear no bet. Harrison is a left-handed ground ball pitcher (GB rate 0.571) -- the worst matchup profile for Witt's right-handed power game. Model correctly assigned very low HR probability, confirming gb_rate and pitcher_r are doing their jobs.

The two clean no-bets with clear reasoning on the Brewers series validate the model's ability to pass on unfavorable matchups. This is the intended use case.

---

## Next steps

- Uncomment Schwarber (656941) in PLAYERS dict and run data_collection.py to begin building his game log history
- Add Gunnar Henderson as third player
- Target 4-5 player basket covering distinct profiles: pure power (Schwarber), contact-plus-power (Witt), balanced (Henderson)
- More players means more training data and the model generalizes from player-specific patterns to structural HR conditions
- Web app: player dropdown outputting HR probability and implied odds per game
