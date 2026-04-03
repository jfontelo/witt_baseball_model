# Model Approach Decision Record

## Summary
After two sessions of modeling, we landed on a working HR binary classifier with meaningful predictive signal (AUC 0.587). This document records the approaches tried, what we learned, and why we made each decision.

---

## Session 1 — TB Regression

### What we tried
XGBoost regressor predicting exact total bases (TB) per game.

### Results
- CV MAE: 1.624 vs baseline 1.511 — **worse than baseline**
- Overfitting gap: 0.849 — severe memorization of training data

### Decision
Abandoned. XGBoost with default parameters was overfitting badly on 619 rows. Switched to Poisson regression as a more statistically appropriate model for count data.

---

## Session 1 — TB Poisson Regression (V2, V3)

### What we tried
Poisson regression predicting expected TB rate (lambda), then deriving over/under probabilities analytically. Added pitcher recent form (era_last5, whip_last5), vs RHB splits (era_vs_rhb), and bullpen stats in V3.

### Results
- CV MAE: 1.502–1.504 vs baseline 1.511 — **marginal improvement**
- Overfitting gap: ~0.001 — no overfitting
- Calibration: mean predicted probability matched actual rate (0.507 = 0.507) ✅
- Discriminating power: spread of 0.234 between top and bottom bins

### Decision
Model is correctly specified and calibrated but lacks discriminating power. Single game TB is too noisy — the model knows the average but can't reliably identify which specific nights are above or below it. Usable as a base rate framework but not strongly predictive on its own.

---

## Session 2 — TB Poisson + Statcast (V4)

### What we tried
Added pybaseball Statcast features: rolling avg exit velocity (7 and 15 game), barrel rate (7 and 15 game), hard hit rate (7 game).

### Results
- CV MAE: 1.510 vs V3's 1.504 — **slightly worse**
- Spread: 0.218 vs V3's 0.234 — **slightly worse**

### Decision
Statcast features didn't add signal for TB prediction. Exit velocity and barrel rate are correlated with TB outcomes but not independently predictive beyond what pitcher stats and park factors already capture. The fundamental problem is TB accumulates across multiple at bats — too many random events summing together.

---

## Session 2 — HR Logistic Regression (V5) ✅ CURRENT BEST MODEL

### What we tried
Switched target to binary HR (did Witt homer yes/no). Used logistic regression — purpose built for binary outcomes, no Poisson intermediate step needed. Same Statcast features plus rolling HR form features.

### Results
- CV AUC: **0.587 ± 0.043** — meaningful signal (>0.55 threshold)
- Baseline accuracy: 0.840 (always predict no HR)
- Model accuracy: 0.827 — slightly below baseline (expected, accuracy is misleading for imbalanced classes)
- Calibration: top bin predicted 28.4%, actual rate 28.2% — **near perfect**
- Spread: 0.193 between top and bottom bins

### Why this works better
- HR is a binary outcome — logistic regression is the correct model
- Barrel rate directly predicts HR — a barreled ball becomes a HR ~50% of the time
- The signal that was diffuse across TB is concentrated and detectable for HR
- Less noise accumulation — one event vs sum of multiple at bats

### Operational use
When the model places a game in the top bin (predicted ~28% HR probability):
- Break-even odds: +253
- If sportbook offers +300 or better → edge exists → consider betting

---

## Key Learnings

1. **Problem framing matters more than model choice.** TB regression vs HR binary classifier is a bigger decision than XGBoost vs Poisson.

2. **More features ≠ better model.** Statcast features added noise for TB, signal for HR. The relationship between barrel rate and HR is direct; between barrel rate and TB it's diffuse.

3. **AUC > accuracy for imbalanced targets.** Accuracy of 82.7% sounds bad vs baseline 84%, but AUC of 0.587 shows real discriminating power.

4. **Calibration matters for betting.** A model that says 28% and hits at 28.2% is operationally trustworthy. The TB model was saying 59% when reality was 50.7%.

5. **Small market players have softer prop lines.** Witt in Kansas City is less efficiently priced than Ohtani or Freeman. The edge opportunity is in the market, not just the model.

---

## Next Steps

- [ ] Build prediction script (`predict.py`) — inputs tonight's game info, outputs HR probability and implied odds
- [ ] Add Schwarber as second player — all-or-nothing profile, stronger platoon splits, Citizens Bank Park HR factor
- [ ] Backtest: simulate betting on top bin games historically, calculate ROI vs break-even
- [ ] Add days rest feature
- [ ] Consider career vs pitcher splits once sample size allows
