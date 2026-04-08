# Bobby Witt Jr. HR Prop Model

A machine learning system for predicting Bobby Witt Jr.'s home run probability in MLB games, designed to identify value against sportsbook prop lines.

---

## The Idea

Sportsbooks price player props based on broad market signals. For a small-market player like Witt, those lines may be less efficiently priced than for nationally followed stars. This model uses Statcast contact quality metrics -- exit velocity, barrel rate, hard hit rate -- combined with pitcher and park factors to generate an independent probability estimate. When that estimate diverges meaningfully from the sportsbook's implied probability, there may be edge.

---

## Current Model

**V7 -- Logistic Regression, HR Binary Classifier**

- **Target:** Did Witt hit at least one HR? (binary 0/1)
- **CV AUC: 0.554** -- meaningful signal above the 0.55 threshold
- **Top bin actual HR rate: 25.8%** vs bottom bin 7.3% -- real discrimination
- **Calibration spread: 0.185** -- held flat from V6

When the model places a game in the top bin, the sportsbook needs to offer better than +288 for edge to exist.

---

## How It Works

### Data Pipeline

Game logs, pitcher stats, and park factors are pulled from the MLB Stats API and stored in PostgreSQL. Statcast pitch-level data (exit velocity, launch angle, barrel classification) is pulled via pybaseball and aggregated to game level. Pitcher season stats are fetched lazily -- only when a pitcher is scheduled to face Witt and not yet cached.

### Features

- **Witt contact quality** -- rolling 7 and 15-game averages of exit velocity, barrel rate, hard hit rate
- **Witt HR form** -- rolling HR rate and lag features
- **Pitcher stats** -- season ERA/WHIP/K9, last 5 starts, vs RHB splits
- **Bullpen quality** -- opponent team bullpen ERA/WHIP
- **Park factors** -- overall park factor and HR-specific park factor
- **Game context** -- home/away, pitcher handedness

### Model

Logistic regression with StandardScaler preprocessing and TimeSeriesSplit cross-validation -- training always uses past data to predict future games, never the reverse.

---

## What We Tried

| Version | Approach | CV Metric | Result |
|---------|----------|-----------|--------|
| V1 | XGBoost Regressor (TB) | MAE 1.624 | Worse than baseline, severe overfitting |
| V2 | Poisson Regression (TB) | MAE 1.502 | Marginal improvement, no overfitting |
| V3 | Poisson + enriched pitcher features | MAE 1.504 | Stable, well-calibrated, weak discriminating power |
| V4 | Poisson + Statcast (TB) | MAE 1.510 | Statcast did not add signal for TB |
| V5 | Logistic Regression (HR binary) | AUC 0.588 | First meaningful signal -- established HR as correct target |
| V6 | Logistic + cleaner feature set | AUC 0.555 | Removed circular HR rolling features; calibration spread improved to 0.185 |
| **V7** | **Logistic + gb_rate + days_rest** | **AUC 0.554** | **Added pitcher GB rate and Witt rest days; spread held at 0.185; current model** |

Key insight: TB is too noisy -- it accumulates across multiple at bats. HR is a single binary event where barrel rate is directly predictive. Switching the target variable was more impactful than any feature engineering change.

---

## Operational Use

The daily prediction workflow:

1. Run `data_collection.py` to update game logs and pitcher stats
2. Run `predict.py` with tonight's confirmed pitcher and park
3. Model outputs P(HR) and equivalent American odds
4. Compare against sportsbook line -- if model probability implies better odds than posted, edge may exist
5. Bet or pass

---

## Project Structure

```
witt_baseball_model/
├── notebooks/          # Exploratory analysis and model development
├── scripts/
│   ├── config.py              # DB credentials via .env
│   ├── data_collection.py     # MLB Stats API + Statcast ingestion
│   ├── model_training.py      # Feature engineering + logistic regression
│   └── predict.py             # Daily prediction script
├── models/
│   ├── witt_hr_logistic_model.pkl
│   └── witt_hr_logistic_scaler.pkl
├── decisions/          # Architecture decision records
├── requirements.txt
└── .env                # Not committed -- DATABASE_URL goes here
```

---

## Database Schema

Three tables in PostgreSQL (hosted on Render):

- `witt_game_logs` -- Witt game-by-game stats with Statcast features
- `pitcher_game_logs` -- Cumulative pitcher stats up to each game (no leakage)
- `park_factors` -- Park factor and HR park factor by venue
- `pitcher_season_stats` -- Lazily loaded season stats per pitcher (cached on first lookup)

---

## Setup

### 1. Clone the repo
```bash
git clone https://github.com/jfontelo/witt_baseball_model.git
cd witt_baseball_model
```

### 2. Create a virtual environment
```bash
python -m venv venv
source venv/bin/activate  # Mac/Linux
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file in the root directory:
```
DATABASE_URL=your_postgresql_connection_string
```

### 5. Run the pipeline
```bash
python scripts/data_collection.py    # Fetch and store game logs
python scripts/model_training.py     # Train the model
python scripts/predict.py            # Generate tonight's prediction
```

---

## Roadmap

### Phase 1 -- Core Model (done)
- [x] Data pipeline: game logs, pitcher stats, park factors
- [x] Statcast integration via pybaseball
- [x] V5 logistic regression HR classifier (AUC 0.587)
- [x] Daily prediction script (`predict.py`)

### Phase 2 -- Expand and Validate (next)
- [ ] Backtest: simulate betting on top-bin games historically, calculate ROI vs break-even
- [ ] Add Kyle Schwarber as second player
- [ ] Add days rest as a feature
- [ ] Lazy pitcher stat fetching for any MLB starter

### Phase 3 -- Web App
- [ ] Player dropdown (starting with Witt and Schwarber)
- [ ] Inputs: opposing pitcher, home/away
- [ ] Output: P(HR), equivalent American odds, edge vs current sportsbook line
- [ ] Instrument user flow as a second portfolio piece

### Phase 4 -- AI Summary
- [ ] Post-game AI summary via Claude API comparing model prediction to actual result

---

## Limitations and Honest Caveats

- **Small sample size:** Witt has 3 MLB seasons. More data means a better model.
- **Model edge is small:** Even a well-calibrated model won't win every bet. A 55% win rate on +EV bets is excellent.
- **Line movement:** Sportsbook lines shift. This model is a pre-game signal, not a live trading tool.
- **No guarantee:** This is a probabilistic tool, not a betting advisor.

---

## Data Sources

- [MLB Stats API](https://statsapi.mlb.com) -- Game logs, player stats, pitcher data
- [Baseball Savant](https://baseballsavant.mlb.com) -- Park factors and Statcast data (via pybaseball)

---

## About

Built by a data and analytics professional as a portfolio project at the intersection of sports analytics, data engineering, and probabilistic modeling. The goal is a rigorous, iterative system -- a framework for thinking clearly about probability and market pricing, not a gambling tool.
