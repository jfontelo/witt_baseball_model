# ⚾ Bobby Witt Jr. - Total Bases Prediction Model

A data engineering and predictive analytics project designed to find **+EV (positive expected value) betting opportunities** on DraftKings by modeling the probability distribution of Bobby Witt Jr.'s Total Bases (TB) in any given game.

> This is V1 of a broader **player props modeling framework** - designed from the ground up to be extensible to other props (stolen bases, hits) and other players.

---

## 🧠 Problem Statement

Sportsbooks like DraftKings price player prop lines based on their own models and market movement. Those lines aren't always accurate - and when they're off, there's a real edge to exploit.

This project asks a simple question:

> **Does DraftKings' implied probability for Bobby Witt Jr.'s TB match what the data actually suggests?**

If our model says there's a 52% chance Witt hits 3+ TB, and DK is pricing that at +122 (implying ~45%), that's a quantifiable edge worth betting.

---

## 🎯 Why Bobby Witt Jr.?

Witt is a rare **5-tool player** - and 3 of those 5 tools directly affect his Total Bases distribution:

- **Hit for average** → singles, staying alive at the plate
- **Hit for power** → doubles and home runs
- **Speed** → turns outs into singles, singles into doubles, singles into triples

This creates a rich, spread-out probability distribution that is harder for sportsbooks to price accurately than a pure power hitter like Giancarlo Stanton (who is essentially HR or nothing). The more nuanced the distribution, the more opportunity for mispricing.

Additionally, as a small-market player, Witt's prop lines receive less sharp betting action than players on marquee teams - meaning lines may sit mispriced longer.

---

## 🎯 Approach

Rather than predicting a single outcome ("he'll get 3 TB tonight"), we model a **full probability distribution** across all possible outcomes using Poisson regression - a statistical method well-suited for count-based outcomes like total bases.

```
TB = 0  →  8%
TB = 1  → 18%
TB = 2  → 25%
TB = 3  → 24%   ← cumulative 3+ = 49%
TB = 4  → 15%
TB = 5+ → 10%
```

We then compare those probabilities directly to DraftKings' implied odds to identify mispriced lines.

---

## 📐 Model Features (V1)

The model is intentionally kept lean for V1 - fewer, high-signal features beat a noisy model with many weak ones.

| Feature | Rationale |
|---|---|
| Witt's TB average (last 10–15 games) | Captures current form |
| Tonight's starting pitcher ERA | Overall quality of opposing arm |
| Tonight's starting pitcher WHIP | Measures how hittable the pitcher is |
| Tonight's starting pitcher K/9 | High K/9 pitchers shift Witt's distribution toward more polarized outcomes |
| Pitcher handedness (RHP/LHP) | Witt has meaningful platoon splits |
| Home or Away | Witt's home/away performance differs |
| Ballpark factor | Park-adjusted run environment |

> **Why not Witt vs. specific pitcher matchups?** Witt has been in the league since 2022. Individual matchup samples (sometimes 2–3 AB) are too small to be statistically meaningful. Pitcher-level aggregate stats are more reliable and always available.

> **Why K/9 matters:** A strikeout artist forces Witt to keep the barrel in the zone longer, making outcomes more polarized - more strikeouts but also more hard contact when he connects. A contact/ground ball pitcher allows Witt to use the whole field, where his speed creates singles and doubles. The pitcher type fundamentally shifts the shape of the distribution, not just the mean.

---

## 🗂️ Project Structure

```
witt_baseball_model/
│
├── config.py                  # Loads DB credentials from .env
├── database_setup.py          # PostgreSQL connection via SQLAlchemy
├── data_collection.py         # Fetches game logs from MLB Stats API → PostgreSQL
├── model_training.py          # Trains Poisson regression model on historical data
├── predictions.py             # Outputs TB probability distribution for a given game
│
├── decisions/                 # Decision log - methodology and scoping choices
├── data/                      # Raw and processed data (local only)
├── models/                    # Serialized trained models
├── notebooks/                 # Exploratory analysis and validation
└── README.md
```

---

## 🏗️ Architecture

```
MLB Stats API
     │
     ▼
data_collection.py  ──►  PostgreSQL (Render)
                               │
                               ▼
                      model_training.py  ──►  Poisson Model
                                                    │
                               ┌────────────────────┘
                               │   Tonight's inputs:
                               │   - Starting pitcher stats
                               │   - Home/Away
                               │   - Ballpark factor
                               ▼
                      predictions.py  ──►  Probability Distribution
                                                    │
                                                    ▼
                                         Compare vs DraftKings Implied Odds
                                                    │
                                                    ▼
                                              Bet or Pass
                                                    │
                                                    ▼
                                         AI Post-Game Summary
                                    (predicted vs actual + analysis)
```

---

## 🗺️ Roadmap

### ✅ Phase 1 - Data Pipeline
- [x] Connect to PostgreSQL on Render
- [x] Fetch Witt per-game historical logs via MLB Stats API
- [x] Fetch Royals team game logs
- [x] Upsert pipeline with conflict handling
- [ ] Add starting pitcher lookup by game
- [ ] Add static ballpark factor table

### 🔄 Phase 2 - Feature Engineering
- [ ] Build rolling window TB averages (7, 14, 30 day)
- [ ] Join pitcher stats to Witt's game logs
- [ ] Encode home/away, RHP/LHP, and K/9 as model inputs
- [ ] Attach park factors per game

### 📊 Phase 3 - Model Training
- [ ] Implement Poisson regression for TB
- [ ] Train on 2022–2024 seasons
- [ ] Validate against held-out games
- [ ] Assess feature importance

### 🎯 Phase 4 - Predictions & Edge Detection
- [ ] Input tonight's game context → output full probability distribution
- [ ] Convert DraftKings American odds to implied probability
- [ ] Flag lines where model probability exceeds implied probability by threshold
- [ ] Output: bet recommendation with confidence level

### ⚙️ Phase 5 - Automation
- [ ] Pre-game pipeline: fetch tonight's starter → run prediction → compare DK lines
- [ ] Logging: track predictions vs actual outcomes
- [ ] Model retraining cadence

### 🤖 Phase 6 - AI Post-Game Summary
- [ ] After each game, automatically compare model prediction vs actual result
- [ ] Use Claude API to generate a plain-English summary of model performance
- [ ] Log summaries to build a track record of model accuracy over time
- [ ] Example output: *"Model predicted 48% chance of 3+ TB. Witt went 2-for-4 with a double (2 TB). Line was fairly priced. No edge identified."*

### 🔁 V2 & Beyond
- [ ] Extend model to Stolen Bases (separate model, different feature set)
- [ ] Apply framework to other 5-tool players (Elly De La Cruz, CJ Abrams, Julio Rodriguez)
- [ ] Explore pitcher type interaction effects (K/9 × park factor)

---

## 🛠️ Setup

### 1. Clone the repo
```bash
git clone https://github.com/jfontelo/witt_baseball_model.git
cd witt_baseball_model
```

### 2. Create a virtual environment
```bash
python -m venv venv
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate     # Windows
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
python database_setup.py   # Verify DB connection
python data_collection.py  # Fetch and store game logs
python model_training.py   # Train the model
python predictions.py      # Generate tonight's prediction
```

---

## ⚠️ Limitations & Honest Caveats

- **Small sample size:** Witt has 3 MLB seasons. More data = better model.
- **Model edge is small:** Even a well-calibrated model won't win every bet. A 55% win rate on +EV bets is excellent.
- **Line movement:** DK lines shift. This model is a pre-game signal, not a live trading tool.
- **No guarantee:** This is a probabilistic tool, not a betting advisor. Always bet responsibly.

---

## 📚 Data Sources

- [MLB Stats API](https://statsapi.mlb.com) - Game logs, player stats, pitcher data
- [FanGraphs Park Factors](https://www.fangraphs.com/guts.aspx?type=pf) - Park adjustment data
- [DraftKings](https://www.draftkings.com) - Prop lines and odds (manual input)

---

## 👤 About

Built by a data and analytics professional as a long-term project at the intersection of **sports analytics, data engineering, and probabilistic modeling**. The goal is a rigorous, iterative framework for thinking clearly about probability and market pricing.
