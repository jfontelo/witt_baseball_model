### predict_chad_patrick.py
# One-off prediction for Royals vs Brewers 4/3/2026
# Chad Patrick's DB entry only has his bad opening day start (ERA 18)
# so we manually override with his 2025 full season stats
# 2025: 3.53 ERA, 1.28 WHIP, 9.57 K/9 across 119.2 IP

import sys
sys.path.append("../scripts")

import pandas as pd
import numpy as np
import joblib
from datetime import datetime, timedelta
from pybaseball import statcast_batter
from data_collection import engine
from sqlalchemy import text

# ─────────────────────────────────────────────
# LOAD MODEL
# ─────────────────────────────────────────────

model  = joblib.load("models/witt_hr_logistic_model.pkl")
scaler = joblib.load("models/witt_hr_logistic_scaler.pkl")

FEATURES = [
    'hr_lag1', 'hr_avg_7', 'hr_avg_15',
    'avg_exit_velo_7', 'avg_exit_velo_15',
    'barrel_rate_7', 'barrel_rate_15', 'hard_hit_rate_7',
    'is_home', 'pitcher_r',
    'era', 'whip', 'k_per_9', 'era_last5', 'era_vs_rhb',
    'bullpen_era',
    'park_factor', 'park_factor_hr',
]

WITT_ID = 677951


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def prob_to_american_odds(p):
    if p >= 0.5:
        return round(-p / (1 - p) * 100)
    else:
        return round((1 - p) / p * 100)

def american_odds_to_prob(odds):
    if odds < 0:
        return -odds / (-odds + 100)
    else:
        return 100 / (odds + 100)


# ─────────────────────────────────────────────
# WITT ROLLING FEATURES
# ─────────────────────────────────────────────

print("\n📊 Witt rolling HR form:")
with engine.connect() as conn:
    df = pd.read_sql(text("""
        SELECT date, hr FROM witt_game_logs
        ORDER BY date DESC LIMIT 20
    """), conn)

df = df.sort_values('date').reset_index(drop=True)
hr_lag1   = df['hr'].iloc[-1]
hr_avg_7  = df['hr'].tail(7).mean()
hr_avg_15 = df['hr'].tail(15).mean()
print(f"  Last game HR:      {hr_lag1}")
print(f"  HR avg (7 games):  {hr_avg_7:.3f}")
print(f"  HR avg (15 games): {hr_avg_15:.3f}")


# ─────────────────────────────────────────────
# STATCAST ROLLING FEATURES
# ─────────────────────────────────────────────

print("\n⚡ Witt contact quality (Statcast):")
end_date   = datetime.today().strftime('%Y-%m-%d')
start_date = (datetime.today() - timedelta(days=60)).strftime('%Y-%m-%d')
print(f"  Pulling Statcast data ({start_date} to {end_date})...")

raw    = statcast_batter(start_date, end_date, player_id=WITT_ID)
batted = raw[raw['launch_speed'].notna()].copy()

game_stats = batted.groupby('game_date').agg(
    avg_exit_velo=('launch_speed', 'mean'),
    barrel_count=('launch_speed_angle', lambda x: (x == 6).sum()),
    hard_hit_count=('launch_speed', lambda x: (x >= 95).sum()),
    batted_balls=('launch_speed', 'count'),
).reset_index()

game_stats['barrel_rate']   = game_stats['barrel_count']   / game_stats['batted_balls']
game_stats['hard_hit_rate'] = game_stats['hard_hit_count'] / game_stats['batted_balls']
game_stats = game_stats.sort_values('game_date').reset_index(drop=True)

n = len(game_stats)
ev7  = game_stats['avg_exit_velo'].tail(min(7,  n)).mean()
ev15 = game_stats['avg_exit_velo'].tail(min(15, n)).mean()
br7  = game_stats['barrel_rate'].tail(min(7,  n)).mean()
br15 = game_stats['barrel_rate'].tail(min(15, n)).mean()
hhr7 = game_stats['hard_hit_rate'].tail(min(7,  n)).mean()

print(f"  Avg exit velo (7):  {ev7:.1f} mph")
print(f"  Barrel rate (7):    {br7:.3f}")
print(f"  Hard hit rate (7):  {hhr7:.3f}")


# ─────────────────────────────────────────────
# PITCHER — MANUAL OVERRIDE
# Chad Patrick 2025 full season (119.2 IP)
# DB only has his rough opening day 2025 start (ERA 18)
# ─────────────────────────────────────────────

print("\n⚾ Pitcher: Chad Patrick (2025 season stats — manual override)")
pitcher = {
    'era':        3.53,
    'whip':       1.28,
    'k_per_9':    9.57,
    'era_last5':  3.53,   # no last5 data available, using season
    'era_vs_rhb': 3.53,   # no vs RHB split available, using season
    'pitcher_r':  1,      # throws right
}
print(f"  ERA:        {pitcher['era']}")
print(f"  WHIP:       {pitcher['whip']}")
print(f"  K/9:        {pitcher['k_per_9']}")
print(f"  Throws:     R")


# ─────────────────────────────────────────────
# PARK FACTORS
# ─────────────────────────────────────────────

print("\n🏟️  Park (Home — Kauffman Stadium):")
with engine.connect() as conn:
    pf = pd.read_sql(text("""
        SELECT park_name, park_factor, park_factor_hr
        FROM park_factors WHERE team_id = 118
    """), conn)

park_factor    = pf.iloc[0]['park_factor']
park_factor_hr = pf.iloc[0]['park_factor_hr']
print(f"  Park factor:   {park_factor}")
print(f"  HR factor:     {park_factor_hr}")


# ─────────────────────────────────────────────
# BULLPEN
# ─────────────────────────────────────────────

with engine.connect() as conn:
    bp = pd.read_sql(text("""
        SELECT bullpen_era FROM bullpen_stats
        WHERE opponent_id = 158 ORDER BY game_id DESC LIMIT 1
    """), conn)

bullpen_era = bp.iloc[0]['bullpen_era'] if not bp.empty else 4.10


# ─────────────────────────────────────────────
# PREDICT
# ─────────────────────────────────────────────

X = pd.DataFrame([{
    'hr_lag1':          hr_lag1,
    'hr_avg_7':         hr_avg_7,
    'hr_avg_15':        hr_avg_15,
    'avg_exit_velo_7':  ev7,
    'avg_exit_velo_15': ev15,
    'barrel_rate_7':    br7,
    'barrel_rate_15':   br15,
    'hard_hit_rate_7':  hhr7,
    'is_home':          1,
    'pitcher_r':        pitcher['pitcher_r'],
    'era':              pitcher['era'],
    'whip':             pitcher['whip'],
    'k_per_9':          pitcher['k_per_9'],
    'era_last5':        pitcher['era_last5'],
    'era_vs_rhb':       pitcher['era_vs_rhb'],
    'bullpen_era':      bullpen_era,
    'park_factor':      park_factor,
    'park_factor_hr':   park_factor_hr,
}])

X_scaled = scaler.transform(X[FEATURES])
p_hr     = model.predict_proba(X_scaled)[0][1]
implied  = prob_to_american_odds(p_hr)

# Sportsbook comparison
book_odds  = 315
book_prob  = american_odds_to_prob(book_odds)
edge       = p_hr - book_prob

print("\n" + "="*55)
print("  BOBBY WITT JR. — HR PROP")
print("  Royals vs Brewers | April 3 2026 | Home")
print("="*55)
print(f"  P(Witt HR tonight):     {p_hr:.1%}")
print(f"  Model implied odds:     {'+' if implied > 0 else ''}{implied}")
print(f"\n  Sportsbook odds:        +{book_odds}")
print(f"  Sportsbook implied:     {book_prob:.1%}")
print(f"  Edge:                   {edge:+.1%}")

if edge > 0.03:
    print(f"\n  ✅ VALUE — Model says {p_hr:.1%}, book says {book_prob:.1%}")
elif edge > 0:
    print(f"\n  🟡 MARGINAL — Small edge, proceed with caution")
else:
    print(f"\n  ❌ NO VALUE — Book is pricing this higher than model")

print("="*55 + "\n")
