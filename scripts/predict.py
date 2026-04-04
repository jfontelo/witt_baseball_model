### predict.py - Bobby Witt Jr. HR Prop Prediction Script
# Run from project root: python3 scripts/predict.py
# Fill in the inputs at the bottom before each game

import sys
sys.path.append("scripts")

import pandas as pd
import numpy as np
import joblib
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime, timedelta
from pybaseball import statcast_batter
from data_collection import engine, get_or_fetch_pitcher_season_stats
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

def get_witt_rolling_features():
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT date, hr FROM player_game_logs
            WHERE player_id = 677951
            ORDER BY date DESC LIMIT 20
        """), conn)

    df = df.sort_values('date').reset_index(drop=True)
    hr_lag1   = df['hr'].iloc[-1]
    hr_avg_7  = df['hr'].tail(7).mean()
    hr_avg_15 = df['hr'].tail(15).mean()

    print(f"  Last game HR:      {hr_lag1}")
    print(f"  HR avg (7 games):  {hr_avg_7:.3f}")
    print(f"  HR avg (15 games): {hr_avg_15:.3f}")

    return hr_lag1, hr_avg_7, hr_avg_15


# ─────────────────────────────────────────────
# STATCAST ROLLING FEATURES
# ─────────────────────────────────────────────

def get_statcast_rolling_features():
    end_date   = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today() - timedelta(days=60)).strftime('%Y-%m-%d')

    print(f"  Pulling Statcast data ({start_date} to {end_date})...")
    raw = statcast_batter(start_date, end_date, player_id=WITT_ID)

    if raw.empty:
        print("  ⚠️ No Statcast data. Using league averages.")
        return 88.0, 88.0, 0.08, 0.08, 0.35

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

    n    = len(game_stats)
    ev7  = game_stats['avg_exit_velo'].tail(min(7,  n)).mean()
    ev15 = game_stats['avg_exit_velo'].tail(min(15, n)).mean()
    br7  = game_stats['barrel_rate'].tail(min(7,  n)).mean()
    br15 = game_stats['barrel_rate'].tail(min(15, n)).mean()
    hhr7 = game_stats['hard_hit_rate'].tail(min(7,  n)).mean()

    print(f"  Avg exit velo (7):  {ev7:.1f} mph")
    print(f"  Barrel rate (7):    {br7:.3f}")
    print(f"  Hard hit rate (7):  {hhr7:.3f}")

    return ev7, ev15, br7, br15, hhr7


# ─────────────────────────────────────────────
# PARK FACTORS
# ─────────────────────────────────────────────

def get_park_factors(opponent_id, is_home):
    team_id = 118 if is_home else opponent_id

    with engine.connect() as conn:
        result = pd.read_sql(text("""
            SELECT park_name, park_factor, park_factor_hr
            FROM park_factors WHERE team_id = :tid
        """), conn, params={"tid": team_id})

    if result.empty:
        print("  ⚠️ Park not found. Using league average.")
        return 100, 100, "Unknown"

    row = result.iloc[0]
    print(f"  Park:        {row['park_name']}")
    print(f"  Park factor: {row['park_factor']}")
    print(f"  HR factor:   {row['park_factor_hr']}")

    return row['park_factor'], row['park_factor_hr'], row['park_name']


# ─────────────────────────────────────────────
# BULLPEN STATS
# ─────────────────────────────────────────────

def get_bullpen_era(opponent_id):
    with engine.connect() as conn:
        result = pd.read_sql(text("""
            SELECT bullpen_era FROM bullpen_stats
            WHERE opponent_id = :oid ORDER BY game_id DESC LIMIT 1
        """), conn, params={"oid": opponent_id})

    return result.iloc[0]['bullpen_era'] if not result.empty else 4.10


# ─────────────────────────────────────────────
# PREDICT
# ─────────────────────────────────────────────

def predict(pitcher_name, pitcher_id, opponent_id, is_home, book_odds=None):
    """
    Run tonight's HR prediction for Witt.

    pitcher_id: MLB player ID — find at mlb.com/player or baseball-reference.com
    opponent_id: team_id from park_factors (e.g. 158 = Brewers)
    """
    print("\n" + "="*55)
    print("  BOBBY WITT JR. — HR PROP PREDICTION")
    print("="*55)

    print("\n📊 Witt rolling HR form:")
    hr_lag1, hr_avg_7, hr_avg_15 = get_witt_rolling_features()

    print("\n⚡ Witt contact quality (Statcast):")
    ev7, ev15, br7, br15, hhr7 = get_statcast_rolling_features()

    print(f"\n⚾ Pitcher: {pitcher_name} (id: {pitcher_id})")
    pitcher = get_or_fetch_pitcher_season_stats(pitcher_name, pitcher_id, datetime.today().year)

    print(f"  ERA:        {pitcher['era']}")
    print(f"  WHIP:       {pitcher['whip']}")
    print(f"  K/9:        {pitcher['k_per_9']}")
    print(f"  Throws:     {'R' if pitcher['pitcher_r'] else 'L'}")

    print(f"\n🏟️  Park ({'Home' if is_home else 'Away'}):")
    park_factor, park_factor_hr, park_name = get_park_factors(opponent_id, is_home)

    bullpen_era = get_bullpen_era(opponent_id)

    X = pd.DataFrame([{
        'hr_lag1':          hr_lag1,
        'hr_avg_7':         hr_avg_7,
        'hr_avg_15':        hr_avg_15,
        'avg_exit_velo_7':  ev7,
        'avg_exit_velo_15': ev15,
        'barrel_rate_7':    br7,
        'barrel_rate_15':   br15,
        'hard_hit_rate_7':  hhr7,
        'is_home':          int(is_home),
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

    print("\n" + "="*55)
    print(f"  P(Witt HR tonight):     {p_hr:.1%}")
    print(f"  Model implied odds:     {'+' if implied > 0 else ''}{implied}")

    if book_odds is not None:
        book_prob = american_odds_to_prob(book_odds)
        edge      = p_hr - book_prob
        print(f"\n  Sportsbook odds:        {'+' if book_odds > 0 else ''}{book_odds}")
        print(f"  Sportsbook implied:     {book_prob:.1%}")
        print(f"  Edge:                   {edge:+.1%}")

        if edge > 0.03:
            print(f"\n  ✅ VALUE — Model says {p_hr:.1%}, book says {book_prob:.1%}")
        elif edge > 0:
            print(f"\n  🟡 MARGINAL — Small edge, proceed with caution")
        else:
            print(f"\n  ❌ NO VALUE — Book is pricing this higher than model")

    print("="*55 + "\n")
    return p_hr


# ─────────────────────────────────────────────
# RUN — fill in inputs before each game
# Pitcher ID: look up at mlb.com/player
# Opponent ID: from park_factors table (e.g. 158 = Brewers, 138 = Cardinals)
# To get team ID, PSQL: SELECT team_id, park_name FROM park_factors ORDER BY park_name;
# ─────────────────────────────────────────────

if __name__ == "__main__":
    PITCHER_NAME = "Chad Patrick"       # e.g. "Chad Patrick"
    PITCHER_ID   = 694477     # e.g. 694477
    OPPONENT_ID  = 158     # e.g. 158
    IS_HOME      = True
    BOOK_ODDS    = +366     # e.g. +383

    if not PITCHER_NAME or not PITCHER_ID or not OPPONENT_ID:
        print("❌ Fill in PITCHER_NAME, PITCHER_ID, and OPPONENT_ID before running.")
    else:
        predict(
            pitcher_name=PITCHER_NAME,
            pitcher_id=PITCHER_ID,
            opponent_id=OPPONENT_ID,
            is_home=IS_HOME,
            book_odds=BOOK_ODDS,
        )
