### predict.py - Bobby Witt Jr. HR Prop Prediction Script
# Run this before a game to get tonight's HR probability and implied odds
# Usage: python predict.py

import sys
sys.path.append("../scripts")

import pandas as pd
import numpy as np
import joblib
import requests
from datetime import datetime, timedelta
from pybaseball import statcast_batter
from data_collection import engine
from sqlalchemy import text

# ─────────────────────────────────────────────
# LOAD SAVED MODEL ARTIFACTS
# ─────────────────────────────────────────────

MODEL_PATH  = "models/witt_hr_logistic_model.pkl"
SCALER_PATH = "models/witt_hr_logistic_scaler.pkl"

model  = joblib.load(MODEL_PATH)
scaler = joblib.load(SCALER_PATH)

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
    """Convert probability to American odds."""
    if p >= 0.5:
        return round(-p / (1 - p) * 100)
    else:
        return round((1 - p) / p * 100)

def american_odds_to_prob(odds):
    """Convert American odds to implied probability."""
    if odds < 0:
        return -odds / (-odds + 100)
    else:
        return 100 / (odds + 100)

def parse_innings(ip_str):
    try:
        ip_whole, ip_frac = divmod(float(ip_str), 1)
        return ip_whole + (ip_frac * 10 / 3)
    except:
        return 0.0


# ─────────────────────────────────────────────
# STEP 1: WITT ROLLING FEATURES FROM DATABASE
# ─────────────────────────────────────────────

def get_witt_rolling_features():
    """Pull Witt's recent game logs from DB and compute rolling HR features."""
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT date, hr
            FROM witt_game_logs
            ORDER BY date DESC
            LIMIT 20
        """), conn)

    df = df.sort_values('date').reset_index(drop=True)

    hr_lag1   = df['hr'].iloc[-1]
    hr_avg_7  = df['hr'].tail(7).mean()
    hr_avg_15 = df['hr'].tail(15).mean()

    print(f"  Last game HR:     {hr_lag1}")
    print(f"  HR avg (7 games): {hr_avg_7:.3f}")
    print(f"  HR avg (15 games):{hr_avg_15:.3f}")

    return hr_lag1, hr_avg_7, hr_avg_15


# ─────────────────────────────────────────────
# STEP 2: STATCAST ROLLING FEATURES
# ─────────────────────────────────────────────

def get_statcast_rolling_features():
    """Pull recent Statcast data for Witt and compute rolling contact quality features."""
    end_date   = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today() - timedelta(days=60)).strftime('%Y-%m-%d')

    print(f"  Pulling Statcast data ({start_date} to {end_date})...")
    raw = statcast_batter(start_date, end_date, player_id=WITT_ID)

    if raw.empty:
        print("  ⚠️ No Statcast data found. Using league averages.")
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

    # Rolling averages — using available games, no shift needed (these are all prior to tonight)
    n = len(game_stats)
    avg_exit_velo_7  = game_stats['avg_exit_velo'].tail(min(7,  n)).mean()
    avg_exit_velo_15 = game_stats['avg_exit_velo'].tail(min(15, n)).mean()
    barrel_rate_7    = game_stats['barrel_rate'].tail(min(7,  n)).mean()
    barrel_rate_15   = game_stats['barrel_rate'].tail(min(15, n)).mean()
    hard_hit_rate_7  = game_stats['hard_hit_rate'].tail(min(7,  n)).mean()

    print(f"  Avg exit velo (7):  {avg_exit_velo_7:.1f} mph")
    print(f"  Barrel rate (7):    {barrel_rate_7:.3f}")
    print(f"  Hard hit rate (7):  {hard_hit_rate_7:.3f}")

    return avg_exit_velo_7, avg_exit_velo_15, barrel_rate_7, barrel_rate_15, hard_hit_rate_7


# ─────────────────────────────────────────────
# STEP 3: PITCHER STATS FROM DATABASE
# ─────────────────────────────────────────────

def get_pitcher_stats(pitcher_name, min_starts=5):
    """
    Look up pitcher stats from the database.
    
    Uses the most recent season's cumulative stats.
    If the current season has fewer than min_starts appearances,
    falls back to the previous season to avoid small sample noise
    (e.g. ERA 18.0 from one bad opening day start).
    """
    current_year = datetime.today().year

    with engine.connect() as conn:
        # Get all appearances for this pitcher, most recent first
        result = pd.read_sql(text("""
            SELECT era, whip, k_per_9, era_last5, whip_last5, era_vs_rhb, throws, season, date
            FROM pitcher_game_logs
            WHERE LOWER(pitcher_name) LIKE LOWER(:name)
            ORDER BY date DESC
        """), conn, params={"name": f"%{pitcher_name}%"})

    if result.empty:
        print(f"  ⚠️ Pitcher '{pitcher_name}' not found in database. Using league averages.")
        return {
            'era': 4.20, 'whip': 1.30, 'k_per_9': 8.8,
            'era_last5': 4.20, 'whip_last5': 1.30,
            'era_vs_rhb': 4.20, 'pitcher_r': 1
        }

    # Check how many starts exist in current season
    current_season = result[result['season'] == current_year]
    prev_season    = result[result['season'] == current_year - 1]

    if len(current_season) >= min_starts:
        # Enough current season data — use most recent entry (cumulative stats)
        row = current_season.iloc[0]
        print(f"  Using {current_year} season stats ({len(current_season)} appearances)")
    elif not prev_season.empty:
        # Too few current season starts — fall back to prior season
        row = prev_season.iloc[0]
        print(f"  ⚠️ Only {len(current_season)} start(s) in {current_year} — using {current_year-1} season stats")
    else:
        row = result.iloc[0]
        print(f"  Using most recent available stats")

    stats = {
        'era':        row['era'],
        'whip':       row['whip'],
        'k_per_9':    row['k_per_9'],
        'era_last5':  row['era_last5'] if pd.notna(row['era_last5']) else row['era'],
        'whip_last5': row['whip_last5'] if pd.notna(row['whip_last5']) else row['whip'],
        'era_vs_rhb': row['era_vs_rhb'] if pd.notna(row['era_vs_rhb']) else row['era'],
        'pitcher_r':  1 if row['throws'] == 'R' else 0,
    }

    print(f"  ERA:           {stats['era']}")
    print(f"  WHIP:          {stats['whip']}")
    print(f"  K/9:           {stats['k_per_9']}")
    print(f"  ERA last 5:    {stats['era_last5']}")
    print(f"  ERA vs RHB:    {stats['era_vs_rhb']}")
    print(f"  Throws:        {'R' if stats['pitcher_r'] else 'L'}")

    return stats


# ─────────────────────────────────────────────
# STEP 4: PARK FACTORS FROM DATABASE
# ─────────────────────────────────────────────

def get_park_factors(opponent_id, is_home):
    """Pull park factors for tonight's game."""
    team_id = 118 if is_home else opponent_id

    with engine.connect() as conn:
        result = pd.read_sql(text("""
            SELECT park_name, park_factor, park_factor_hr
            FROM park_factors
            WHERE team_id = :team_id
        """), conn, params={"team_id": team_id})

    if result.empty:
        print("  ⚠️ Park not found. Using league average.")
        return 100, 100, "Unknown"

    row = result.iloc[0]
    print(f"  Park:          {row['park_name']}")
    print(f"  Park factor:   {row['park_factor']}")
    print(f"  HR factor:     {row['park_factor_hr']}")

    return row['park_factor'], row['park_factor_hr'], row['park_name']


# ─────────────────────────────────────────────
# STEP 5: BULLPEN STATS FROM DATABASE
# ─────────────────────────────────────────────

def get_bullpen_stats(opponent_id):
    """Pull opponent bullpen ERA from most recent game in database."""
    with engine.connect() as conn:
        result = pd.read_sql(text("""
            SELECT bullpen_era
            FROM bullpen_stats
            WHERE opponent_id = :opponent_id
            ORDER BY game_id DESC
            LIMIT 1
        """), conn, params={"opponent_id": opponent_id})

    if result.empty:
        return 4.10

    return result.iloc[0]['bullpen_era']


# ─────────────────────────────────────────────
# MAIN PREDICTION
# ─────────────────────────────────────────────

def predict(pitcher_name, opponent_id, is_home, book_odds=None):
    """
    Run tonight's HR prediction for Witt.

    Args:
        pitcher_name (str): Starting pitcher name (partial match ok)
        opponent_id (int): Opponent team_id from park_factors table
        is_home (bool): Is Witt playing at home?
        book_odds (int, optional): Sportsbook American odds for HR prop (e.g. +315)
    """
    print("\n" + "="*55)
    print("  BOBBY WITT JR. — HR PROP PREDICTION")
    print("="*55)

    # Gather all features
    print("\n📊 Witt rolling HR form:")
    hr_lag1, hr_avg_7, hr_avg_15 = get_witt_rolling_features()

    print("\n⚡ Witt contact quality (Statcast):")
    ev7, ev15, br7, br15, hhr7 = get_statcast_rolling_features()

    print(f"\n⚾ Pitcher: {pitcher_name}")
    pitcher = get_pitcher_stats(pitcher_name)

    print(f"\n🏟️  Park ({'Home' if is_home else 'Away'}):")
    park_factor, park_factor_hr, park_name = get_park_factors(opponent_id, is_home)

    bullpen_era = get_bullpen_stats(opponent_id)

    # Assemble feature vector
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
    p_hr = model.predict_proba(X_scaled)[0][1]
    implied_odds = prob_to_american_odds(p_hr)

    # Output
    print("\n" + "="*55)
    print(f"  P(Witt HR tonight):     {p_hr:.1%}")
    print(f"  Model implied odds:     {'+' if implied_odds > 0 else ''}{implied_odds}")

    if book_odds is not None:
        book_prob = american_odds_to_prob(book_odds)
        edge = p_hr - book_prob
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
# RUN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    # ── Tonight's game inputs ──
    # Update these before each game
    PITCHER_NAME = "Chad Patrick"
    OPPONENT_ID  = 158        # Milwaukee Brewers
    IS_HOME      = True       # Royals at home
    BOOK_ODDS    = +315       # Sportsbook HR prop odds

    predict(
        pitcher_name=PITCHER_NAME,
        opponent_id=OPPONENT_ID,
        is_home=IS_HOME,
        book_odds=BOOK_ODDS,
    )
