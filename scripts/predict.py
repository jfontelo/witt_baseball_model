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

model  = joblib.load("models/witt_hr_logistic_v7_model.pkl")
scaler = joblib.load("models/witt_hr_logistic_v7_scaler.pkl")

FEATURES = [
    'avg_exit_velo_15',
    'barrel_rate_15',
    'hard_hit_rate_15',
    'hr_zone_rate_15',    # % batted balls in 25-35 degree launch angle band
    'is_home',
    'pitcher_r',
    'days_rest',          # 0 = back-to-back, capped at 4
    'era',
    'whip',
    'k_per_9',
    'era_last5',
    'era_vs_rhb',
    'gb_rate',            # pitcher ground ball tendency
    'bullpen_era',
    'park_factor',
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
# DAYS REST
# ─────────────────────────────────────────────

def get_days_rest(game_date=None):
    """
    Compute how many rest days Witt has entering tonight's game.
    game_date defaults to today. Capped at 4 — consistent with model training.
    """
    target = pd.to_datetime(game_date or datetime.today().strftime('%Y-%m-%d'))

    with engine.connect() as conn:
        result = pd.read_sql(text("""
            SELECT date FROM player_game_logs
            WHERE player_id = 677951 AND date < :target
            ORDER BY date DESC LIMIT 1
        """), conn, params={"target": str(target.date())})

    if result.empty:
        print("  ⚠️ No prior games found. Using 1 day rest.")
        return 1

    last_game = pd.to_datetime(result.iloc[0]['date'])
    days = (target - last_game).days - 1
    days = max(0, min(days, 4))  # clip 0-4

    print(f"  Last game:   {last_game.date()}")
    print(f"  Days rest:   {days}")
    return days


# ─────────────────────────────────────────────
# STATCAST ROLLING FEATURES
# ─────────────────────────────────────────────

def get_statcast_rolling_features():
    end_date   = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today() - timedelta(days=75)).strftime('%Y-%m-%d')

    print(f"  Pulling Statcast data ({start_date} to {end_date})...")
    raw = statcast_batter(start_date, end_date, player_id=WITT_ID)

    if raw.empty:
        print("  ⚠️ No Statcast data. Using league averages.")
        return 88.0, 0.08, 0.35, 0.28

    batted = raw[raw['launch_speed'].notna()].copy()

    # HR zone: 25-35 degree launch angle band
    batted['in_hr_zone'] = batted['launch_angle'].between(25, 35).astype(int)

    game_stats = batted.groupby('game_date').agg(
        avg_exit_velo=('launch_speed', 'mean'),
        barrel_count=('launch_speed_angle', lambda x: (x == 6).sum()),
        hard_hit_count=('launch_speed', lambda x: (x >= 95).sum()),
        hr_zone_count=('in_hr_zone', 'sum'),
        batted_balls=('launch_speed', 'count'),
    ).reset_index()

    game_stats['barrel_rate']   = game_stats['barrel_count']   / game_stats['batted_balls']
    game_stats['hard_hit_rate'] = game_stats['hard_hit_count'] / game_stats['batted_balls']
    game_stats['hr_zone_rate']  = game_stats['hr_zone_count']  / game_stats['batted_balls']
    game_stats = game_stats.sort_values('game_date').reset_index(drop=True)

    n     = len(game_stats)
    ev15  = game_stats['avg_exit_velo'].tail(min(15, n)).mean()
    br15  = game_stats['barrel_rate'].tail(min(15, n)).mean()
    hhr15 = game_stats['hard_hit_rate'].tail(min(15, n)).mean()
    hrz15 = game_stats['hr_zone_rate'].tail(min(15, n)).mean()

    print(f"  Avg exit velo (15):   {ev15:.1f} mph")
    print(f"  Barrel rate (15):     {br15:.3f}")
    print(f"  Hard hit rate (15):   {hhr15:.3f}")
    print(f"  HR zone rate (15):    {hrz15:.3f}  (25-35 degree band)")

    return ev15, br15, hhr15, hrz15


# ─────────────────────────────────────────────
# PARK FACTORS
# ─────────────────────────────────────────────

def get_park_factors(opponent_id, is_home):
    team_id = 118 if is_home else opponent_id

    with engine.connect() as conn:
        result = pd.read_sql(text("""
            SELECT park_name, park_factor
            FROM park_factors WHERE team_id = :tid
        """), conn, params={"tid": team_id})

    if result.empty:
        print("  ⚠️ Park not found. Using league average.")
        return 100, "Unknown"

    row = result.iloc[0]
    print(f"  Park:        {row['park_name']}")
    print(f"  Park factor: {row['park_factor']}")

    return row['park_factor'], row['park_name']


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

def predict(pitcher_name, pitcher_id, opponent_id, is_home, book_odds=None, game_date=None):
    """
    Run tonight's HR prediction for Witt.

    pitcher_id:  MLB player ID — find at mlb.com/player or baseball-reference.com
    opponent_id: team_id from park_factors (e.g. 158 = Brewers)
    game_date:   optional override, defaults to today (YYYY-MM-DD)
    """
    print("\n" + "="*55)
    print("  BOBBY WITT JR. — HR PROP PREDICTION")
    print("="*55)

    print("\n📅 Rest:")
    days_rest = get_days_rest(game_date)

    print("\n⚡ Witt contact quality (Statcast, 15-day):")
    ev15, br15, hhr15, hrz15 = get_statcast_rolling_features()

    print(f"\n⚾ Pitcher: {pitcher_name} (id: {pitcher_id})")
    pitcher = get_or_fetch_pitcher_season_stats(pitcher_name, pitcher_id, datetime.today().year)

    print(f"  ERA:        {pitcher['era']}")
    print(f"  WHIP:       {pitcher['whip']}")
    print(f"  K/9:        {pitcher['k_per_9']}")
    print(f"  GB rate:    {pitcher.get('gb_rate', 0.44):.3f}  ({'ground ball' if pitcher.get('gb_rate', 0.44) > 0.50 else 'fly ball'} pitcher)")
    print(f"  Throws:     {'R' if pitcher['pitcher_r'] else 'L'}")

    print(f"\n🏟️  Park ({'Home' if is_home else 'Away'}):")
    park_factor, park_name = get_park_factors(opponent_id, is_home)

    bullpen_era = get_bullpen_era(opponent_id)

    X = pd.DataFrame([{
        'avg_exit_velo_15': ev15,
        'barrel_rate_15':   br15,
        'hard_hit_rate_15': hhr15,
        'hr_zone_rate_15':  hrz15,
        'is_home':          int(is_home),
        'pitcher_r':        pitcher['pitcher_r'],
        'days_rest':        days_rest,
        'era':              pitcher['era'],
        'whip':             pitcher['whip'],
        'k_per_9':          pitcher['k_per_9'],
        'era_last5':        pitcher['era_last5'],
        'era_vs_rhb':       pitcher['era_vs_rhb'],
        'gb_rate':          pitcher.get('gb_rate', 0.44),
        'bullpen_era':      bullpen_era,
        'park_factor':      park_factor,
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
# Opponent ID: from park_factors table
# To get team IDs: SELECT team_id, park_name FROM park_factors ORDER BY park_name;
# ─────────────────────────────────────────────

if __name__ == "__main__":
    PITCHER_NAME = "Kyle Harrison"
    PITCHER_ID   = 690986
    OPPONENT_ID  = 158        # 158 = Brewers
    IS_HOME      = True
    BOOK_ODDS    = +391

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
