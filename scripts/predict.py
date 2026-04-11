### predict.py - HR Prop Prediction Script
# Supports: Bobby Witt Jr. (witt), Julio Rodriguez (julio)
#
# Usage: python3 scripts/predict.py
# Set PLAYER at the bottom, then fill in game inputs.

import sys
import warnings
warnings.filterwarnings("ignore")
sys.path.append("../scripts")

import pandas as pd
import numpy as np
import joblib
import requests
import os
import sys
import warnings
from datetime import datetime, timedelta
from pybaseball import statcast_batter
from data_collection import engine
from sqlalchemy import text

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ─────────────────────────────────────────────
# PLAYER REGISTRY
# ─────────────────────────────────────────────

PLAYERS = {
    'witt': {
        'name':       'Bobby Witt Jr.',
        'player_id':  677951,
        'team_id':    118,       # KC Royals
        'model':  os.path.join(BASE_DIR, 'models', 'witt_hr_logistic_v10_model.pkl'),
        'scaler': os.path.join(BASE_DIR, 'models', 'witt_hr_logistic_v10_scaler.pkl'),
        'baseline':   0.160,
        'features': [
            'avg_exit_velo_15',
            'barrel_rate_15',
            'hard_hit_rate_15',
            'hr_zone_rate_15',
            'is_home',
            'pitcher_r',
            'era',
            'k_per_9',
            'park_factor',
        ],
    },
    'julio': {
        'name':       'Julio Rodriguez',
        'player_id':  677594,
        'team_id':    136,       # Seattle Mariners
        'model':  os.path.join(BASE_DIR, 'models', 'julio_hr_logistic_v2_model.pkl'),
        'scaler': os.path.join(BASE_DIR, 'models', 'julio_hr_logistic_v2_scaler.pkl'),
        'baseline':   0.181,
        'features': [
            'avg_exit_velo_15',
            'barrel_rate_15',
            'hard_hit_rate_15',
            'hr_zone_rate_15',
            'is_home',
            'pitcher_r',
            'era',
            'k_per_9',
            'park_factor',
        ],
    },
    'greene': {
        'name':       'Riley Greene',
        'player_id':  682985,
        'team_id':    116,       # Detroit Tigers
        'model':  os.path.join(BASE_DIR, 'models', 'greene_hr_logistic_v1_model.pkl'),
        'scaler': os.path.join(BASE_DIR, 'models', 'greene_hr_logistic_v1_scaler.pkl'),
        'baseline':   0.145,
        'features': [
            'avg_exit_velo_15',
            'barrel_rate_15',
            'hard_hit_rate_15',
            'hr_zone_rate_15',
            'is_home',
            'pitcher_r',
            'era',
            'park_factor',
        ],
    },
}

LEAGUE_AVG = {'era': 4.20, 'k_per_9': 8.9}
MIN_IP     = 10


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
# STEP 1: PITCHER STATS
# ─────────────────────────────────────────────

def fetch_pitcher_stats(pitcher_id, pitcher_name):
    """
    Fetch ERA, K/9, and handedness for a pitcher.
    Priority: current season (if MIN_IP met) -> prior season -> league average.
    """
    current_year = datetime.now().year

    for season in [current_year, current_year - 1]:
        url = (
            f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}"
            f"?hydrate=stats(group=pitching,type=season,season={season})"
        )
        try:
            r = requests.get(url, timeout=10)
            data = r.json()
            person = data['people'][0]
            throws = person.get('pitchHand', {}).get('code', 'R')

            stats_list = person.get('stats', [])
            if not stats_list:
                continue

            s = stats_list[0].get('splits', [])
            if not s:
                continue

            pitching = s[0]['stat']
            ip_str   = pitching.get('inningsPitched', '0.0')
            ip_whole, ip_frac = divmod(float(ip_str), 1)
            ip = ip_whole + (ip_frac * 10 / 3)

            if ip < MIN_IP:
                print(f"  {season} sample too small ({ip:.1f} IP) — trying prior season...")
                continue

            era     = float(pitching.get('era', LEAGUE_AVG['era']))
            so      = float(pitching.get('strikeOuts', 0))
            k_per_9 = round((so / ip) * 9, 2) if ip > 0 else LEAGUE_AVG['k_per_9']

            print(f"  Loaded {pitcher_name} {season}: ERA {era}, K/9 {k_per_9}, IP {ip:.1f}, throws {throws}")
            return era, k_per_9, throws

        except Exception as e:
            print(f"  API error for {pitcher_name} {season}: {e}")
            continue

    print(f"  Falling back to league averages for {pitcher_name}")
    return LEAGUE_AVG['era'], LEAGUE_AVG['k_per_9'], 'R'


# ─────────────────────────────────────────────
# STEP 2: PARK FACTOR
# ─────────────────────────────────────────────

def get_park_factor(opponent_id, is_home, home_team_id):
    """Look up park factor. Home = player's home park, Away = opponent park."""
    park_team_id = home_team_id if is_home else opponent_id
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT park_factor FROM park_factors WHERE team_id = :tid"
        ), {"tid": park_team_id}).fetchone()
    if result:
        return float(result[0])
    print(f"  No park factor found for team_id {park_team_id}, using 100")
    return 100.0


# ─────────────────────────────────────────────
# STEP 3: STATCAST ROLLING FEATURES
# ─────────────────────────────────────────────

def get_statcast_features(player_id, player_name):
    """
    Pull player's Statcast data and compute 15-day rolling features.
    Returns the most recent row's rolling values.
    """
    today    = datetime.now().date()
    lookback = today - timedelta(days=60)

    print(f"  Pulling Statcast data for {player_name}...")
    raw = statcast_batter(str(lookback), str(today), player_id=player_id)

    if raw.empty:
        print("  No Statcast data — using neutral values")
        return {
            'avg_exit_velo_15': 89.0,
            'barrel_rate_15':    0.08,
            'hard_hit_rate_15':  0.38,
            'hr_zone_rate_15':   0.12,
        }

    batted = raw[raw['launch_speed'].notna()].copy()
    batted['in_hr_zone'] = batted['launch_angle'].between(25, 35).astype(int)

    game_stats = batted.groupby('game_date').agg(
        avg_exit_velo=('launch_speed', 'mean'),
        barrel_count=('launch_speed_angle', lambda x: (x == 6).sum()),
        hard_hit_count=('launch_speed', lambda x: (x >= 95).sum()),
        hr_zone_count=('in_hr_zone', 'sum'),
        batted_balls=('launch_speed', 'count'),
    ).reset_index().sort_values('game_date')

    game_stats['barrel_rate']   = game_stats['barrel_count']   / game_stats['batted_balls']
    game_stats['hard_hit_rate'] = game_stats['hard_hit_count'] / game_stats['batted_balls']
    game_stats['hr_zone_rate']  = game_stats['hr_zone_count']  / game_stats['batted_balls']

    game_stats['avg_exit_velo_15'] = game_stats['avg_exit_velo'].shift(1).rolling(15, min_periods=5).mean()
    game_stats['barrel_rate_15']   = game_stats['barrel_rate'].shift(1).rolling(15, min_periods=5).mean()
    game_stats['hard_hit_rate_15'] = game_stats['hard_hit_rate'].shift(1).rolling(15, min_periods=5).mean()
    game_stats['hr_zone_rate_15']  = game_stats['hr_zone_rate'].shift(1).rolling(15, min_periods=5).mean()

    last = game_stats.dropna(subset=['avg_exit_velo_15']).iloc[-1]

    features = {
        'avg_exit_velo_15': round(last['avg_exit_velo_15'], 2),
        'barrel_rate_15':   round(last['barrel_rate_15'],   3),
        'hard_hit_rate_15': round(last['hard_hit_rate_15'], 3),
        'hr_zone_rate_15':  round(last['hr_zone_rate_15'],  3),
    }
    print(f"  Statcast (15-day rolling): {features}")
    return features


# ─────────────────────────────────────────────
# MAIN PREDICTION
# ─────────────────────────────────────────────

def predict(player_key, pitcher_name, pitcher_id, opponent_id, is_home, book_odds=None):

    if player_key not in PLAYERS:
        print(f"❌ Unknown player '{player_key}'. Choose from: {list(PLAYERS.keys())}")
        return

    player   = PLAYERS[player_key]
    model    = joblib.load(player['model'])
    scaler   = joblib.load(player['scaler'])
    features = player['features']
    baseline = player['baseline']

    print("\n" + "="*50)
    print(f"  {player['name']} HR Prop — {datetime.now().strftime('%B %d, %Y')}")
    print("="*50)

    # Pitcher
    print("\n[1/3] Fetching pitcher stats...")
    era, k_per_9, throws = fetch_pitcher_stats(pitcher_id, pitcher_name)
    pitcher_r = 1 if throws == 'R' else 0

    # Park
    print("\n[2/3] Looking up park factor...")
    park_factor = get_park_factor(opponent_id, is_home, player['team_id'])
    location    = "Home" if is_home else "Away"
    print(f"  Park factor: {park_factor} ({location})")

    # Statcast
    print("\n[3/3] Computing rolling Statcast features...")
    statcast = get_statcast_features(player['player_id'], player['name'])

    # Assemble feature row — only pass features this player's model uses
    row = {
        'avg_exit_velo_15': statcast['avg_exit_velo_15'],
        'barrel_rate_15':   statcast['barrel_rate_15'],
        'hard_hit_rate_15': statcast['hard_hit_rate_15'],
        'hr_zone_rate_15':  statcast['hr_zone_rate_15'],
        'is_home':          int(is_home),
        'pitcher_r':        pitcher_r,
        'era':              era,
        'k_per_9':          k_per_9,
        'park_factor':      park_factor,
    }

    X = pd.DataFrame([row])[features]
    X_scaled = scaler.transform(X)
    p_hr = model.predict_proba(X_scaled)[0][1]

    # Output
    implied = prob_to_american_odds(p_hr)

    print("\n" + "="*50)
    print(f"  {player['name']}  |  {datetime.now().strftime('%b %d, %Y')}")
    print(f"  {'Home vs' if is_home else 'Away @'} team_id {opponent_id}  |  Park factor: {park_factor}")
    print("-"*50)
    print(f"  Pitcher:   {pitcher_name} ({'RHP' if pitcher_r else 'LHP'})  |  ERA {era}" + (f"  |  K/9 {k_per_9}" if 'k_per_9' in features else ""))
    print("-"*50)
    print(f"  Model:   {implied:+d}   ({p_hr*100:.1f}%)")

    if book_odds is not None:
        book_prob = american_odds_to_prob(book_odds)
        edge      = p_hr - book_prob
        print(f"  Book:    {book_odds:+d}   ({book_prob*100:.1f}%)")
        odds_edge = implied - book_odds
        print(f"  Edge:    {'+' if odds_edge >= 0 else ''}{odds_edge} odds pts   ({'+' if edge >= 0 else ''}{edge*100:.1f}pp)")
        print("-"*50)
        if edge > 0.03:
            print(f"  ✅ VALUE  —  model beats book by {edge*100:.1f}pp")
        elif edge > 0:
            print(f"  ⚠️  MARGINAL  —  slim edge, proceed cautiously")
        else:
            print(f"  ❌ PASS  —  book is better priced than model")
    else:
        print(f"  No book odds provided")

    print("="*50 + "\n")


# ─────────────────────────────────────────────
# INPUTS — fill these in before running
# ─────────────────────────────────────────────

if __name__ == "__main__":
    PLAYER       = "greene"   # "witt", "julio", or "greene"

    PITCHER_NAME = "Janson Junk"       # e.g. "Tanner Bibee"
    PITCHER_ID   = 676083     # e.g. 669456
    OPPONENT_ID  = 146     # e.g. 114  (Cleveland = 114)
    IS_HOME      = True    # True = home, False = away
    BOOK_ODDS    = +525     # e.g. +350  (optional)

    if not PITCHER_NAME or not PITCHER_ID or not OPPONENT_ID:
        print("❌ Fill in PITCHER_NAME, PITCHER_ID, and OPPONENT_ID before running.")
    else:
        predict(
            player_key=PLAYER,
            pitcher_name=PITCHER_NAME,
            pitcher_id=PITCHER_ID,
            opponent_id=OPPONENT_ID,
            is_home=IS_HOME,
            book_odds=BOOK_ODDS,
        )
