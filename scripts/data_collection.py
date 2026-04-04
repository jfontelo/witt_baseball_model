### data_collection.py - Fetch & Store Player Game Logs + Opposing Pitcher Stats
# Refactored to be player-agnostic — supports Witt, Schwarber, and future players
# All player-specific data keyed by player_id, not player name

import subprocess

try:
    import requests
except ModuleNotFoundError:
    subprocess.run(["pip", "install", "requests"], check=True)
    import requests

try:
    import pandas as pd
except ModuleNotFoundError:
    subprocess.run(["pip", "install", "pandas"], check=True)
    import pandas as pd

try:
    import sqlalchemy
except ModuleNotFoundError:
    subprocess.run(["pip", "install", "sqlalchemy"], check=True)
    import sqlalchemy

try:
    import psycopg2
except ModuleNotFoundError:
    subprocess.run(["pip", "install", "psycopg2-binary"], check=True)
    import psycopg2

from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert
from config import DATABASE_URL
from datetime import datetime

engine = create_engine(DATABASE_URL)


# ─────────────────────────────────────────────
# PLAYERS
# Add new players here — player_id from MLB Stats API
# ─────────────────────────────────────────────

PLAYERS = {
    677951: {"name": "Bobby Witt Jr.",   "team_id": 118, "bats": "R"},
    # 656941: {"name": "Kyle Schwarber",   "team_id": 143, "bats": "L"},  # uncomment when ready
}


# ─────────────────────────────────────────────
# PARK FACTORS
# ─────────────────────────────────────────────

PARK_FACTORS = {
    115: ("Coors Field",              115, 118, 126, 200, 110),
    133: ("Sutter Health Park",       108, 107, 122,  82, 112),
    116: ("Comerica Park",            105, 104, 101, 161, 114),
    119: ("Dodger Stadium",           104,  99,  98,  79, 137),
    141: ("Rogers Centre",            103, 102, 100,  69, 118),
    111: ("Fenway Park",              103, 105, 114,  94,  84),
    110: ("Camden Yards",             103, 103, 103, 106, 121),
    143: ("Citizens Bank Park",       102, 104,  99, 109, 117),
    109: ("Chase Field",              102, 102,  98, 113, 218),
    139: ("Steinbrenner Field",       102, 104, 109,  85,  59),
    108: ("Angel Stadium",            101,  99,  99,  93,  96),
    120: ("Nationals Park",           101, 104, 108,  98, 125),
    144: ("Truist Park",              101, 101, 103,  94,  83),
    142: ("Target Field",             101, 103, 106, 112,  67),
    113: ("Great American Ball Park",  99,  97,  95,  99,  64),
    137: ("Oracle Park",               99, 100, 103, 107, 102),
    121: ("Citi Field",                99,  99, 102,  89,  81),
    147: ("Yankee Stadium",            99,  92,  90,  86,  91),
    158: ("American Family Field",     98,  95,  95,  94,  90),
    112: ("Wrigley Field",             98,  96,  94,  89, 132),
    145: ("Rate Field",                98,  96,  98,  91,  65),
    117: ("Daikin Park",               97,  97,  97,  92,  80),
    146: ("loanDepot Park",            97,  98,  99, 101, 148),
    138: ("Busch Stadium",             97, 103, 108, 109,  56),
    118: ("Kauffman Stadium",          97, 100,  99, 106, 212),
    134: ("PNC Park",                  96, 100, 101, 117, 108),
    135: ("Petco Park",                95,  92,  96,  82,  83),
    114: ("Progressive Field",         95,  95,  98,  91,  63),
    140: ("Globe Life Field",          91,  92,  94,  98,  53),
    136: ("T-Mobile Park",             91,  89,  87,  95,  31),
}


# ─────────────────────────────────────────────
# UPSERT HELPER
# ─────────────────────────────────────────────

def upsert_table(df, table_name, unique_columns):
    with engine.connect() as conn:
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        records = df.to_dict(orient="records")
        stmt = insert(table).values(records)
        update_columns = {col.name: col for col in table.columns if col.name not in unique_columns}
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=unique_columns,
            set_=update_columns
        )
        conn.execute(upsert_stmt)
        conn.commit()


# ─────────────────────────────────────────────
# TABLE CREATION
# ─────────────────────────────────────────────

def create_tables():
    create_statements = [
        text("""
        CREATE TABLE IF NOT EXISTS player_game_logs (
            game_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            date DATE,
            team TEXT,
            season INTEGER,
            opponent TEXT,
            opponent_id INTEGER,
            home_away TEXT,
            pa SMALLINT,
            h SMALLINT,
            hr SMALLINT,
            tb SMALLINT,
            sb SMALLINT,
            cs SMALLINT,
            bb SMALLINT,
            so SMALLINT,
            rbi SMALLINT,
            ops TEXT,
            UNIQUE (game_id, player_id)
        );
        """),
        text("""
        CREATE TABLE IF NOT EXISTS pitcher_game_logs (
            game_id INTEGER NOT NULL,
            date DATE,
            season INTEGER,
            pitcher_id INTEGER,
            pitcher_name TEXT,
            throws TEXT,
            era FLOAT,
            whip FLOAT,
            k_per_9 FLOAT,
            era_last5 FLOAT,
            whip_last5 FLOAT,
            k_per_9_last5 FLOAT,
            era_vs_rhb FLOAT,
            whip_vs_rhb FLOAT,
            is_first_time_opponent BOOLEAN DEFAULT FALSE,
            UNIQUE (game_id)
        );
        """),
        text("""
        CREATE TABLE IF NOT EXISTS park_factors (
            team_id INTEGER PRIMARY KEY,
            park_name TEXT,
            park_factor INTEGER,
            park_factor_1b INTEGER,
            park_factor_2b INTEGER,
            park_factor_3b INTEGER,
            park_factor_hr INTEGER
        );
        """),
        text("""
        CREATE TABLE IF NOT EXISTS bullpen_stats (
            game_id INTEGER NOT NULL,
            opponent_id INTEGER,
            season INTEGER,
            bullpen_era FLOAT,
            bullpen_whip FLOAT,
            bullpen_k_per_9 FLOAT,
            UNIQUE (game_id)
        );
        """),
    ]

    # Migrations — add new columns and migrate old witt_game_logs if it exists
    migrate_statements = [
        text("ALTER TABLE pitcher_game_logs ADD COLUMN IF NOT EXISTS era_last5 FLOAT;"),
        text("ALTER TABLE pitcher_game_logs ADD COLUMN IF NOT EXISTS whip_last5 FLOAT;"),
        text("ALTER TABLE pitcher_game_logs ADD COLUMN IF NOT EXISTS k_per_9_last5 FLOAT;"),
        text("ALTER TABLE pitcher_game_logs ADD COLUMN IF NOT EXISTS era_vs_rhb FLOAT;"),
        text("ALTER TABLE pitcher_game_logs ADD COLUMN IF NOT EXISTS whip_vs_rhb FLOAT;"),
        text("ALTER TABLE pitcher_game_logs ADD COLUMN IF NOT EXISTS is_first_time_opponent BOOLEAN DEFAULT FALSE;"),
        # Migrate old witt_game_logs into player_game_logs if it exists
        text("""
        INSERT INTO player_game_logs (
            game_id, player_id, date, team, season, opponent,
            opponent_id, home_away, pa, h, hr, tb, sb, cs, bb, so, rbi, ops
        )
        SELECT game_id, 677951, date, team, season, opponent,
               opponent_id, home_away, pa, h, hr, tb, sb, cs, bb, so, rbi, ops
        FROM witt_game_logs
        ON CONFLICT (game_id, player_id) DO NOTHING;
        """),
    ]

    with engine.connect() as conn:
        for statement in create_statements:
            conn.execute(statement)
        for statement in migrate_statements:
            try:
                conn.execute(statement)
            except Exception:
                pass  # migration may already be done or old table may not exist
        conn.commit()

create_tables()


# ─────────────────────────────────────────────
# INNINGS PITCHED HELPER
# ─────────────────────────────────────────────

def parse_innings(ip_str):
    """Convert MLB innings pitched string (e.g. '6.1') to decimal innings."""
    try:
        ip_whole, ip_frac = divmod(float(ip_str), 1)
        return ip_whole + (ip_frac * 10 / 3)
    except (ValueError, TypeError):
        return 0.0


# ─────────────────────────────────────────────
# FETCH PLAYER GAME LOGS
# Player-agnostic — pass any MLB player_id
# ─────────────────────────────────────────────

def fetch_player_game_logs(player_id, seasons):
    """Fetch per-game hitting stats for any player across given seasons."""
    game_logs = []

    for season in seasons:
        try:
            url = (
                f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
                f"?stats=gameLog&group=hitting&season={season}"
            )
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"⚠️ API request failed for player {player_id} in {season}: {e}")
            continue

        if "stats" not in data or not data["stats"]:
            print(f"⚠️ No game logs for player {player_id} in {season}. Skipping.")
            continue

        for game in data["stats"][0]["splits"]:
            game_logs.append({
                "game_id":     game["game"].get("gamePk", None),
                "player_id":   player_id,
                "date":        game.get("date", None),
                "team":        game["team"]["name"],
                "opponent":    game.get("opponent", {}).get("name", None),
                "opponent_id": game.get("opponent", {}).get("id", None),
                "season":      season,
                "home_away":   "home" if game.get("isHome", False) else "away",
                "pa":          game["stat"].get("plateAppearances", None),
                "h":           game["stat"].get("hits", None),
                "hr":          game["stat"].get("homeRuns", None),
                "tb":          game["stat"].get("totalBases", None),
                "sb":          game["stat"].get("stolenBases", None),
                "cs":          game["stat"].get("caughtStealing", None),
                "bb":          game["stat"].get("baseOnBalls", None),
                "so":          game["stat"].get("strikeOuts", None),
                "rbi":         game["stat"].get("rbi", None),
                "ops":         game["stat"].get("ops", None),
            })

    return pd.DataFrame(game_logs)


# ─────────────────────────────────────────────
# OPPOSING PITCHER LOOKUP
# ─────────────────────────────────────────────

def get_opposing_starting_pitcher(game_id, player_team_id):
    """Look up the opposing starting pitcher for a given game."""
    try:
        url = f"https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Boxscore fetch failed for game {game_id}: {e}")
        return None

    teams = data.get("teams", {})
    for side in ["away", "home"]:
        team_info = teams.get(side, {})
        team_id = team_info.get("team", {}).get("id")
        if team_id != player_team_id:
            pitchers = team_info.get("pitchers", [])
            if pitchers:
                starter_id = pitchers[0]
                players = team_info.get("players", {})
                player_key = f"ID{starter_id}"
                player_info = players.get(player_key, {}).get("person", {})
                return {
                    "pitcher_id":   starter_id,
                    "pitcher_name": player_info.get("fullName", "Unknown"),
                }

    return None


# ─────────────────────────────────────────────
# PITCHER SEASON STATS
# Cumulative ERA/WHIP/K9, last 5 starts, vs RHB splits
# ─────────────────────────────────────────────

def get_pitcher_season_stats(pitcher_id, season, before_date):
    """Compute a pitcher's stats entering a specific game."""
    LEAGUE_AVG = {"era": 4.20, "whip": 1.30, "k_per_9": 8.8}

    try:
        gamelog_url = (
            f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}/stats"
            f"?stats=gameLog&group=pitching&season={season}"
        )
        gamelog_response = requests.get(gamelog_url)
        gamelog_response.raise_for_status()
        gamelog_data = gamelog_response.json()

        bio_url = f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}"
        bio_response = requests.get(bio_url)
        bio_response.raise_for_status()
        bio_data = bio_response.json()

        splits_url = (
            f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}/stats"
            f"?stats=statSplits&group=pitching&season={season}&sitCodes=vr"
        )
        splits_response = requests.get(splits_url)
        splits_response.raise_for_status()
        splits_data = splits_response.json()

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Pitcher fetch failed for {pitcher_id}, {season}: {e}")
        return None

    throws = None
    people = bio_data.get("people", [])
    if people:
        throws = people[0].get("pitchHand", {}).get("code")

    all_splits = (
        gamelog_data.get("stats", [{}])[0].get("splits", [])
        if gamelog_data.get("stats") else []
    )
    prior_games = [
        g for g in all_splits
        if g.get("date", "9999-99-99") < before_date
        and parse_innings(g.get("stat", {}).get("inningsPitched", "0")) >= 1.0
    ]

    if not prior_games:
        print(f"  ℹ️ No prior appearances for pitcher {pitcher_id} before {before_date}. Using league averages.")
        return {
            "throws": throws,
            **LEAGUE_AVG,
            "era_last5": LEAGUE_AVG["era"],
            "whip_last5": LEAGUE_AVG["whip"],
            "k_per_9_last5": LEAGUE_AVG["k_per_9"],
            "era_vs_rhb": LEAGUE_AVG["era"],
            "whip_vs_rhb": LEAGUE_AVG["whip"],
        }

    def accumulate(games):
        er, inn, h, bb, k = 0, 0.0, 0, 0, 0
        for g in games:
            s = g.get("stat", {})
            inn += parse_innings(s.get("inningsPitched", "0"))
            er  += s.get("earnedRuns", 0)
            h   += s.get("hits", 0)
            bb  += s.get("baseOnBalls", 0)
            k   += s.get("strikeOuts", 0)
        return er, inn, h, bb, k

    er, inn, h, bb, k = accumulate(prior_games)
    if inn == 0:
        era, whip, k9 = LEAGUE_AVG["era"], LEAGUE_AVG["whip"], LEAGUE_AVG["k_per_9"]
    else:
        era  = round((er / inn) * 9, 2)
        whip = round((h + bb) / inn, 2)
        k9   = round((k / inn) * 9, 2)

    last5 = prior_games[-5:]
    er5, inn5, h5, bb5, k5 = accumulate(last5)
    if inn5 == 0:
        era5, whip5, k9_5 = LEAGUE_AVG["era"], LEAGUE_AVG["whip"], LEAGUE_AVG["k_per_9"]
    else:
        era5  = round((er5 / inn5) * 9, 2)
        whip5 = round((h5 + bb5) / inn5, 2)
        k9_5  = round((k5 / inn5) * 9, 2)

    rhb_splits = splits_data.get("stats", [{}])[0].get("splits", []) if splits_data.get("stats") else []
    era_vs_rhb  = LEAGUE_AVG["era"]
    whip_vs_rhb = LEAGUE_AVG["whip"]

    if rhb_splits:
        s = rhb_splits[0].get("stat", {})
        rhb_ip = parse_innings(s.get("inningsPitched", "0"))
        if rhb_ip > 0:
            era_vs_rhb  = round((s.get("earnedRuns", 0) / rhb_ip) * 9, 2)
            whip_vs_rhb = round((s.get("hits", 0) + s.get("baseOnBalls", 0)) / rhb_ip, 2)

    return {
        "throws":        throws,
        "era":           era,
        "whip":          whip,
        "k_per_9":       k9,
        "era_last5":     era5,
        "whip_last5":    whip5,
        "k_per_9_last5": k9_5,
        "era_vs_rhb":    era_vs_rhb,
        "whip_vs_rhb":   whip_vs_rhb,
    }


# ─────────────────────────────────────────────
# PITCHER LAZY LOADER
# Called from predict.py — gets clean season stats
# for any pitcher regardless of whether Witt has faced them
# ─────────────────────────────────────────────

def get_or_fetch_pitcher_season_stats(pitcher_name, pitcher_id, season):
    """
    Lazy loader for pitcher season stats.

    Logic:
    1. Check pitcher_game_logs for this season — need >= 5 starts for reliable stats
    2. If not enough current season data, check prior season
    3. If neither has enough data, fetch directly from MLB Stats API
       using full season cumulative stats (not game-by-game which is noisy early)
    4. Cache result in pitcher_game_logs for future use

    This solves the early season problem where ERA 18.0 from one bad start
    corrupts predictions for pitchers Witt hasn't faced much.
    """
    LEAGUE_AVG = {"era": 4.20, "whip": 1.30, "k_per_9": 8.8}
    MIN_STARTS = 5

    # ── Check DB for current and prior season ──
    with engine.connect() as conn:
        result = pd.read_sql(text("""
            SELECT era, whip, k_per_9, era_last5, whip_last5,
                   era_vs_rhb, throws, season, date
            FROM pitcher_game_logs
            WHERE pitcher_id = :pid
            ORDER BY date DESC
        """), conn, params={"pid": pitcher_id})

    if not result.empty:
        current = result[result["season"] == season]
        prior   = result[result["season"] == season - 1]

        if len(current) >= MIN_STARTS:
            row = current.iloc[0]
            print(f"  Using {season} DB stats ({len(current)} appearances)")
            return _row_to_stats(row)

        if len(prior) >= MIN_STARTS:
            row = prior.iloc[0]
            print(f"  ⚠️ Only {len(current)} start(s) in {season} — using {season-1} DB stats")
            return _row_to_stats(row)

    # ── Not enough DB data — fetch full season from API ──
    print(f"  Fetching {season} season stats from MLB API for {pitcher_name}...")
    try:
        stats_url = (
            f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}/stats"
            f"?stats=season&group=pitching&season={season}"
        )
        stats_resp = requests.get(stats_url)
        stats_resp.raise_for_status()
        stats_data = stats_resp.json()

        splits_url = (
            f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}/stats"
            f"?stats=statSplits&group=pitching&season={season}&sitCodes=vr"
        )
        splits_resp = requests.get(splits_url)
        splits_resp.raise_for_status()
        splits_data = splits_resp.json()

        bio_url = f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}"
        bio_resp = requests.get(bio_url)
        bio_resp.raise_for_status()
        bio_data = bio_resp.json()

    except requests.exceptions.RequestException as e:
        print(f"  ⚠️ API fetch failed: {e}. Using league averages.")
        return {**LEAGUE_AVG, "era_last5": LEAGUE_AVG["era"],
                "era_vs_rhb": LEAGUE_AVG["era"], "pitcher_r": 1}

    # Handedness
    throws = None
    people = bio_data.get("people", [])
    if people:
        throws = people[0].get("pitchHand", {}).get("code")

    # Season cumulative stats
    season_splits = (
        stats_data.get("stats", [{}])[0].get("splits", [])
        if stats_data.get("stats") else []
    )

    if not season_splits:
        # Try prior season from API
        print(f"  No {season} MLB stats found. Trying {season-1}...")
        try:
            prev_url = (
                f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}/stats"
                f"?stats=season&group=pitching&season={season-1}"
            )
            prev_resp = requests.get(prev_url)
            prev_resp.raise_for_status()
            prev_data = prev_resp.json()
            season_splits = (
                prev_data.get("stats", [{}])[0].get("splits", [])
                if prev_data.get("stats") else []
            )
        except Exception:
            pass

    if not season_splits:
        print(f"  ⚠️ No stats found. Using league averages.")
        return {**LEAGUE_AVG, "era_last5": LEAGUE_AVG["era"],
                "era_vs_rhb": LEAGUE_AVG["era"], "pitcher_r": int(throws == "R") if throws else 1}

    s   = season_splits[0].get("stat", {})
    inn = parse_innings(s.get("inningsPitched", "0"))

    if inn < 5:
        print(f"  ⚠️ Only {inn:.1f} IP found. Using league averages.")
        return {**LEAGUE_AVG, "era_last5": LEAGUE_AVG["era"],
                "era_vs_rhb": LEAGUE_AVG["era"], "pitcher_r": int(throws == "R") if throws else 1}

    era  = round((s.get("earnedRuns", 0) / inn) * 9, 2)
    whip = round((s.get("hits", 0) + s.get("baseOnBalls", 0)) / inn, 2)
    k9   = round((s.get("strikeOuts", 0) / inn) * 9, 2)

    # vs RHB
    rhb = splits_data.get("stats", [{}])[0].get("splits", []) if splits_data.get("stats") else []
    era_vs_rhb = LEAGUE_AVG["era"]
    if rhb:
        rs     = rhb[0].get("stat", {})
        rhb_ip = parse_innings(rs.get("inningsPitched", "0"))
        if rhb_ip > 0:
            era_vs_rhb = round((rs.get("earnedRuns", 0) / rhb_ip) * 9, 2)

    print(f"  ✅ ERA {era}, WHIP {whip}, K/9 {k9} ({inn:.1f} IP)")

    return {
        "era":        era,
        "whip":       whip,
        "k_per_9":    k9,
        "era_last5":  era,
        "era_vs_rhb": era_vs_rhb,
        "pitcher_r":  int(throws == "R") if throws else 1,
    }


def _row_to_stats(row):
    """Convert a pitcher_game_logs DB row to the stats dict predict.py expects."""
    return {
        "era":        row["era"],
        "whip":       row["whip"],
        "k_per_9":    row["k_per_9"],
        "era_last5":  row["era_last5"] if pd.notna(row["era_last5"]) else row["era"],
        "era_vs_rhb": row["era_vs_rhb"] if pd.notna(row["era_vs_rhb"]) else row["era"],
        "pitcher_r":  1 if row["throws"] == "R" else 0,
    }


# ─────────────────────────────────────────────
# BULLPEN STATS
# ─────────────────────────────────────────────

def get_bullpen_stats(opponent_id, season, before_date):
    LEAGUE_BP = {"bullpen_era": 4.10, "bullpen_whip": 1.28, "bullpen_k_per_9": 9.2}

    try:
        team_url = (
            f"https://statsapi.mlb.com/api/v1/teams/{opponent_id}/stats"
            f"?stats=season&group=pitching&season={season}&playerPool=qualifier"
        )
        response = requests.get(team_url)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Bullpen fetch failed for team {opponent_id}, {season}: {e}")
        return LEAGUE_BP

    splits = data.get("stats", [{}])[0].get("splits", []) if data.get("stats") else []
    if not splits:
        return LEAGUE_BP

    s = splits[0].get("stat", {})
    inn = parse_innings(s.get("inningsPitched", "0"))
    if inn == 0:
        return LEAGUE_BP

    return {
        "bullpen_era":     round((s.get("earnedRuns", 0) / inn) * 9, 2),
        "bullpen_whip":    round((s.get("hits", 0) + s.get("baseOnBalls", 0)) / inn, 2),
        "bullpen_k_per_9": round((s.get("strikeOuts", 0) / inn) * 9, 2),
    }


# ─────────────────────────────────────────────
# FETCH PITCHER GAME LOGS
# Tracks is_first_time_opponent per player
# ─────────────────────────────────────────────

def fetch_pitcher_game_logs(player_df, player_id):
    """
    For each game in a player's game log, look up the opposing starting pitcher
    and their season stats.

    is_first_time_opponent = True when this pitcher appears for the first time
    in this player's history. Useful for debugging and potential future feature.
    """
    player_info    = PLAYERS.get(player_id, {})
    player_team_id = player_info.get("team_id", 118)

    seen_pitcher_ids = set()
    pitcher_rows     = []
    seen_game_ids    = set()

    player_df_sorted = player_df.sort_values("date").reset_index(drop=True)

    for _, row in player_df_sorted.iterrows():
        game_id = row["game_id"]
        season  = row["season"]
        date    = row["date"]

        if game_id in seen_game_ids or pd.isna(game_id):
            continue
        seen_game_ids.add(game_id)

        pitcher_info = get_opposing_starting_pitcher(int(game_id), player_team_id)
        if not pitcher_info:
            print(f"⚠️ Could not identify starting pitcher for game {game_id}. Skipping.")
            continue

        pitcher_id   = pitcher_info["pitcher_id"]
        pitcher_name = pitcher_info["pitcher_name"]

        is_first_time = pitcher_id not in seen_pitcher_ids
        seen_pitcher_ids.add(pitcher_id)

        stats = get_pitcher_season_stats(pitcher_id, season, before_date=str(date))
        if not stats:
            continue

        pitcher_rows.append({
            "game_id":                int(game_id),
            "date":                   date,
            "season":                 season,
            "pitcher_id":             pitcher_id,
            "pitcher_name":           pitcher_name,
            "throws":                 stats["throws"],
            "era":                    stats["era"],
            "whip":                   stats["whip"],
            "k_per_9":                stats["k_per_9"],
            "era_last5":              stats["era_last5"],
            "whip_last5":             stats["whip_last5"],
            "k_per_9_last5":          stats["k_per_9_last5"],
            "era_vs_rhb":             stats["era_vs_rhb"],
            "whip_vs_rhb":            stats["whip_vs_rhb"],
            "is_first_time_opponent": is_first_time,
        })

    return pd.DataFrame(pitcher_rows)


# ─────────────────────────────────────────────
# FETCH TODAY'S PITCHER
# Proactively pulls tonight's opponent starter stats
# even if the player has never faced them before
# ─────────────────────────────────────────────

def fetch_todays_pitcher(player_id, game_date=None):
    """
    Look up tonight's scheduled game, identify the opposing starter,
    and pull their current season stats if not already in pitcher_game_logs.

    Handles doubleheaders by processing all games on the date.
    Sets is_first_time_opponent = True for pitchers never seen before.
    """
    player_info    = PLAYERS.get(player_id, {})
    player_team_id = player_info.get("team_id")
    season         = datetime.today().year
    date_str       = game_date or datetime.today().strftime('%Y-%m-%d')

    if not player_team_id:
        print(f"⚠️ Player {player_id} not in PLAYERS dict.")
        return

    try:
        schedule_url = (
            f"https://statsapi.mlb.com/api/v1/schedule"
            f"?sportId=1&teamId={player_team_id}&date={date_str}"
        )
        response = requests.get(schedule_url)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Schedule fetch failed: {e}")
        return

    dates = data.get("dates", [])
    if not dates:
        print(f"  No games scheduled for team {player_team_id} on {date_str}")
        return

    for game in dates[0].get("games", []):
        game_id = game.get("gamePk")
        if not game_id:
            continue

        with engine.connect() as conn:
            existing = pd.read_sql(text("""
                SELECT game_id FROM pitcher_game_logs WHERE game_id = :gid
            """), conn, params={"gid": game_id})

        if not existing.empty:
            print(f"  Pitcher data already exists for game {game_id}. Skipping.")
            continue

        teams = game.get("teams", {})
        for side in ["away", "home"]:
            team_data = teams.get(side, {})
            if team_data.get("team", {}).get("id") != player_team_id:
                probable     = team_data.get("probablePitcher", {})
                pitcher_id   = probable.get("id")
                pitcher_name = probable.get("fullName", "Unknown")

                if not pitcher_id:
                    print(f"  No probable pitcher listed for game {game_id} yet.")
                    continue

                print(f"  Tonight's pitcher: {pitcher_name} (id: {pitcher_id})")

                with engine.connect() as conn:
                    prior = pd.read_sql(text("""
                        SELECT COUNT(*) as n FROM pitcher_game_logs
                        WHERE pitcher_id = :pid
                    """), conn, params={"pid": pitcher_id})

                is_first_time = prior.iloc[0]["n"] == 0

                if is_first_time:
                    print(f"  First time opponent — fetching {season} season stats...")
                else:
                    print(f"  Known pitcher — fetching current season stats...")

                stats = get_pitcher_season_stats(pitcher_id, season, before_date=date_str)
                if not stats:
                    return

                row = {
                    "game_id":                game_id,
                    "date":                   date_str,
                    "season":                 season,
                    "pitcher_id":             pitcher_id,
                    "pitcher_name":           pitcher_name,
                    "throws":                 stats["throws"],
                    "era":                    stats["era"],
                    "whip":                   stats["whip"],
                    "k_per_9":                stats["k_per_9"],
                    "era_last5":              stats["era_last5"],
                    "whip_last5":             stats["whip_last5"],
                    "k_per_9_last5":          stats["k_per_9_last5"],
                    "era_vs_rhb":             stats["era_vs_rhb"],
                    "whip_vs_rhb":            stats["whip_vs_rhb"],
                    "is_first_time_opponent": is_first_time,
                }

                df = pd.DataFrame([row])
                upsert_table(df, "pitcher_game_logs", ["game_id"])
                print(f"  ✅ Stored pitcher stats for {pitcher_name} (game {game_id})")


# ─────────────────────────────────────────────
# FETCH BULLPEN GAME LOGS
# ─────────────────────────────────────────────

def fetch_bullpen_game_logs(player_df):
    bullpen_rows  = []
    seen_game_ids = set()

    for _, row in player_df.iterrows():
        game_id     = row["game_id"]
        season      = row["season"]
        date        = row["date"]
        opponent_id = row["opponent_id"]

        if game_id in seen_game_ids or pd.isna(game_id) or pd.isna(opponent_id):
            continue
        seen_game_ids.add(game_id)

        stats = get_bullpen_stats(int(opponent_id), season, before_date=str(date))

        bullpen_rows.append({
            "game_id":         int(game_id),
            "opponent_id":     int(opponent_id),
            "season":          season,
            "bullpen_era":     stats["bullpen_era"],
            "bullpen_whip":    stats["bullpen_whip"],
            "bullpen_k_per_9": stats["bullpen_k_per_9"],
        })

    return pd.DataFrame(bullpen_rows)


# ─────────────────────────────────────────────
# PARK FACTORS UPSERT
# ─────────────────────────────────────────────

def upsert_park_factors():
    records = [
        {
            "team_id":        team_id,
            "park_name":      factors[0],
            "park_factor":    factors[1],
            "park_factor_1b": factors[2],
            "park_factor_2b": factors[3],
            "park_factor_3b": factors[4],
            "park_factor_hr": factors[5],
        }
        for team_id, factors in PARK_FACTORS.items()
    ]
    df = pd.DataFrame(records)
    upsert_table(df, "park_factors", ["team_id"])


# ─────────────────────────────────────────────
# RUN PIPELINE
# ─────────────────────────────────────────────

if __name__ == "__main__":
    seasons = list(range(2022, pd.Timestamp.today().year + 1))

    for player_id, player_info in PLAYERS.items():
        player_name = player_info["name"]
        print(f"\n{'='*50}")
        print(f"Processing {player_name} (id: {player_id})")
        print(f"{'='*50}")

        # 1. Fetch and upsert player game logs
        print(f"Fetching game logs...")
        df_game_logs = fetch_player_game_logs(player_id, seasons)

        if not df_game_logs.empty:
            upsert_table(df_game_logs, "player_game_logs", ["game_id", "player_id"])
            print(f"✅ player_game_logs upserted for {player_name}!")

        # 2. Fetch and upsert opposing pitcher stats
        print(f"Fetching opposing pitcher stats...")
        df_pitchers = fetch_pitcher_game_logs(df_game_logs, player_id)

        if not df_pitchers.empty:
            upsert_table(df_pitchers, "pitcher_game_logs", ["game_id"])
            print(f"✅ pitcher_game_logs upserted!")

        # 3. Fetch and upsert bullpen stats
        print(f"Fetching bullpen stats...")
        df_bullpen = fetch_bullpen_game_logs(df_game_logs)

        if not df_bullpen.empty:
            upsert_table(df_bullpen, "bullpen_stats", ["game_id"])
            print(f"✅ bullpen_stats upserted!")

        # 4. Proactively fetch tonight's pitcher
        print(f"Fetching today's pitcher...")
        fetch_todays_pitcher(player_id)

    # 5. Upsert park factors (shared across all players)
    print(f"\nUpserting park factors...")
    upsert_park_factors()
    print(f"✅ park_factors upserted!")

    print(f"\n🚀 Data collection complete!")
