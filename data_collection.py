### data_collection.py - Fetch & Store Witt Game Logs + Opposing Pitcher Stats
import subprocess

# Ensure required packages are installed
try:
    import requests
except ModuleNotFoundError:
    print("⚠️ requests not found. Installing...")
    subprocess.run(["pip", "install", "requests"], check=True)
    import requests

try:
    import pandas as pd
except ModuleNotFoundError:
    print("⚠️ pandas not found. Installing...")
    subprocess.run(["pip", "install", "pandas"], check=True)
    import pandas as pd

try:
    import sqlalchemy
except ModuleNotFoundError:
    print("⚠️ sqlalchemy not found. Installing...")
    subprocess.run(["pip", "install", "sqlalchemy"], check=True)
    import sqlalchemy

try:
    import psycopg2
except ModuleNotFoundError:
    print("⚠️ psycopg2 not found. Installing...")
    subprocess.run(["pip", "install", "psycopg2-binary"], check=True)
    import psycopg2

from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert
from config import DATABASE_URL

# Connect to PostgreSQL
engine = create_engine(DATABASE_URL)


# ─────────────────────────────────────────────
# PARK FACTORS
# Source: Baseball Savant Statcast Park Factors, 2025 single-year
# 100 = league average, >100 = hitter friendly, <100 = pitcher friendly
# Keys: team_id → (park_name, park_factor, 1B, 2B, 3B, HR)
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


def get_park_factor(opponent_id, home_away):
    """
    Return park factor stats for a given game.
    - If Witt is home, use Kauffman (team_id 118).
    - If Witt is away, use the opponent's park.
    """
    team_id = 118 if home_away == "home" else opponent_id
    factors = PARK_FACTORS.get(team_id, ("Unknown", 100, 100, 100, 100, 100))
    return {
        "park_name":      factors[0],
        "park_factor":    factors[1],
        "park_factor_1b": factors[2],
        "park_factor_2b": factors[3],
        "park_factor_3b": factors[4],
        "park_factor_hr": factors[5],
    }


# ─────────────────────────────────────────────
# UPSERT HELPER
# ─────────────────────────────────────────────

def upsert_table(df, table_name, unique_columns):
    """
    Upsert (insert or update) data into the given table based on unique constraints.

    Args:
        df (pd.DataFrame): DataFrame containing the data to insert.
        table_name (str): Name of the table in the database.
        unique_columns (list): Columns that define the unique constraint.
    """
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
    """Create necessary tables if they don't exist."""
    create_statements = [
        text("""
        CREATE TABLE IF NOT EXISTS witt_game_logs (
            game_id INTEGER NOT NULL,
            date DATE,
            team TEXT NOT NULL,
            season INTEGER,
            opponent TEXT,
            opponent_id INTEGER,
            home_away TEXT,
            pa SMALLINT,
            h SMALLINT,
            tb SMALLINT,
            sb SMALLINT,
            cs SMALLINT,
            bb SMALLINT,
            so SMALLINT,
            rbi SMALLINT,
            ops TEXT,
            UNIQUE (game_id, team)
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
        """)
    ]

    with engine.connect() as conn:
        for statement in create_statements:
            conn.execute(statement)
        conn.commit()

create_tables()


# ─────────────────────────────────────────────
# FETCH WITT GAME LOGS
# ─────────────────────────────────────────────

def fetch_witt_game_logs(player_id, seasons):
    """Fetch Bobby Witt Jr.'s per-game hitting stats for the given seasons."""
    witt_game_logs = []

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
            print(f"⚠️ API request failed for Witt's logs in {season}: {e}")
            continue

        if "stats" not in data or not data["stats"]:
            print(f"⚠️ No game logs available for Witt in {season}. Skipping.")
            continue

        for game in data["stats"][0]["splits"]:
            witt_game_logs.append({
                "game_id": game["game"].get("gamePk", None),
                "date": game.get("date", None),
                "team": game["team"]["name"],
                "opponent": game.get("opponent", {}).get("name", None),
                "opponent_id": game.get("opponent", {}).get("id", None),
                "season": season,
                "home_away": "home" if game.get("isHome", False) else "away",
                "pa": game["stat"].get("plateAppearances", 0),
                "h": game["stat"].get("hits", 0),
                "tb": game["stat"].get("totalBases", 0),
                "sb": game["stat"].get("stolenBases", 0),
                "cs": game["stat"].get("caughtStealing", 0),
                "bb": game["stat"].get("baseOnBalls", 0),
                "so": game["stat"].get("strikeOuts", 0),
                "rbi": game["stat"].get("rbi", 0),
                "ops": game["stat"].get("ops", None),
            })

    return pd.DataFrame(witt_game_logs)


# ─────────────────────────────────────────────
# FETCH OPPOSING PITCHER GAME LOGS
# ─────────────────────────────────────────────

def get_opposing_starting_pitcher(game_id, royals_team_id=118):
    """
    For a given game_id, fetch the boxscore and return the opposing team's
    starting pitcher (first pitcher listed for the non-Royals side).

    Returns:
        dict with pitcher_id and pitcher_name, or None if not found.
    """
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
        if team_id != royals_team_id:
            pitchers = team_info.get("pitchers", [])
            if pitchers:
                starter_id = pitchers[0]  # First pitcher listed = starter
                players = team_info.get("players", {})
                player_key = f"ID{starter_id}"
                player_info = players.get(player_key, {}).get("person", {})
                return {
                    "pitcher_id": starter_id,
                    "pitcher_name": player_info.get("fullName", "Unknown"),
                }

    return None


def get_pitcher_season_stats(pitcher_id, season, before_date):
    """
    Compute a pitcher's cumulative ERA, WHIP, and K/9 entering a specific game,
    using only games they pitched in that season before before_date.

    For rookies or pitchers with no prior appearances, falls back to league averages.

    Args:
        pitcher_id (int): MLB pitcher ID.
        season (int): Season year.
        before_date (str): ISO date string (YYYY-MM-DD). Only games before this date count.

    Returns:
        dict with throws, era, whip, k_per_9.
    """
    # League average fallbacks (MLB ~2022-2024 averages)
    LEAGUE_AVG = {"era": 4.20, "whip": 1.30, "k_per_9": 8.8}

    try:
        # Fetch pitcher's game log for the season
        gamelog_url = (
            f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}/stats"
            f"?stats=gameLog&group=pitching&season={season}"
        )
        gamelog_response = requests.get(gamelog_url)
        gamelog_response.raise_for_status()
        gamelog_data = gamelog_response.json()

        # Fetch handedness
        bio_url = f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}"
        bio_response = requests.get(bio_url)
        bio_response.raise_for_status()
        bio_data = bio_response.json()
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Pitcher fetch failed for {pitcher_id}, {season}: {e}")
        return None

    # Get handedness
    throws = None
    people = bio_data.get("people", [])
    if people:
        throws = people[0].get("pitchHand", {}).get("code")  # "R" or "L"

    # Filter game log to only games before the target date
    splits = (
        gamelog_data.get("stats", [{}])[0].get("splits", [])
        if gamelog_data.get("stats")
        else []
    )
    prior_games = [
        g for g in splits
        if g.get("date", "9999-99-99") < before_date
    ]

    # No prior appearances → use league averages
    if not prior_games:
        print(f"  ℹ️ No prior appearances for pitcher {pitcher_id} before {before_date}. Using league averages.")
        return {"throws": throws, **LEAGUE_AVG}

    # Accumulate raw counting stats across prior games
    total_earned_runs = 0
    total_innings = 0.0
    total_hits = 0
    total_walks = 0
    total_strikeouts = 0

    for game in prior_games:
        stat = game.get("stat", {})

        # Innings pitched stored as "6.1" meaning 6 and 1/3 innings
        ip_str = stat.get("inningsPitched", "0")
        try:
            ip_whole, ip_frac = divmod(float(ip_str), 1)
            innings = ip_whole + (ip_frac * 10 / 3)  # convert .1 → 1/3, .2 → 2/3
        except (ValueError, TypeError):
            innings = 0.0

        total_earned_runs += stat.get("earnedRuns", 0)
        total_innings += innings
        total_hits += stat.get("hits", 0)
        total_walks += stat.get("baseOnBalls", 0)
        total_strikeouts += stat.get("strikeOuts", 0)

    # Compute cumulative ERA, WHIP, K/9 — guard against division by zero
    if total_innings == 0:
        return {"throws": throws, **LEAGUE_AVG}

    era = round((total_earned_runs / total_innings) * 9, 2)
    whip = round((total_hits + total_walks) / total_innings, 2)
    k_per_9 = round((total_strikeouts / total_innings) * 9, 2)

    return {"throws": throws, "era": era, "whip": whip, "k_per_9": k_per_9}


def fetch_pitcher_game_logs(witt_df):
    """
    For each game in Witt's game log, look up the opposing starting pitcher
    and their season stats. Returns a DataFrame ready to upsert.

    Args:
        witt_df (pd.DataFrame): Witt's game logs (must have game_id, date, season columns).
    """
    pitcher_rows = []
    seen_game_ids = set()

    for _, row in witt_df.iterrows():
        game_id = row["game_id"]
        season = row["season"]
        date = row["date"]

        if game_id in seen_game_ids or pd.isna(game_id):
            continue
        seen_game_ids.add(game_id)

        pitcher_info = get_opposing_starting_pitcher(int(game_id))
        if not pitcher_info:
            print(f"⚠️ Could not identify starting pitcher for game {game_id}. Skipping.")
            continue

        pitcher_id = pitcher_info["pitcher_id"]
        pitcher_name = pitcher_info["pitcher_name"]

        stats = get_pitcher_season_stats(pitcher_id, season, before_date=str(date))
        if not stats:
            continue

        pitcher_rows.append({
            "game_id": int(game_id),
            "date": date,
            "season": season,
            "pitcher_id": pitcher_id,
            "pitcher_name": pitcher_name,
            "throws": stats["throws"],
            "era": stats["era"],
            "whip": stats["whip"],
            "k_per_9": stats["k_per_9"],
        })

    return pd.DataFrame(pitcher_rows)


def upsert_park_factors():
    """Load the PARK_FACTORS dictionary into the park_factors table."""
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
# Only executes when run directly, not when imported
# ─────────────────────────────────────────────

if __name__ == "__main__":
    player_id = 677951  # Bobby Witt Jr.
    seasons = list(range(2022, pd.Timestamp.today().year))

    # 1. Fetch and upsert Witt's game logs
    print("Fetching Witt game logs...")
    df_witt_game_logs = fetch_witt_game_logs(player_id, seasons)
    df_witt_game_logs.columns = df_witt_game_logs.columns.str.lower().str.strip()

    with engine.connect() as conn:
        witt_columns = [
            row[0] for row in conn.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name = 'witt_game_logs'")
            )
        ]
    df_witt_game_logs = df_witt_game_logs.reindex(columns=witt_columns, fill_value=None)

    if not df_witt_game_logs.empty:
        upsert_table(df_witt_game_logs, "witt_game_logs", ["game_id", "team"])
        print("✅ witt_game_logs upserted successfully!")

    # 2. Fetch and upsert opposing pitcher stats for each Witt game
    print("Fetching opposing pitcher stats...")
    df_pitcher_game_logs = fetch_pitcher_game_logs(df_witt_game_logs)

    if not df_pitcher_game_logs.empty:
        upsert_table(df_pitcher_game_logs, "pitcher_game_logs", ["game_id"])
        print("✅ pitcher_game_logs upserted successfully!")

    # 3. Upsert park factors
    print("Upserting park factors...")
    upsert_park_factors()
    print("✅ park_factors upserted successfully!")

    print("🚀 Data collection complete and stored in PostgreSQL!")
