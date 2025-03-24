### data_collection.py - Fetch & Store Royals Data
import subprocess  # Allows installing missing packages

# Ensure required packages are installed
try:
    import requests
except ModuleNotFoundError:
    print("⚠️ requests not found. Installing...")
    subprocess.run(["pip", "install", "requests"], check=True)
    import requests  # Re-import after installation
try:
    import pandas as pd
except ModuleNotFoundError:
    print("⚠️ pandas not found. Installing...")
    subprocess.run(["pip", "install", "pandas"], check=True)
    import pandas as pd  # Import again after installation 

try:
    import statsapi
except ModuleNotFoundError:
    print("⚠️ MLB-StatsAPI not found. Installing...")
    subprocess.run(["pip", "install", "MLB-StatsAPI"], check=True)
    import statsapi

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
    import psycopg2  # Re-import after installation

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, text
from sqlalchemy.dialects.postgresql import insert
from config import DATABASE_URL
import json

# Connect to PostgreSQL
engine = create_engine(DATABASE_URL)

# API Base URL
API_BASE = "https://statsapi.mlb.com/"

def upsert_table(df, table_name, unique_columns):
    """
    Upsert (insert or update) data into the given table based on unique constraints.

    Args:
        df (pd.DataFrame): Dataframe containing the data to insert.
        table_name (str): Name of the table in the database.
        unique_columns (list): List of columns that define the unique constraint.

    Returns:
        None
    """
    with engine.connect() as conn:
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)

        # Convert DataFrame to list of dictionaries
        records = df.to_dict(orient="records")

        # Prepare the insert statement
        stmt = insert(table).values(records)

        # Define the update logic for ON CONFLICT
        update_columns = {col.name: col for col in table.columns if col.name not in unique_columns}
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=unique_columns,  # Define uniqueness constraint
            set_=update_columns  # Update all other columns
        )

        # Execute the upsert statement
        conn.execute(upsert_stmt)
        conn.commit()

# Function to create necessary tables if they don't exist
import os
from sqlalchemy import create_engine, text

# Fetch database URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL is None:
    raise ValueError("DATABASE_URL environment variable not set!")

def create_tables():
    """Create necessary tables if they don't exist."""
    create_statements = [
        text("""
        CREATE TABLE IF NOT EXISTS witt_game_logs (
            game_id INTEGER NOT NULL,
            date DATE,
            team TEXT NOT NULL,
            opponent TEXT,
            home_away TEXT,
            ab SMALLINT,
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
        CREATE TABLE IF NOT EXISTS royals_game_logs (
            game_id INTEGER NOT NULL,
            date DATE,
            team TEXT NOT NULL,  -- Ensuring team column is added dynamically
            opponent TEXT,
            home_away TEXT,
            ab INTEGER,
            h INTEGER,
            tb INTEGER,
            sb INTEGER,
            cs INTEGER,
            bb INTEGER,
            so INTEGER,
            rbi INTEGER,
            ops TEXT,
            UNIQUE (game_id, team)
        );
        """),
        text("""
        CREATE TABLE IF NOT EXISTS opponent_offense_game_logs (
            game_id INTEGER NOT NULL,
            date DATE,
            team TEXT NOT NULL,
            season INTEGER,
            home_away TEXT,
            ab INTEGER,
            h INTEGER,
            tb INTEGER,
            sb INTEGER,
            cs INTEGER,
            bb INTEGER,
            so INTEGER,
            rbi INTEGER,
            ops TEXT,
            UNIQUE (game_id, team)
        );
        """),
        text("""
        CREATE TABLE IF NOT EXISTS opponent_defense_game_logs (
            game_id INTEGER NOT NULL,
            date DATE,
            team TEXT NOT NULL,
            season INTEGER,
            home_away TEXT,
            era FLOAT,
            whip FLOAT,
            opponent_obp FLOAT,
            opponent_slg FLOAT,
            cs_percentage FLOAT,
            errors INTEGER,
            UNIQUE (game_id, team)
        );
        """)
    ]

    with engine.connect() as conn:
        for statement in create_statements:
            conn.execute(statement)
        conn.commit()

# Call function to ensure tables exist before inserting data
create_tables()
        
# Fetch Bobby Witt Jr.'s historical game logs
def fetch_witt_game_logs(player_id, seasons):
    witt_game_logs = []

    for season in seasons:
        try:
            url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats?stats=gameLog&group=hitting&season={season}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"⚠️ API request failed for Witt's logs in {season}: {e}")
            continue  # Skip to the next season if API fails

        if "stats" not in data or not data["stats"]:
            print(f"⚠️ No game logs available for Witt in {season}. Skipping.")
            continue

        for game in data["stats"][0]["splits"]:  # ✅ Use `data` instead of `response`
            witt_game_logs.append({
                "game_id": game["game"].get("gamePk", None),
                "date": game["game"].get("officialDate", game["game"].get("date", None)),
                "team": game["team"]["name"],
                "opponent": game["opponent"]["name"],
                "home_away": "home" if game.get("isHome", False) else "away",
                "ab": game["stat"].get("atBats", 0),
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
    
# Fetch Royals historical game logs
def fetch_royals_game_logs(team_id, seasons):
    """
    Fetch per-game offensive stats for the Kansas City Royals from the MLB API.
    """
    royals_game_logs = []

    for season in seasons:
        try:
            url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/stats?stats=gameLog&group=hitting&season={season}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"⚠️ API request failed for Royals' logs in {season}: {e}")
            continue  # Skip to the next season if API fails

        if "stats" not in data or not data["stats"]:
            print(f"⚠️ No game logs available for Royals in {season}. Skipping.")
            continue
        
        # Extract the team name once from the API response (assuming it's under "team" at the top level)
        team_name = data.get("team", {}).get("name", "Kansas City Royals")  # Default to KCR if missing
        
        for game in data["stats"][0]["splits"]:
            royals_game_logs.append({
                "game_id": game["game"].get("gamePk", None),
                "date": game["game"].get("officialDate", game["game"].get("date", None)),  # Extracts official date
                "team": team_name,  # Uses extracted team name dynamically
                "opponent": game.get("opponent", {}).get("name", None),  # Extract opponent team name
                "home_away": "Home" if game.get("isHome", False) else "Away",
                "ab": game["stat"].get("atBats", 0),
                "h": game["stat"].get("hits", 0),
                "tb": game["stat"].get("totalBases", 0),
                "sb": game["stat"].get("stolenBases", 0),
                "cs": game["stat"].get("caughtStealing", 0),
                "bb": game["stat"].get("baseOnBalls", 0),
                "so": game["stat"].get("strikeOuts", 0),
                "rbi": game["stat"].get("rbi", 0),
                "ops": game["stat"].get("ops", None),
            })

    return pd.DataFrame(royals_game_logs)


# Define player/team IDs
player_id = 677951  # Bobby Witt Jr.'s Player ID
team_id = 118  # Kansas City Royals Team ID
seasons = list(range(2022, pd.Timestamp.today().year))  # Fetch from 2022 onward

# Fetch game logs with required parameters
witt_game_logs = fetch_witt_game_logs(player_id, seasons)
royals_game_logs = fetch_royals_game_logs(team_id, seasons)

# Convert to DataFrames
df_witt_game_logs = pd.DataFrame(witt_game_logs)
df_royals_game_logs = pd.DataFrame(royals_game_logs)

# Ensure column names are lowercase and stripped of whitespace
df_witt_game_logs.columns = df_witt_game_logs.columns.str.lower().str.strip()
df_royals_game_logs.columns = df_royals_game_logs.columns.str.lower().str.strip()

# Debug: Confirm that lowercasing worked properly
print("Columns in df_witt_game_logs (before upsert):", list(df_witt_game_logs.columns))
print("Columns in df_royals_game_logs (before upsert):", list(df_royals_game_logs.columns))

# Fetch PostgreSQL table schema for comparison
with engine.connect() as conn:
    witt_result = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'witt_game_logs'"))
    royals_result = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'royals_game_logs'"))

    witt_columns = [row[0] for row in witt_result]
    royals_columns = [row[0] for row in royals_result]

    print("Columns in PostgreSQL witt_game_logs:", witt_columns)
    print("Columns in PostgreSQL royals_game_logs:", royals_columns)

# Ensure "team" exists in royals_game_logs to avoid KeyError
if "team" not in df_royals_game_logs.columns:
    df_royals_game_logs["team"] = "Kansas City Royals"

# Ensure DataFrame contains only columns that exist in PostgreSQL, filling missing ones with None
df_witt_game_logs = df_witt_game_logs.reindex(columns=witt_columns, fill_value=None)
df_royals_game_logs = df_royals_game_logs.reindex(columns=royals_columns, fill_value=None)

# Upsert each DataFrame into PostgreSQL
if not df_witt_game_logs.empty:
    print("🤓 Columns in df_witt_game_logs (before upsert):", df_witt_game_logs.columns)
    upsert_table(df_witt_game_logs, "witt_game_logs", ["game_id", "team"])
    print("✅ `witt_game_logs` upserted successfully!")

if not df_royals_game_logs.empty:
    print("🤓 Columns in df_royals_game_logs (before upsert):", df_royals_game_logs.columns)
    upsert_table(df_royals_game_logs, "royals_game_logs", ["game_id", "team"])
    print("✅ `royals_game_logs` upserted successfully!")

# BECAUSE THE OPPONENT LOGS WERE USING THE ROYALS_GAME_LOGS OPPONENTS & GAME_ID, WE'LL HAVE TO RUN THIS MODULE BELOW

# Fetch opponent historical offensive game logs
def fetch_opponent_offense_game_logs(seasons):
    """
    Fetches per-game offensive stats for opponents that faced the Royals.
    """
    opponent_offense_game_logs = []

    for season in seasons:
        # Ensure the season parameter is correctly passed as a tuple
        query = "SELECT DISTINCT opponent, game_id FROM royals_game_logs WHERE season = %s;"
        opponent_teams = pd.read_sql(query, con=engine, params=[season])  # Fixed: season passed correctly

        for _, row in opponent_teams.iterrows():
            team_name = row["opponent"]
            game_id = row["game_id"]

            try:
                url = f"https://statsapi.mlb.com/api/v1/teams/stats?team={team_name}&group=hitting&stats=gameLog&season={season}"
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                print(f"⚠️ API request failed for {team_name} offense logs in {season}: {e}")
                continue

            if "stats" not in data or not data["stats"]:
                print(f"⚠️ No offensive stats for {team_name} in {season}. Skipping.")
                continue

            for game in data["stats"][0]["splits"]:
                opponent_offense_game_logs.append({
                    "game_id": game["game"].get("gamePk", None),
                    "date": game["game"].get("officialDate", game["game"].get("date", None)),  # Ensures correct date extraction
                    "team": team_name,  # Extracts team name instead of ID
                    "season": season,
                    "home_away": "Home" if game.get("isHome", False) else "Away",  # Fixes Home/Away detection
                    "ab": game["stat"].get("atBats", 0),
                    "h": game["stat"].get("hits", 0),
                    "tb": game["stat"].get("totalBases", 0),
                    "sb": game["stat"].get("stolenBases", 0),
                    "cs": game["stat"].get("caughtStealing", 0),
                    "bb": game["stat"].get("baseOnBalls", 0),
                    "so": game["stat"].get("strikeOuts", 0),
                    "rbi": game["stat"].get("rbi", 0),
                    "ops": game["stat"].get("ops", None),
                })

    return pd.DataFrame(opponent_offense_game_logs)
    
# Fetch opponent historical defensive game logs
def fetch_opponent_defense_game_logs(seasons):
    """
    Fetches per-game defensive stats for opponents that faced the Royals.
    """
    opponent_defense_game_logs = []

    for season in seasons:
        query = "SELECT DISTINCT opponent, game_id FROM royals_game_logs WHERE season = %s;"
        opponent_teams = pd.read_sql(query, con=engine, params=(season,))  # Ensure tuple format

        for _, row in opponent_teams.iterrows():
            team_name = row["opponent"]
            game_id = row["game_id"]

            try:
                # API Call for opponent's defensive game log stats
                url = f"https://statsapi.mlb.com/api/v1/teams/stats?team={team_name}&group=pitching,fielding&stats=gameLog&season={season}"
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                print(f"⚠️ API request failed for {team_name} defense logs in {season}: {e}")
                continue

            if "stats" not in data or not data["stats"]:
                print(f"⚠️ No defensive stats for {team_name} in {season}. Skipping.")
                continue

            for game in data["stats"][0]["splits"]:
                opponent_defense_game_logs.append({
                    "game_id": game["game"].get("gamePk", None),
                    "date": game["game"].get("officialDate", game["game"].get("date", None)),  # Ensures correct date extraction
                    "team": team_name,
                    "season": season,
                    "home_away": "away" if game.get("isHome", False) else "home",  # Prevents KeyError
                    "era": game["stat"].get("earnedRunAverage", None),
                    "whip": game["stat"].get("whip", None),
                    "opponent_obp": game["stat"].get("obp", None),
                    "opponent_slg": game["stat"].get("slg", None),
                    "cs_percentage": game["stat"].get("caughtStealingPercent", None),
                    "errors": game["stat"].get("errors", 0),
                })

    return pd.DataFrame(opponent_defense_game_logs)

    return pd.DataFrame(opponent_defense_game_logs)

# Fetch game logs with required parameters
opponent_offense_game_logs = fetch_opponent_offense_game_logs(seasons)
opponent_defense_game_logs = fetch_opponent_defense_game_logs(seasons)

# Convert to DataFrames
df_opponent_offense_game_logs = pd.DataFrame(opponent_offense_game_logs)
df_opponent_defense_game_logs = pd.DataFrame(opponent_defense_game_logs)

# Upsert each DataFrame into PostgreSQL
if not df_opponent_offense_game_logs.empty:
    upsert_table(df_opponent_offense_game_logs, "opponent_offense_game_logs", ["game_id", "team"])
    print("✅ `opponent_offense_game_logs` upserted successfully!")

if not df_opponent_defense_game_logs.empty:
    upsert_table(df_opponent_defense_game_logs, "opponent_defense_game_logs", ["game_id", "team"])
    print("✅ `opponent_defense_game_logs` upserted successfully!")


print("🚀 Data collection complete and stored in PostgreSQL!")


