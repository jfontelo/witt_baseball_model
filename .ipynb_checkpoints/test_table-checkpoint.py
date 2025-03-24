# This is a test table to validate if the upsert function is working correctly

import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL is None:
    raise ValueError("DATABASE_URL environment variable not set!")

engine = create_engine(DATABASE_URL)
metadata = MetaData()

test_table = Table(
    "test_table",
    metadata,
    Column("game_id", Integer, nullable=False),
    Column("team", String, nullable=False),
    Column("date", Date),
    Column("score", Integer),
    UniqueConstraint("game_id", "team", name="uix_game_team")
)

# Drop and recreate for a fresh test
metadata.drop_all(engine, tables=[test_table])
metadata.create_all(engine)

def upsert_table(df, table_name, unique_columns):
    with engine.connect() as conn:
        local_metadata = MetaData()
        table = Table(table_name, local_metadata, autoload_with=engine)

        records = df.to_dict(orient="records")
        stmt = insert(table).values(records)

        # Use EXCLUDED columns so the row actually updates with the new values
        update_columns = {
            c.name: getattr(stmt.excluded, c.name)
            for c in table.columns
            if c.name not in unique_columns
        }

        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=unique_columns,
            set_=update_columns
        )
        conn.execute(upsert_stmt)
        conn.commit()

# Initial data
initial_data = [
    {"game_id": 1, "team": "Team A", "date": "2023-01-01", "score": 10},
    {"game_id": 2, "team": "Team B", "date": "2023-01-02", "score": 15},
]
df_initial = pd.DataFrame(initial_data)

# Insert
upsert_table(df_initial, "test_table", ["game_id", "team"])
print("Initial insert complete!")

# Now upsert with an updated score
update_data = [
    {"game_id": 1, "team": "Team A", "date": "2023-01-01", "score": 20},
]
df_update = pd.DataFrame(update_data)
upsert_table(df_update, "test_table", ["game_id", "team"])
print("Upsert (update) complete!")

# Verify results
with engine.connect() as conn:
    result = conn.execute(text("SELECT game_id, team, date, score FROM test_table")).fetchall()
    for row in result:
        # Just print row._mapping to see it as a dict
        print(dict(row._mapping))