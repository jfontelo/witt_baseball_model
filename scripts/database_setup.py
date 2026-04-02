### database_setup.py - Connect to PostgreSQL on Render
import os
import subprocess  # Allows running shell commands from Python
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Ensure required packages are installed
try:
    import psycopg2
except ModuleNotFoundError:
    print("⚠️ psycopg2 not found. Installing...")
    subprocess.run(["pip", "install", "psycopg2-binary"], check=True)
    import psycopg2  # Import again after installation

# Load environment variables
load_dotenv()

# Debug: Print DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL")
print("DATABASE_URL Loaded:", DATABASE_URL)  # Debugging step

# Create database connection
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("✅ Successfully connected to PostgreSQL!")
except Exception as e:
    print("❌ Database connection failed:", e)