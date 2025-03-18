### config.py - Load Database Credentials Securely
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get database connection URL from environment variable
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL is None:
    raise ValueError("❌ DATABASE_URL environment variable not set!")