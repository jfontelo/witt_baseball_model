### config.py - Load Database Credentials Securely
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Retrieve database URL
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL is None:
    raise ValueError("DATABASE_URL not found. Make sure .env is correctly set up.")