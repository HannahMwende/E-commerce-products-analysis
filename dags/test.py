import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database Configuration Dictionary
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
}

# Print dictionary contents
print("Database Configuration:")
for key, value in DB_CONFIG.items():
    print(f"{key}: {value}")

# Function to test DB connection
def test_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT version();")  # Query to check PostgreSQL version
        db_version = cur.fetchone()
        print(f"Connected to database. PostgreSQL version: {db_version[0]}")
        cur.close()
        conn.close()
    except psycopg2.OperationalError as e:
        print(f"Error: Unable to connect to the database.\n{e}")

# Run the DB connection test
test_db_connection()
