import psycopg2
import csv
import os
import psycopg2   # all modules installed via requirements.txt
from dotenv import load_dotenv


# Load environment variables from the .env file
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')

# Function to connect to PostgreSQL
def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        exit(1)

# Function to create the `jumia_microwaves` table if it does not exist
# def create_table(cur):
#     create_table_query = """
#     CREATE TABLE IF NOT EXISTS jumia_phones (
#         id SERIAL PRIMARY KEY,
#         Description TEXT,
#         brand TEXT,
#         price INTEGER,
#         old_price INTEGER,
#         reviews TEXT,
#         RAM TEXT,
#         storage TEXT,
#         Battery TEXT,
#         source TEXT,
#         urls TEXT
#     );
#     """
#     try:
#         cur.execute(create_table_query)
#     except psycopg2.Error as e:
#         print(f"Error creating table: {e}")
#         exit(1)

# Main function to ingest data
def ingest_phone_data():
    # Connect to PostgreSQL
    conn = connect_to_db()
    cur = conn.cursor()

    # Create the table if it does not exist
    # create_table(cur)

    # Define the CSV file path
    csv_file_path = r"./data/clean/jumia_clean_microwaves.csv"

    # Check if the CSV file exists
    if not os.path.exists(csv_file_path):
        print(f"Error: File not found at {csv_file_path}")
        conn.close()
        exit(1)

    # Open the CSV file and ingest data
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            data_reader = csv.reader(file)
            next(data_reader)  # Skip the header row

            # Insert each row into the table
            for row in data_reader:
                if len(row) != 11:  # Ensure row has exactly 8 values
                    print(f"Skipping row with incorrect number of values: {row}")
                    continue
                cur.execute("""
                    INSERT INTO microwaves (id,description,brand,price,old_price,capacity, reviews, source, url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, row)

        # Commit the transaction
        conn.commit()
        print("Data ingested successfully")
    except Exception as e:
        print(f"Error while ingesting data: {e}")
        conn.rollback()
    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()

if __name__ == "__main__":
    ingest_phone_data()
