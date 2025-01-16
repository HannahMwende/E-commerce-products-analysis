import psycopg2
import logging
import pandas as pd
import csv
import os

# Database Connection Parameters
DB_HOST = 'localhost'
DB_NAME = 'airflow'
DB_USER = 'airflow'
DB_PASSWORD = 'airflow'
DB_PORT = '5432'

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
        logging.info("Successfully connected to the database.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        exit(1)

# Function to fetch and display data
def fetch_table_data():
    conn = connect_to_db()
    query = "SELECT * FROM kilimall_phones_table"
    dataframe = pd.read_sql_query(query, conn)
    print("Table data:")
    print(dataframe.head())
    conn.close()

# Main function to ingest data
def ingest_data():
    # Connect to PostgreSQL
    conn = connect_to_db()

    # CSV file path
    csv_path = r"C:\Users\Vivian.Obino1\Desktop\e-commerce analysis\data\clean\kilimall_clean_phones.csv"

    # Check if the CSV file exists
    if not os.path.exists(csv_path):
        print(f"Error: File not found at {csv_path}")
        conn.close()
        exit(1)

    try:
        with conn.cursor() as cur:
            with open(csv_path, 'r', encoding='utf-8') as file:
                data_reader = csv.reader(file)
                next(data_reader)  # Skip the header row

                # Insert each row into the table
                for row in data_reader:
                    try:
                        # Remove `id` column (first column in the row)
                        insert = """
                            INSERT INTO kilimall_phones_table(description, brand, price, number_of_reviews, ram, storage, battery, source, urls)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """

                        cur.execute(insert, row[1:])  # Skip the `id` column
                    except Exception as e:
                        print(f"Error inserting row {row[1:]}: {e}")
                        break
        conn.commit()
        logging.info("Data ingested successfully")
    except Exception as e:
        logging.error(f"Error while ingesting data: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    ingest_data()
    fetch_table_data()
