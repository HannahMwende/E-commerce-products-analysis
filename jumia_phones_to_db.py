import psycopg2
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
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        exit(1)

# Function to create the `jumia_microwaves` table if it does not exist
def create_table(cur):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS jumia_phones (
        id SERIAL PRIMARY KEY,
        Description TEXT,
        brand TEXT,
        price INTEGER,
        old_price INTEGER,
        reviews TEXT,
        RAM TEXT,
        storage TEXT,
        Battery TEXT,
        source TEXT,
        urls TEXT
    );
    """
    try:
        cur.execute(create_table_query)
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        exit(1)

# Main function to ingest data
def ingest_phone_data():
    # Connect to PostgreSQL
    conn = connect_to_db()
    cur = conn.cursor()

    # Create the table if it does not exist
    create_table(cur)

    # Define the CSV file path
    csv_file_path = r'C:\Users\charity.ngari\Desktop\e-commerce-product-analysis\data\clean_data\jumia_clean_phones.csv'

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
                    INSERT INTO jumia_phones (id,Description,brand,price,old_price,reviews,RAM,storage,Battery,source, urls)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
