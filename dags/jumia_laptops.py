from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import os
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
import time
import random
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Load environment variables
load_dotenv()


# Database Connection Parameters
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
}

# Directory paths for saving scraped and cleaned data
RAW_DATA_PATH = "/usr/local/airflow/data/scraped/jumia_laptops.csv"
CLEAN_DATA_PATH = "/usr/local/airflow/data/clean/jumia_laptops.csv"

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:92.0) Gecko/20100101 Firefox/92.0'}

# Scraping function
def scrape_laptop_data():
    laptops = []
    # Loop through pages 1 to 50
    for page in range(1, 51):  # Pages 1 to 50
        print(f"Scraping page {page}...")
        try:    
            result = requests.get(f'https://www.jumia.co.ke/laptops/?page=2#catalog-listing{page}', headers = headers, timeout=10)
            content = result.text
            # Parse the HTML content
            soup = BeautifulSoup(content, features="html.parser")
            # Find all laptop elements on the current page
            laptops_info = soup.find_all('article', class_="prd _fb col c-prd")
            # Loop through each laptop and extract data
            for laptop_info in laptops_info:
                # Extract laptop details
                laptop_name = laptop_info.find('h3', class_='name').text.strip()
                laptop_price = laptop_info.find('div', class_='prc').text.strip()
                rating_element = laptop_info.find('div', class_='stars _s')
                laptop_ratings = rating_element.text.strip() if rating_element else "None"
                laptop_links = laptop_info.find('a', class_='core')['href'].strip()
                # Append the data to the list
                laptops.append({
                    "Name": laptop_name,
                    "Price": laptop_price,
                    "Ratings": laptop_ratings,
                    "Links": f"https://www.jumia.co.ke{laptop_links}"
                })
            time.sleep(random.uniform(2, 5))  # Random delay to reduce blocking
        except requests.RequestException as e:
            print(f"Error on page {page}: {e}")

    # Save the extracted data 
    df = pd.DataFrame(laptops)
    os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)
    df.to_csv(RAW_DATA_PATH, index=False, encoding='utf-8')
    print("Scraping completed.")



# Data Cleaning function
def clean_laptop_data():
    # Extract brand name
    def extract_brand(name):
        match = re.search(r'(HP|Lenovo|Dell|Acer|)', name, re.IGNORECASE)
        return match.group(0) if match else 'Unknown'
    # Extract RAM
    def extract_ram(name):
        match = re.search(r'(\d+GB)\s*RAM', name)
        return match.group(1) if match else 'Unknown'
    # Extract ROM (HDD/SSD)
    def extract_rom(name):
        match = re.search(r'(\d+GB|TB)\s*(HDD|SSD)', name)
        return match.group(0) if match else 'Uknown'
    # Extract processor type
    def extract_processor(name):
        match = re.search(r'Intel\s*(Core\s*I\d)', name)
        return match.group(1) if match else 'Unknown'
      #Extract screen_size              
    def extract_screen_size(name):
        match = re.search(r'(\d+\.?\d*)"\s*', name)
        return match.group(1) if match else 'Unknown'
    # Extract the price from the 'Price' column
    def extract_price(price):
        match = re.search(r'KSh\s*(\d+([,]\d{3})*)', price)
        if match:
            return float(match.group(1).replace(',', ''))
        return None
    # # Extract reviews 
    # def extract_reviews(reviews):
    #     match = re.search(r'\((\d+)\)', reviews)
    #     if match:
    #         return int(match.group(1))
    #     return None
    # Extract ratings (the number before "out of 5")
    def extract_ratings(ratings):
        ratings = str(ratings)        
        match = re.search(r'(\d+\.\d+)', ratings)
        if match:
            return float(match.group(1))
        return None
    laptops_df = pd.read_csv(RAW_DATA_PATH)
    laptops_df['name'] = laptops_df['Name']
    laptops_df['brand'] = laptops_df['Name'].apply(extract_brand)
    laptops_df['ram'] = laptops_df['Name'].apply(extract_ram)
    laptops_df['rom'] = laptops_df['Name'].apply(extract_rom)
    laptops_df['processor'] = laptops_df['Name'].apply(extract_processor)
    laptops_df['screen_size'] = laptops_df['Name'].apply(extract_screen_size)
    laptops_df['price'] = laptops_df['Price'].apply(extract_price)
    #laptops_df['reviews'] = laptops_df['Reviews'].apply(extract_reviews)
    laptops_df['ratings'] = laptops_df['Ratings'].apply(extract_ratings)
    laptops_df['links'] = laptops_df['Links']
    laptops_df['source'] = 'Jumia'

    data = laptops_df[['name','brand', 'ram', 'rom', 'processor', 'screen_size', 'price','ratings', 'links','source']]
    os.makedirs(os.path.dirname(CLEAN_DATA_PATH), exist_ok=True)
    data.to_csv(CLEAN_DATA_PATH, index=False)
    print("Data cleaning completed and saved.")



# Storing data to DB function
def ingest_data():
    pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    
    with open("/usr/local/airflow/data/clean/jumia_laptops.csv", "r") as f:
        cur.copy_expert(
            "COPY laptops(name, brand, ram, rom, processor, screen_size, price, ratings, links, source) FROM STDIN WITH CSV HEADER DELIMITER ','",
            f,
        )
    conn.commit()
    cur.close()
    conn.close()
    print("Data ingested successfully.")

# Airflow DAG definition
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    "scrape_clean_store_laptop_data",
    default_args=default_args,
    description="Scrape, clean, and store laptop data from Jumia",
    schedule_interval="@once",
    catchup=False,
) as dag:
    
    # Task 1: Create the `laptops` table in PostgreSQL if it doesn't exist
    create_table_task = PostgresOperator(
        task_id="create_laptop_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS laptops (
                id SERIAL PRIMARY KEY,
                name TEXT,
                brand TEXT,
                ram TEXT,
                rom TEXT,
                processor TEXT,
                screen_size TEXT,
                price FLOAT,
                ratings FLOAT,
                links TEXT,
                source TEXT
            );
        """,
    )

    # Task 2: Scrape laptop data
    scrape_data_task = PythonOperator(
        task_id="scrape_laptop_data",
        python_callable=scrape_laptop_data
    )

    # Task 3: Clean laptop data
    clean_data_task = PythonOperator(
        task_id="clean_laptop_data",
        python_callable=clean_laptop_data
    )

    # Task 4: Insert cleaned data into PostgreSQL
    ingest_data_task = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data
    )

    create_table_task >> scrape_data_task >> clean_data_task >> ingest_data_task
