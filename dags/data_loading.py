# Import libraries
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime

# PostgreSQL connection
def postgres_conn():
    conn = psycopg2.connect(
        dbname=" ",
        user=" ",
        password=" ",
        host=" ",
        port=" "        
    )
    return conn

# Create laptops table
def create_table():
    conn = get_postgres_conn()
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS jumia_laptops (
        Name VARCHAR,
        Brand VARCHAR(100),
        RAM VARCHAR(10),
        ROM VARCHAR(20),
        Processor VARCHAR(50),
        Screen_Size VARCHAR(10),
        Price FLOAT,
        Reviews INT,
        Ratings FLOAT
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

# Create fridges table
def create_fridges_table():
    conn = get_postgres_conn()
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS jumia_fridges(
    Name VARCHAR,
    Brand VARCHAR(100),
    Size(Litres) NUMERIC,
    Doors NUMERIC,
    Color VARCHAR(20),
    Warranty(Years) NUMERIC,
    Price FLOAT,
    Reviews INT,
    Ratings FLOAT
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

# Load laptops data from CSV
def load_csv_to_postgres():
    df = pd.read_csv('./web-scrapping/data/laptops_clean.csv')

    conn = get_postgres_conn()
    cursor = conn.cursor()

    # Loop through file
    for index, row in df.iterrows():
        insert_query = """
        INSERT INTO jumia_laptops (Name, Brand, RAM, ROM, Processor, Screen_Size, Price, Reviews, Ratings)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, tuple(row))
        conn.commit()
        cursor.close()
        conn.close()

# Load fridges data from CSV into Postgres
def load_fridges_to_postgres():
    df = pd.read_csv('./web-scrapping/data/fridges_clean.csv')

    conn = get_postgres_conn()
    cursor = conn.cursor()

    # Loop through each row to insert into Postgres
    for index, row in df.iterrows():
        insert_query = """
        INSERT INTO jumia_fridges (Name, Brand, Size(Litres), Doors, Color, Warranty(Years), Price, Reviews, Ratings)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

# Default Arguments for Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Create the DAG
dag = DAG(
    'load_csv_to_postgres',
    default_args=default_args,
    description='A simple DAG to load laptops and CSV data to PostgreSQL',
    schedule_interval=None,
)

# Task to create the laptops table
create_laptops_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)

# Task to create fridges table
create_fridges_table_task = PythonOperator(
    task_id='create_fridges_table',
    python_callable=create_fridges_table,
    dag=dag,
)

# Task to load laptops data into PostgreSQL
load_laptops_data = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    dag=dag,
)

# Task to load fridges data into PostgreSQL
load_fridges_task = PythonOperator(
    task_id='load_fridges_to_postgres',
    python_callable=load_fridges_to_postgres,
    dag=dag,
)

# Set up task dependencies
create_laptops_table >> load_laptops_data
create_fridges_table_task >> load_fridges_task