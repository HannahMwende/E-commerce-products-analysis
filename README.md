# E-Commerce Product Pricing Dashboard

### Problem Statement

With the ever-changing prices on e-commerce platforms, it becomes challenging for consumers to track and find the best deals. This project aims to solve this problem by creating a data-driven dashboard that monitors product pricing across multiple e-commerce platforms and sends best price alerts to subscribers via email.

### Solution

This project involves an end-to-end data pipeline to track, analyze, and visualize product pricing trends. The system scrapes product data from e-commerce platforms, processes it through an automated ETL workflow, stores it in a database, and visualizes insights using Jupyter, Power BI, and Tableau. Best price alerts are sent to subscribers based on predefined conditions.

### Architecture
![jjool](https://github.com/user-attachments/assets/ad1f7e81-ea5b-4062-b34e-f70f8ec9207b)

### Technology Stack

* Programming Language: Python
* Orchestration: Apache Airflow
* Database: PostgreSQL
* Visualization: Power BI, Tableau, Jupyter Notebook
* Containerization: Docker
* Automation: ETL pipeline for data processing

  
### Setup and Installation
1. Clone this repository:

      ```git clone https://github.com/HannahMwende/E-commerce-products-analysis.git```

2. Install required packages:

      ```pip install -r requirements.txt```

3. Set up PostgreSQL database and update connection settings in the configuration file.

4. Configure and run Airflow DAGs for automated scraping and data processing.

5. Deploy the containerized application using Docker:
   
      ```docker-compose up -d```
