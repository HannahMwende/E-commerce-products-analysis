#Importing libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np


# Initialize a list to store all fridges data
fridges = []

# Loop through all 16 pages
for x in range(1, 17):  # Pages 1 to 16
    print(f"Scraping page {x}...")
    
    # Send a GET request to the page
    result = requests.get(f'https://www.kilimall.co.ke/search?q=FRIDGE&page={x}&source=search|enterSearch|FRIDGE')
    
    # Check if the request was successful
    if result.status_code == 200:
        soup = BeautifulSoup(result.text, 'html.parser')  # Parse the HTML content
        
        # Extract fridge details from divs with the class "info-box".
        fridges_info = soup.find_all('div', class_="info-box")
        
        # Extract relevant details
        for fridge_info in fridges_info:
            # Safely extract data, handle cases where tags are missing
            fridge_name = fridge_info.find('p', class_='product-title')
            fridge_price = fridge_info.find('div', class_='product-price')
            fridge_reviews = fridge_info.find('span', class_='reviews')
            
            # Clean and append extracted data
            fridges.append({
                "Name": fridge_name.text.strip() if fridge_name else "N/A",
                "Price": fridge_price.text.strip() if fridge_price else "N/A",
                "Reviews": fridge_reviews.text.strip() if fridge_reviews else "N/A"
            })
    else:
        print(f"Failed to fetch page {x}, Status code: {result.status_code}")

# Print results
for fridge in fridges:
    print(fridge)

#Save results as a DataFrame
df_fridges = pd.DataFrame(fridges)
df_fridges.to_csv('csv_files/fridges.csv', index=True)



#Extracting laptops data
# Initialize a list to store all laptops' data
laptops = []

# Loop through all 90 pages
for x in range(1, 91):  # Pages 1 to 90
    print(f"Scraping page {x}...")
    
    # Send a GET request to the page
    result = requests.get(f'https://www.kilimall.co.ke/search?q=laptop&page={x}&source=search|enterSearch|laptop')
    
    # Check if the request was successful
    if result.status_code == 200:
        soup = BeautifulSoup(result.text, 'html.parser')  # Parse the HTML content
        
        # Extract laptop details from divs with the class "info-box".
        laptops_info = soup.find_all('div', class_="info-box")
        
        # Extract relevant details
        for laptop_info in laptops_info:
            # Safely extract data, handle cases where tags are missing
            laptop_name = laptop_info.find('p', class_='product-title')
            laptop_price = laptop_info.find('div', class_='product-price')
            laptop_reviews = laptop_info.find('span', class_='reviews')
            
            # Clean and append extracted data
            laptops.append({
                "Name": laptop_name.text.strip() if laptop_name else "N/A",
                "Price": laptop_price.text.strip() if laptop_price else "N/A",
                "Reviews": laptop_reviews.text.strip() if laptop_reviews else "N/A"
            })
    else:
        print(f"Failed to fetch page {x}, Status code: {result.status_code}")

# Print results
for laptop in laptops:
    print(laptop)

#Save results as a DataFrame
df_laptops = pd.DataFrame(laptops)
df_laptops.to_csv('csv_files/laptops.csv, index = False')