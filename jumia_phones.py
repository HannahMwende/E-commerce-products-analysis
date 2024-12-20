
import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv

def web_scraping(url):
    descriptions = []
    prices = []
    older_prices = []
    reviewss = []

    for i in range(2, 50):
        page_url = f"{url}{i}#catalog-listing"  # Append page number correctly to the base URL
        r = requests.get(page_url)
        print(f"Fetching data from: {page_url} - Status: {r.status_code}")

        if r.status_code != 200:
            print(f"Failed to fetch data from {page_url}")
            continue

        soup = BeautifulSoup(r.text, 'html.parser')

        # Extract product prices
        price_elements = soup.find_all("div", {"class": "prc"})
        for price_element in price_elements:
            prices.append(price_element.text.strip())  # Add the text content of price

        # Extract old prices
        old_price_elements = soup.find_all("div", class_="old")
        for old_price_element in old_price_elements:
            older_prices.append(old_price_element.text.strip())  # Add the text content of old prices

        # Extract product descriptions
        desc_elements = soup.find_all("h3", class_="name")
        for desc_element in desc_elements:
            descriptions.append(desc_element.text.strip())  # Add the text content of descriptions
        
        # Extract reviews
        reviews = soup.find_all("div", class_ = "stars _s")
        for rev in reviews:
            revs = reviewss.append(rev.text.strip())
    

    # Return collected data
    rows = list(zip(descriptions, prices, older_prices, reviewss))
    return rows

    
def write_to_csv(data, filename):
    # Define column headers
    headers = ["Description", "Price", "Old Price", "reviewss"]

    # Write data to a CSV file
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)  # Write the header row
        writer.writerows(data)    # Write the data rows
    print(f"Data written to {filename}")

    