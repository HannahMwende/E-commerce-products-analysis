import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import re

def web_scraping(base_url, first_page_url):
    descriptions = []
    prices = []
    older_prices = []
    reviews_list = []

    # Scrape the first page
    r = requests.get(first_page_url)
    print(f"Fetching data from: {first_page_url} - Status: {r.status_code}")

    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')

        # Extract product prices
        price_elements = soup.find_all("div", {"class": "prc"})
        for price_element in price_elements:
            prices.append(price_element.text.strip())

        # Extract old prices
        old_price_elements = soup.find_all("div", class_="old")
        for old_price_element in old_price_elements:
            older_prices.append(old_price_element.text.strip())

        # Extract product descriptions
        desc_elements = soup.find_all("h3", class_="name")
        for desc_element in desc_elements:
            descriptions.append(desc_element.text.strip())

        # Extract reviews
        reviews = soup.find_all("div", class_="stars _s")
        for rev in reviews:
            reviews_list.append(rev.text.strip())
    else:
        print(f"Failed to fetch data from {first_page_url}")

    # Scrape pages 2 to 50
    for i in range(2, 51):
        page_url = f"{base_url}{i}#catalog-listing"
        r = requests.get(page_url)
        print(f"Fetching data from: {page_url} - Status: {r.status_code}")

        if r.status_code != 200:
            print(f"Failed to fetch data from {page_url}")
            continue

        soup = BeautifulSoup(r.text, 'html.parser')

        # Extract product prices
        price_elements = soup.find_all("div", {"class": "prc"})
        for price_element in price_elements:
            prices.append(price_element.text.strip())

        # Extract old prices
        old_price_elements = soup.find_all("div", class_="old")
        for old_price_element in old_price_elements:
            older_prices.append(old_price_element.text.strip())

        # Extract product descriptions
        desc_elements = soup.find_all("h3", class_="name")
        for desc_element in desc_elements:
            descriptions.append(desc_element.text.strip())

        # Extract reviews
        reviews = soup.find_all("div", class_="stars _s")
        for rev in reviews:
            reviews_list.append(rev.text.strip())

    # Return collected data
    rows = list(zip(descriptions, prices, older_prices, reviews_list))
    return rows


def collect_data_phones():

    data = web_scraping(base_url, first_page_url)

    # Convert to DataFrame for easier handling
    df = pd.DataFrame(data, columns=['Description', 'Price', 'Old Price', 'Reviews'])

    # Save to CSV
    df.to_csv('jumia_mobile_phones.csv', index=False)


 #Base URL for pages 2 to 50
base_url = "https://www.jumia.co.ke//mobile-phones/?page="

# URL for the first page
first_page_url = "https://www.jumia.co.ke/mobile-phones/"

collect_data_phones()


