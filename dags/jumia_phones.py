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
    urls = []

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

        
        products = soup.find_all('a', class_='core')

        for product in products:
                # Extract product link
                link = product['href'] if 'href' in product.attrs else None
                # Complete the URL if the link is relative
                link = f"https://www.jumia.co.ke{link}" if link and link.startswith('/') else link
                urls.append(link)


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
        
        products = soup.find_all('a', class_='core')

        for product in products:
            # Extract product link 
            link = product['href'] if 'href' in product.attrs else None
            # Complete the URL if the link is relative
            link = f"https://www.jumia.co.ke{link}" if link and link.startswith('/') else link
            urls.append(link)


    # Return collected data
    rows = list(zip(descriptions, prices, older_prices, reviews_list, urls))
    return rows


def collect_data_phones():

    data = web_scraping(base_url, first_page_url)

    # Convert to DataFrame for easier handling
    df = pd.DataFrame(data, columns=['Description', 'Price', 'Old Price', 'Reviews', 'urls'])

    # Save to CSV
    df.to_csv('..data\jumia_scraped_phones.csv', index=False)


 #Base URL for pages 2 to 50
base_url = "https://www.jumia.co.ke//mobile-phones/?page="

# URL for the first page
first_page_url = "https://www.jumia.co.ke/mobile-phones/"

collect_data_phones()


def phones_cleaning(csv_path):
    dataset = pd.read_csv(csv_path)
    id = [i for i in range(1, len(dataset) + 1)]
    dataset["id"] = id
    dataset["id"] = dataset["id"].astype(int)
    dataset["source"] = ["Jumia"] * len(data)
    dataset["price"] = dataset["Price"].str.replace("KSh", "").str.replace(",", "")
    # dataset.drop("Price", axis = 1)
    dataset["price"] = dataset["price"].str.strip().str.replace("-", "").str.extract(r"(\d+)", expand = False)
    dataset["price"] = dataset["price"].astype(int)
    dataset["old_price"] = dataset["Old Price"].str.replace("KSh", "").str.replace(",", "")
    dataset["old_price"] = dataset["old_price"].str.strip().str.replace("-", "").str.extract(r"(\d+)", expand = False)
    dataset["old_price"] = dataset["old_price"].astype(int)
    # dataset.drop("Old Price", axis = 1)
    dataset["reviews"] = dataset["Reviews"].str.replace("out of 5", "").astype(float)
    # dataset.drop("Reviews", axis = 1)
    pattern_brand = r"^[a-zA-Z0-9\s]+"
    dataset["brand"] = dataset["Description"].str.extract(f"({pattern_brand})")
    pattern_ram = r"\d\s*[GB]+\s+RAM"
    dataset["RAM"] = dataset["Description"].str.extract(f"({pattern_ram})")
    dataset["RAM"] = dataset["RAM"].str.strip().str.replace("GB RAM", "")
    dataset["RAM"] = pd.to_numeric(dataset["RAM"], errors="coerce")
    pattern_rom = r"\b(128GB|64GB|256GB)\b"
    values = dataset["Description"].str.extract(f"({pattern_rom})").fillna("Unknown")
    dataset["storage"] = values[0]
    dataset["storage"] = dataset["storage"].str.replace("GB", "").str.strip()
    dataset["storage"] = pd.to_numeric(dataset["storage"], errors = "coerce")
    pattern_bat = r"[0-9]+\s*(mah|MAH|MaH|mAh|MAh)"
    result = dataset["Description"].str.extract(f"({pattern_bat})")
    dataset["Battery"] = result[0]
    dataset["Battery"] = dataset["Battery"].str.replace("mAh", "").str.replace("mah","").str.replace("MAH","").str.replace("MaH","").str.replace("MAh","")
    dataset["Battery"] = pd.to_numeric(data["Battery"], errors = "coerce")
    dataset = dataset.drop(columns = ["Price", "Old Price", "Reviews"])
    columns = ["id", "Description", "brand", "price", "old_price", "reviews", "RAM", "storage", "Battery", "source", "url"]
    dataset = dataset[columns]
    dataset.to_csv(r"..\data\clean_data\jumia_clean_phones.csv", index = False)
    
    return dataset


csv_path = "data/scraped_data/jumia_scraped_phones.csv"


phones_cleaning(csv_path)