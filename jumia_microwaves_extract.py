import requests
from bs4 import BeautifulSoup
import pandas as pd

def web_scraping_jumia_microwaves(url):

    descriptions = []
    prices = []
    older_prices = []
    reviews_list = []


    r = requests.get(url)
    print(f"Fetching data from {url} Status {r}")

    if r.status_code == 200:
        soup = BeautifulSoup(r.text, "html.parser")

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
        reviews = soup.find_all("div", class_="in")
        for rev in reviews:
            reviews_list.append(rev.text.strip())
    else:
        print(f"Failed to fetch data from {first_page_url}")
    

    rows = list(zip(descriptions, prices, older_prices, reviews_list))
    return rows



def collect_data_microwaves():
    data = web_scraping_jumia_microwaves(url)

    df = pd.DataFrame(data, columns = ['descriptions', 'price', 'old_price', 'ratings'])

    df.to_csv("jumia_microwaves.csv", index = False)


