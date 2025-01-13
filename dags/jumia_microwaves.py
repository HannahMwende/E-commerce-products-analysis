import requests
from bs4 import BeautifulSoup
import pandas as pd

def web_scraping_jumia_microwaves(url, baseurl):

    descriptions = []
    prices = []
    older_prices = []
    reviews_list = []
    urls = []

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
        print(f"Failed to fetch data from {url}")
    
    for i in range(2, 51):
        page_url = f"{baseurl}{i}#catalog-listing"
        r = requests.get(page_url)
        print(f"Fetching data from: {page_url} - Status: {r.status_code}")
    
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
        print(f"Failed to fetch data from {page_url}")

    rows = list(zip(descriptions, prices, older_prices, reviews_list, urls))
    return rows



def collect_data_microwaves():
    data = web_scraping_jumia_microwaves(url, baseurl)

    df = pd.DataFrame(data, columns = ['descriptions', 'price', 'old_price', 'ratings', 'urls'])

    df.to_csv(r"..\data\scraped_data\jumia_scraped_microwaves.csv", index = False)


url = "https://www.jumia.co.ke/small-appliances-microwave/"
# baseurl = "https://www.jumia.co.ke/catalog/?q=microwaves&amp;page="
baseurl = "https://www.jumia.co.ke/catalog/?q=microwaves&page="


collect_data_microwaves()



def jumia_microwaves_cleaning(csv_path):
    data = pd.read_csv(csv_path)
    data.head()
    data["Price"] = data["price"].str.replace("KSh", "").str.replace(",","")
    data["Reviews"] = data["ratings"].str.replace(" out of 5", "")
    data["Old_price"] = data["old_price"].str.replace("KSh ", "").str.replace(",", "")
    data["Price"] = data["Price"].astype(int)
    data["Reviews"] = data["Reviews"].astype(float)
    data["Old_price"] = data["Old_price"].astype(int)
    pattern = r"^[a-zA-Z]+"
    data["brand"] = data["descriptions"].str.extract(f"({pattern})")
    pattern_cap = r'(\d+)\s*(?=litres|l|L)'
    result = data["descriptions"].str.extract(f"({pattern_cap})")
    data["capacity"] = result[0]
    data["capacity"] = data["capacity"].str.strip().astype(float)
    id = [i for i in range(1, len(data) + 1)]
    data["id"] = id
    data["id"] - data["id"].astype(int)
    data["source"] = ["Jumia"] * len(data)
    data = data.drop(columns = ["price", "ratings", "old_price"])
    columns = ["id", "descriptions", "brand", "Price", "Old_price", "capacity", "Reviews", "source", "urls"]
    data = data[columns]
    data.rename(columns = {"descriptions" : "description", "Price" : "price", "Old_price" : "old_price", "Reviews" :  "reviews"})
    data = data[columns]
    data.to_csv(r"..data\clean_data\jumia_clean_microwaves.csv", index=False)

    return data


csv_path = r"..data\scraped_data\jumia_scraped_microwaves.csv"


jumia_microwaves_cleaning(csv_path)