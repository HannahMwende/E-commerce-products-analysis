# import libraries
import re
import time
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

# 1. scraping tvs data from kilimall website

# setting the base url & user agent
baseurl = "https://www.kilimall.co.ke/"
headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6439.0 AOL/9.7 AOLBuild/4343.4043.US Safari/537.36'}

# Scraping Tvs
# getting data with tvs product links
tv_links = []

for x in range(1,119):
    r = requests.get(f"https://www.kilimall.co.ke/category/televisions?backCid=5406&form=category&source=category|allCategory|televisions&page={x}", headers=headers)
    soup = BeautifulSoup(r.content, "html.parser")
    television_list = soup.find_all("div", class_ = "product-item")

    for television in television_list:
        for link in television.find_all("a", href = True):
            tv_links.append("https://www.kilimall.co.ke" + link["href"])


# getting other product details
tv_list = []
for x in range(1,119):
    #print("saving:" + str(x))
    r = requests.get(f"https://www.kilimall.co.ke/category/televisions?backCid=5406&form=category&source=category|allCategory|televisions&page={x}", headers=headers)
    soup = BeautifulSoup(r.content, "html.parser")
    television_list = soup.find_all("div", class_ = "info-box")

    for item in television_list:
        product_name = item.find("p", class_ = "product-title").text.strip()
        product_price = item.find("div", class_ = "product-price").text.strip()
        product_reviews = item.find("span", class_ = "reviews").text.strip()
        # define a dictionary
        tvs_dict = {
            "product_name": product_name,
            "product_reviews": product_reviews,
            "product_price": product_price,
            }
        tv_list.append(tvs_dict)

# Convert tv_list into a DataFrame
tv_df = pd.DataFrame(tv_list)

# Append tv_links as a new column
tv_df["product_link"] = tv_links

# save as csv
tv_df.to_csv(r"data\scraped\kilimall_tvs.csv", index = False, encoding = "utf-8")

print("successfully saved scraped kilimall_tvs.csv")


# 2. cleaning tvs data

# Load the csv data
tv_data = pd.read_csv(r"data\scraped\kilimall_tvs.csv")

# the product name includes emojis, that need to be edited out before cleaning
# Function to remove emojis and special characters
def remove_emojis(text):
    return re.sub(r"[^\w\s,.-]", "", text)

# apply the function to the "product_name" column
tv_data["clean_product_name"] = tv_data["product_name"].apply(remove_emojis)

# definine lists of available brands and size from kilimall website
tv_brands = ["vitron", "hisense","tcl", "generic", "vision","gld", "amtec", "samsung", "ctc", "skyworth", "syinix", "lg", "synix", "ailyons", "sony", "artel", "eoco", "& other fairies", "glaze", "infinix", "haier", "euroken", "iconix", "golden tech", "Microsoft lumia", "hotpoint", "fenghua", "konka", "power", "premier", "royal", "armco", "bic", "ccit", "ctroniq", "hifinit", "htc", "mara", "sonar", "trinity", "vitafoam", "x-tigi", "xiaomi", "wyinix", "sardin", "solar max", "globalstar", "tornado", "mg", "alyons", "solarmax", "ailynos", "von", "star x", "weyon", "itel", "ica", "skyview", "starmax", "aiylons", "skymax"]
tv_sizes = ["32", "43", "50", "55", "65", "75", "24", "40", "19", "26","22", "85", "70", "17", "105", "90"]
tv_types = ["smart", "digital", "semi-smart"]

# defining column extraction functions
# i) functions to match brands
def match_brand(product_name):
    for brand in tv_brands:
        if brand.lower() in product_name.lower():
            return brand
    return "unknown"

# ii) function to extract size by checking against available sizes list

def extract_size(product_name):
    for size in tv_sizes:
        if str(size) in product_name:  # Check if the size number exists in the title as a string
            return size
    return None

# iii) function to match TV type
def match_tv_type(product_name):
    for tv_type in tv_types:
        if tv_type.lower() in product_name.lower():
            return tv_type
    return "unknown"

# apply the functions to the DataFrame
tv_data["brand"] = tv_data["clean_product_name"].apply(match_brand)
tv_data["size"] = tv_data["clean_product_name"].apply(extract_size)
tv_data["tv_type"] = tv_data["clean_product_name"].apply(match_tv_type)

# remove brackets from product_reviews column
tv_data["product_reviews"] = tv_data["product_reviews"].apply(lambda x: x.strip("()"))

# cleaning the product_price column
tv_data["product_price"] = tv_data["product_price"].apply(lambda x: int(x.replace("KSh", "").replace(",", "").strip()))

# drop product_name
tv_data.drop(columns=["product_name"], inplace=True)

# check and remove duplicates
tv_data_no_dupes = tv_data.drop_duplicates(keep="first")

# change column names
tv_data_no_dupes = tv_data_no_dupes.rename(columns={"clean_product_name": "name", "product_reviews": "reviews", "product_price": "price", "tv_type": "type", "product_link": "url"})

# add a source column
tv_data_no_dupes["source"] = "kilimall" 

# add id coumn based on df index
tv_data_no_dupes["id"] = tv_data_no_dupes.index

# restructure the dataframe to have the columns in a more logical order
tv_data_no_dupes = tv_data_no_dupes[["id", "source", "name", "brand", "size", "type", "reviews", "price", "url"]]

# save the cleaned data to a csv file
tv_data_no_dupes.to_csv(r"data\clean\kilimall_tvs.csv", index=False)

print("successfully saved clean kilimall_tvs.csv")