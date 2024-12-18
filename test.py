# importing modules
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd

# setting the base url & user agent
baseurl = "https://www.kilimall.co.ke/"
headers = {'User-Agent' : 'https://explore.whatismybrowser.com/useragents/parse/4396-aol-browser-windows-webkit'} 

test_link = "https://www.kilimall.co.ke/listing/2253185-y30-tws-wireless-earphones-50-noise-cancelling-earpods-bluetooth-earphones-for-for-android-ios-smart-phones?skuId=16936489&source=search-hotSearch-earphone&isAd=ad"

r = requests.get(test_link, headers=headers)
soup = BeautifulSoup(r.content, "html.parser")

product_id = soup.find("div", class_ = "last-b-name name").text.strip()
name = soup.find("div", class_ = "product-title").text.strip()
current_price = soup.find("span", class_ = "sale-price").text.strip()
previous_price = soup.find("span", class_ = "del-price").text.strip()
reviews = soup.find("span", class_ = "reviews").text.strip()
rating = soup.find("span", class_ = "rate").text.strip()
    
# define a dictionary
tvs_dict = {
    "product_id": product_id,
    "name": name,
    "rating": rating,
    "reviews": reviews,
    "current_price": current_price,
    "previous_price": previous_price
    }

print(tvs_dict)
