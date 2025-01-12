# import libraries
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd

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

# Scraping cooktops and standing cookers
# (a) cooktop data
# getting data with cooktop_product links
cooktops_links = []

for x in range(1,137):
    r_cooktops_links = requests.get(f"https://www.kilimall.co.ke/category/cooktops?backCid=5461&form=category&source=search|enterSearch|Gas+Cookers&page={x}", headers=headers)
    soup_cooktops_links = BeautifulSoup(r_cooktops_links.content, "html.parser")
    cooktop_list = soup_cooktops_links.find_all("div", class_ = "product-item")

    for cooktop in cooktop_list:
        for link in cooktop.find_all("a", href = True):
            cooktops_links.append("https://www.kilimall.co.ke" + link["href"])

# getting other product details
ctops_list = [] # initializing an empty list that will store the results of the for loop

for x in range(1,137): # range of website pages that has cooktops product displayed on them
    #print("saving:" + str(x))
    r_cooktops = requests.get(f"https://www.kilimall.co.ke/category/cooktops?backCid=5461&form=category&source=search|enterSearch|Gas+Cookers&page={x}",headers=headers)
    soup_cooktops = BeautifulSoup(r_cooktops.content, "html.parser")
    cooktops_list = soup_cooktops.find_all("div", class_ = "info-box")
    #print(cooktops_list)
    for item in cooktops_list:
        cooktop_name = item.find("p", class_ = "product-title").text.strip()
        cooktop_price = item.find("div", class_ = "product-price").text.strip()
        cooktop_reviews = item.find("span", class_ = "reviews").text.strip()
        # define a dictionary
        cooktops_dict = {
            "cooktop_name": cooktop_name,
            "cooktop_reviews": cooktop_reviews,
            "cooktop_price": cooktop_price,
            }
        ctops_list.append(cooktops_dict)

# converting the list to a dataframe
cooktops_df = pd.DataFrame(ctops_list)

# Append tv_links as a new column
cooktops_df["product_link"] = cooktops_links

# save as csv
cooktops_df.to_csv(r"data\scraped\kilimall_cooktops.csv", index = False, encoding = "utf-8")

print("successfully saved scraped kilimall_cooktops.csv")

# (b) Standing cookers
# getting data with standing cooker product links
cooker_links = []

for x in range(1,33):
    r_cooker_links = requests.get(f"https://www.kilimall.co.ke/category/standing-gas-cookers?id=2207&form=category&page={x}", headers=headers)
    soup_cooker_links = BeautifulSoup(r_cooker_links.content, "html.parser")
    cooker_link_list = soup_cooker_links.find_all("div", class_ = "product-item")

    for cooker in cooker_link_list:
        for link in cooker.find_all("a", href = True):
            cooker_links.append("https://www.kilimall.co.ke" + link["href"])


# getting other product details
cookers_list = [] # initializing an empty list that will store the results of the for loop

for x in range(1, 33): # range of website pages that has cooktops product displayed on them
    #print("saving:" + str(x))
    r_cookers = requests.get(f"https://www.kilimall.co.ke/category/standing-gas-cookers?id=2207&form=category&page={x}", headers=headers)
    soup_cookers = BeautifulSoup(r_cookers.content, "html.parser")
    scookers_list = soup_cookers.find_all("div", class_ = "info-box")
    #print(len(standing_cookers_list))
    
    for item in scookers_list:
        standing_cooker_name = item.find("p", class_ = "product-title").text.strip()
        standing_cooker_price = item.find("div", class_ = "product-price").text.strip()
        standing_cooker_reviews = item.find("span", class_ = "reviews").text.strip()
        # define a dictionary
        standing_cooker_dict = {
            "standing_cooker_name": standing_cooker_name,
            "standing_cooker_reviews": standing_cooker_reviews,
            "standing_cooker_price": standing_cooker_price,
            }
        cookers_list.append(standing_cooker_dict)

# converting the list to a dataframe
cooker_df = pd.DataFrame(cookers_list)

# append cooker_links as a new column
cooker_df["product_link"] = cooker_links

# save as csv
cooker_df.to_csv(r"data\scraped\kilimall_standingcooker.csv", index = False, encoding = "utf-8")

print("successfully saved scraped kilimall_standingcooker.csv")

