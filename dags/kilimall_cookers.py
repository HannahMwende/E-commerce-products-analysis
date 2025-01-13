# import libraries
import re
import time
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

# 1. Scraping cooktops and standing cookers from kilimall website
# setting the base url & user agent

baseurl = "https://www.kilimall.co.ke/"
headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.123 Safari/537.36 AOLShield/123.0.6312.3'} 

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

#print("successfully saved scraped kilimall_cooktops.csv")

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

#print("successfully saved scraped kilimall_standingcooker.csv")

# 2. cleaning cookers data

# (a) cooktops
# load the csv data
cooktops_df = pd.read_csv(r'data\scraped\kilimall_cooktops.csv')

# function to remove emojis and special characters
def remove_emojis(text):
    return re.sub(r'[^\w\s,.-]', '', text)

# apply the function to 'cooktop_name'
cooktops_df['clean_cooktop_name'] = cooktops_df['cooktop_name'].apply(remove_emojis)

# drop duplicates
cooktop_no_dupes_df = cooktops_df.drop_duplicates()

# create a list of available brands and types to help with creating additional columns in the df
cooktop_brands = ['generic', 'nunix', 'ailyons', 'eurochef', 'rashnik', 'sokany', 'ramtons', 'eurochef', 'mara', 'premier', 'sweet home', 'edison', 'sayona', 'roch', 'silvercrest', 'hisense', 'ipcone', 'kitchen37', 'toseeu', 'amaze', 'microsoft lumia', 'fashion king', 'mika', 'rebune', 'annov', 'euroken', 'hotpoint', 'jamesport', 'jtc', 'jikokoa', 'lenovo', 'sterling', 'u7', 'vitron', 'fenghua', '& other fairies', 'ahitar', 'bosch', 'gt sonic', 'rebune', 'thl', 'vention', 'weiqin', 'kilimall', 'armco', 'aucma', 'alldocube', 'amazon', 'androidly', 'Starlux', 'Lyons', 'Edenberg', 'boko', 'jiko okoa', 'xiaomi', 'euro chef', 'jiko koa', 'von', 'ampia', 'intex', 'veigapro', 'silver crest', 'amaize', 'jamespot', 'ilyons', 'ramtoms', 'ohms', 'velton', 'jx', 'sc']
cooktop_types = ['gas', 'electric', 'electric and gas', 'not specified']

# defining extraction functions
# (i) functions to match brands
def match_cooktop_brand(cooktop_name):
    for brand in cooktop_brands:
        if brand.lower() in cooktop_name.lower():
            return brand
    return 'unknown'


# (ii) function to match cooktop type
def match_cooktop_type(cooktop_name):
    for cooktop_type in cooktop_types:
        if cooktop_type.lower() in cooktop_name.lower():
            return cooktop_type
    return 'unknown'

# apply the functions to the DataFrame
cooktop_no_dupes_df = cooktop_no_dupes_df.copy()
cooktop_no_dupes_df.loc[:, 'brand'] = cooktop_no_dupes_df['clean_cooktop_name'].apply(match_cooktop_brand)
cooktop_no_dupes_df.loc[:, 'cooktop_type'] = cooktop_no_dupes_df['clean_cooktop_name'].apply(match_cooktop_type)

# remove brackets from product_reviews
cooktop_no_dupes_df['cooktop_reviews'] = cooktop_no_dupes_df['cooktop_reviews'].apply(lambda x: x.strip('()'))

# cleaning the cooktop_price column
cooktop_no_dupes_df['cooktop_price'] = cooktop_no_dupes_df['cooktop_price'].apply(lambda x: int(x.replace('KSh', '').replace(',', '').strip()))

# drop cooktop_name
cooktop_no_dupes_df.drop(columns=['cooktop_name'], inplace=True)

# rename the columns
cooktop_no_dupes_df = cooktop_no_dupes_df.rename(columns={'clean_cooktop_name': 'name', 'cooktop_reviews': 'reviews', 'cooktop_price': 'price','cooktop_type': 'type', 'product_link': 'url'})

# restructure the dataframe to have the columns in a more logical order
cooktop_no_dupes_df = cooktop_no_dupes_df[['name', 'brand', 'type', 'reviews', 'price', 'url']]

# add a category column
cooktop_no_dupes_df['category'] = 'cooktop'


# (a) standing cooker
# load the csv data
standingcooker_df = pd.read_csv(r'data\scraped\kilimall_standingcooker.csv')

# apply the function that removes emojis to 'standing_cooker_name'
standingcooker_df['clean_standingcooker_name'] = standingcooker_df['standing_cooker_name'].apply(remove_emojis)

# drop duplicates
standingcooker_no_dupes_df = standingcooker_df.drop_duplicates()

# create a list of available brands, types and oven capacity to create additional columns in the df
standingcooker_brands = ['generic', 'nunix', 'mika', 'hotpoint', 'eurochef', 'ramtons', 'premier', 'volsmart', 'sayona', 'haier', 'hisense', 'roch', 'bruhm', 'euroken', 'ailyons', 'amaze', 'icecool', 'exzel', 'lg', 'rebune', 'sarah', 'jiko okoa', 'von', 'bjs', 'sarahtech', 'rashnik', 'vision', 'sarah tech', 'globalstar', 'unitech', 'tlac', 'global tech', 'meko', 'beko', 'bosch', 'nunnix', 'starlux', 'armco', 'solstar', 'silver crest', 'jikokoa', 'eroucheif', 'primier', 'icook']
standingcooker_types = ['3 gas+1 electric', '4 gas', '2 gas+2 electric']
oven_capacities = ['40-60 l', 'without oven', '30-40 l', '10-20 l']

# defining extraction functions
# (i) function to match brands
def match_standingcooker_brand(standing_cooker_name):
    for brand in standingcooker_brands:
        if brand.lower() in standing_cooker_name.lower():
            return brand
    return 'unknown'


# (ii) function to match standing cooker type
def match_standingcooker_type(standing_cooker_name):
    for standingcooker_type in standingcooker_types:
        if standingcooker_type.lower() in standing_cooker_name.lower():
            return standingcooker_type
    return 'unknown'

# (iii) function to match oven capacity
def match_capacity(standing_cooker_name):
    for capacity in oven_capacities:
        if capacity.lower() in standing_cooker_name.lower():
            return capacity
    return 'unknown'

# apply the functions to the DataFrame
standingcooker_no_dupes_df = standingcooker_no_dupes_df.copy()

# modify the DataFrame
standingcooker_no_dupes_df['brand'] = standingcooker_no_dupes_df['clean_standingcooker_name'].apply(match_standingcooker_brand)
standingcooker_no_dupes_df['standing_cooker_type'] = standingcooker_no_dupes_df['clean_standingcooker_name'].apply(match_standingcooker_type)
standingcooker_no_dupes_df['oven_capacity'] = standingcooker_no_dupes_df['clean_standingcooker_name'].apply(match_capacity)

# remove brackets from standing_cooker_reviews
standingcooker_no_dupes_df['standing_cooker_reviews'] = standingcooker_no_dupes_df['standing_cooker_reviews'].apply(lambda x: x.strip('()'))

# cleaning the standing_cooker_price column
standingcooker_no_dupes_df['standing_cooker_price'] = standingcooker_no_dupes_df['standing_cooker_price'].apply(lambda x: int(x.replace('KSh', '').replace(',', '').strip()))

# drop standing_cooker_name
standingcooker_no_dupes_df.drop(columns=['standing_cooker_name'], inplace=True)

# add category column
standingcooker_no_dupes_df['category'] = 'standing cooker'

# renaming columns
standingcooker_no_dupes_df = standingcooker_no_dupes_df.rename(columns={'standing_cooker_reviews':'reviews', 'standing_cooker_price':'price', 'product_link':'url', 'clean_standingcooker_name':'name', 'standing_cooker_type':'type', 'oven_capacity':'capacity'})

# structure the df columns
standingcooker_no_dupes_df = standingcooker_no_dupes_df[['name', 'brand', 'type', 'capacity', 'reviews', 'price', 'url', 'category']]

# 3. combining cooktops and standing cookers into one df

# add the missing capcity column to cooktop df with null values
cooktop_no_dupes_df.insert(3, 'capacity', np.nan)

# merge the two df 
cookers = pd.concat([cooktop_no_dupes_df, standingcooker_no_dupes_df], ignore_index=True)

# add id and source columns to cookers df

# define a function that add id and source column
def id_source (df):
    df.insert(0, 'id', cookers.index)
    df.insert(1, 'source', 'kilimall')

    return df

# adding the columns to cookers df
id_source(cookers)

# restructure the df columns
cookers = cookers[['id', 'name', 'price', 'brand', 'capacity', 'type', 'reviews', 'category', 'source', 'url']]

# save the cleaned data to a csv file
cookers.to_csv(r'data\clean\kilimall_cookers.csv', index=False)

#print('successfully saved clean kilimall_cookers.csv')