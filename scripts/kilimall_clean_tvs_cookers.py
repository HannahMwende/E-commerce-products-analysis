# importing libararies
import time
import pandas as pd
import numpy as np 
import re

# 1. cleaning tvs data

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

# save the cleaned data to a csv file
cookers.to_csv(r'data\clean\kilimall_cookers.csv', index=False)

print('successfully saved clean kilimall_cookers.csv')