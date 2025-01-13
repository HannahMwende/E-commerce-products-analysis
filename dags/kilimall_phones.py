import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import re

baseurl = "https://www.kilimall.co.ke/"
headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.123 Safari/537.36 AOLShield/123.0.6312.3'}

phones_list = []  # List to store phone details

for x in range(1, 150):  # Loop through the pages
    print(f"Saving page: {x}")
    r_phone = requests.get(f"https://www.kilimall.co.ke/search?q=phones&page={x}&source=search|enterSearch|phones")
    soup_phone = BeautifulSoup(r_phone.content, "html.parser")
    
    # Find all phone product containers
    phone_items = soup_phone.find_all("div", class_="info-box")
    
    for item in phone_items:
        try:
            phones_name = item.find("p", class_="product-title").text.strip()
            phones_price = item.find("div", class_="product-price").text.strip()
            phones_reviews = item.find("span", class_="reviews").text.strip() if item.find("span", class_="reviews") else "No reviews"
            a_tag = item.find("a", href=True)
            phones_url = "https://www.kilimall.co.ke" + a_tag["href"] if a_tag else "No URL available"
            
            
            # Create a dictionary for each phone
            phones_dict = {
                "phones_name": phones_name,
                "phones_reviews": phones_reviews,
                "phones_price": phones_price,
                "phones_url": phones_url,
            }
            
            phones_list.append(phones_dict)
        except AttributeError:
            # Skip items with missing details
            continue

print(f"Total phones scraped: {len(phones_list)}")
dataframe = pd.read_csv(r"C:\Users\Vivian.Obino1\Desktop\e-commerce analysis\data\scraped\kilimall_scraped_phones.csv")
dataframe_cleaned = dataframe.drop_duplicates()
dataframe['phones_price']= dataframe['phones_price'].str.strip('KSh,')
dataframe['phones_reviews'] = dataframe['phones_reviews'].str.extract(r'(\d+)').astype(int)

# List of words to remove
words_to_remove = [
    'xmas', 'sale', 'new', 'arrival', 'brand', 'refurbished', 'year', 'sales', 'promotion', 
    'original', 'super', 'limited', 'offer', 'holiday', 'offers', 'black', 'friday', 'deals', 
    'arrivals', 'christmas', 'smart', 'end', 'free', 'charger', 'official', 'phones', 'special', 
    'buy', 'big', 'launch', 'november','Limited','OFFER','Brand','New','Face','unlock','Arrival','Black','Friday','NEW','CHEAP','SMART','BLACK','NOVEMBER','OFFERS','Unlocked','Phones','BRAND','Original','Free','Charger','Exclusive','Special','Offer','Phone','LIMITED'
    'anniversary','Arrivals','Best','for','a','Refirbishe','time','BRANDNEW','CHRISTMAS','HOLIDAY','OFFEROFFER','REFURBISHED','END','YEAR','Buy','Official','Offiicial','BIG','DEALS','ARRIVALS','OFFERS','Unlock','Google','system','Techweek','mobile','Harambee','phone','Global','Version'
    ,'Refurbishe','Systems','Edition','Honor','ORIGINAL','Freeyond','machine','The','Refur','version','Latest','SOWHAT'
   ,'GRAB','QUICK','SALE','Firmware','Super','Sale','Badili','FreeYond','Smartphone','HONOR','Cell','net','Top','International','edition','high','matching','EASTER','Sharp','YOUR'
   ,'certified','Certified','DISCOUNT','SPECIAL','SPECIAL','ARRIVAL','Renovate','SEALED','OFFERSBRAND','EXPANDABLE','FRIDAY','FLASH','LIMTED'
]


# Compile regex pattern
pattern = r'\b(?:' + '|'.join(map(re.escape, words_to_remove)) + r')\b'

# Functions to clean the data
def remove_brackets(column):
    return column.str.replace(r'\[.*?\]', '', regex=True)

def clean_column(column):
    return column.str.replace(pattern, '', flags=re.IGNORECASE).str.replace(r'\s+', ' ', regex=True).str.strip()

def remove_symbols(column):
    return column.str.replace(r'[!"+]', '', regex=True)

# Apply cleaning functions
dataframe['phones_name'] = remove_brackets(dataframe['phones_name'])  # Remove text in square brackets
dataframe['phones_name'] = clean_column(dataframe['phones_name'])    # Remove unwanted words
dataframe['phones_name'] = remove_symbols(dataframe['phones_name'])  # Remove special characters

# Save cleaned data to a new CSV file
cleaned_file_path = 'kilimall_clean_phones.csv'
dataframe.to_csv(cleaned_file_path, index=False)
dataframe["phones_name"] = dataframe["phones_name"].str.replace("Refurbished", "")
dataframe["phones_name"] = dataframe["phones_name"].str.replace(pattern, '', regex=True).str.strip()
# Function to clean the phones_name column
def clean_phones_name(name):
    # Remove unwanted characters: (), {}, !!, [], "", and numbers before the first letter
    name = re.sub(r'[\(\)\{\}\[\]\"!]+', '', name)  # Remove parentheses, brackets, quotes, and exclamation marks
    name = re.sub(r'^\d+\s*', '', name)  # Remove numbers at the start of the string
    return name.strip()  # Remove leading and trailing whitespace

# Apply the cleaning function to the phones_name column
dataframe["phones_name"] = dataframe["phones_name"].apply(clean_phones_name)
def remove_first_quote(name):
    return re.sub(r'^[\'"]', '', name)  # Remove the first single or double quote if present

# Apply the function to the phones_name column
dataframe["phones_name"] = dataframe["phones_name"].apply(remove_first_quote)
# Function to ensure the first character is an alphabet
def remove_non_alphabet_start(name):
    return re.sub(r'^[^a-zA-Z]+', '', name)  # Remove any non-alphabet characters from the start

# Apply the function to the phones_name column
dataframe["phones_name"] = dataframe["phones_name"].apply(remove_non_alphabet_start)
# Delete the first row
dataframe = dataframe.drop(index=0)

# Reset index if you want to reindex the DataFrame
dataframe = dataframe.reset_index(drop=True)
ram_pattern = r'\b(2|3|4|6|8|12|24)\s*GB'

# Function to extract RAM size
def extract_ram(phones_name):
    ram = re.findall(ram_pattern, phones_name)  # Find all occurrences that match the pattern
    return ram[0] if ram else None  # If found, return the first match; else return None

storage_pattern = r'\b(16|32|64|128|256)\s*GB'
def extract_storage(phones_name):
    storage = re.findall(storage_pattern, phones_name)  # Find all occurrences that match the pattern
    return storage[0] if storage else None  # If found, return the first match; else return None
def extract_brand(phones_name):
    # Split the name into words and take the first two or three
    words = phones_name.split()
    return ' '.join(words[:3])  # Return the first three words (or fewer if not available)
def extract_battery(phones_name):
    # Search for a pattern that captures numbers followed by 'mAh'
    match = re.search(r'(\d+)\s*mAh', phones_name)
    if match:
        return match.group(1)  # Return the first captured group (the number)
    return None  # If no match, return None

# Apply the function to the 'phones_name' column and create a new 'battery' column
dataframe['battery'] = dataframe['phones_name'].apply(extract_battery)
# Apply the function to the 'phones_name' column and create a new 'storage' column
dataframe['ram'] = dataframe['phones_name'].apply(extract_ram)
dataframe['storage'] = dataframe['phones_name'].apply(extract_storage)
# Apply the function to the 'phones_name' column and create a new 'brand' column
dataframe['brand'] = dataframe['phones_name'].apply(extract_brand)
dataframe
dataframe['brand'] = dataframe['phones_name'].apply(extract_brand)
dataframe['storage'] = dataframe['phones_name'].apply(extract_storage)
dataframe['ram'] = dataframe['phones_name'].apply(extract_ram)
dataframe['battery'] = dataframe['phones_name'].apply(extract_battery)
dataframe.rename(columns={'phones_reviews': 'reviews'}, inplace=True)

new_dataframe = dataframe[['brand', 'storage', 'ram', 'phones_price', 'reviews', 'battery']]
new_dataframe.rename(columns ={'phones_price':'price'}, inplace=True)
new_dataframe.to_csv("kilimall_clean_phones.csv", index=False)






