import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import re

baseurl = "https://www.kilimall.co.ke/"
headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.123 Safari/537.36 AOLShield/123.0.6312.3'}
microwaves_links = []

for x in range(1,42):
    r = requests.get(f"https://www.kilimall.co.ke/search?q=microwave&page={x}&source=search|enterSearch|microwave")
    soup = BeautifulSoup(r.content, "html.parser")
    microwaves_list = soup.find_all("div", class_ = "product-item")

    for microwave in microwaves_list:
        for link in microwave.find_all("a", href = True):
            microwaves_links.append("https://www.kilimall.co.ke" + link["href"])

print(len(microwaves_links)) 
import requests
from bs4 import BeautifulSoup
import csv

# Open a CSV file for writing
with open("kilimall_microwaves_scraped.csv", mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=["microwaves_name", "microwaves_reviews", "microwaves_price", "microwaves_links"])
    writer.writeheader()  # Write column headers
    
    for x in range(1, 150):  # Adjust the range as needed
        print(f"Processing page: {x}")
        r = requests.get(f"https://www.kilimall.co.ke/search?q=microwaves&page={x}&source=search|enterSearch|microwaves")
        soup = BeautifulSoup(r.content, "html.parser")
        items = soup.find_all("div", class_="info-box")
        
        for item in items:
            try:
                microwaves_name = item.find("p", class_="product-title").text.strip()
                microwaves_price = item.find("div", class_="product-price").text.strip()
                microwaves_reviews = item.find("span", class_="reviews").text.strip() if item.find("span", class_="reviews") else "No reviews"
                
                # Safely get the link
                link_tag = item.find("a", href=True)
                microwaves_links = "https://www.kilimall.co.ke" + link_tag["href"] if link_tag else "No link available"
                
                # Define a dictionary for each microwave
                microwave_dict = {
                    "microwaves_name": microwaves_name,
                    "microwaves_reviews": microwaves_reviews,
                    "microwaves_price": microwaves_price,
                    "microwaves_links": microwaves_links,
                }
                
                # Write the data directly to the CSV file
                writer.writerow(microwave_dict)
            except AttributeError:
                # Skip items with missing data
                continue

df = pd.read_csv(r"C:\Users\Vivian.Obino1\Desktop\e-commerce analysis\data\scraped\kilimall_microwaves_scraped.csv")
df_cleaned = df.dropna()
df['microwaves_price']= df['microwaves_price'].str.strip('KSh,')
df['microwaves_reviews'] = df['microwaves_reviews'].str.extract(r'(\d+)').astype(int)
# List of unwanted words (make sure all variations are included)
unwanted_words = ['clearance', 'CLEARANCE', 'SALE', 'OFFER', 'Best', 'CHOOSE', 'Offers', 'QUALITY', 'THE', 'FUTURE',
                  'EMBRACE', 'LATEST', 'TREND', 'MEGASALE', 'Buy', 'NOW', 'AND', 'ENJOY', 'UPTO', 'NEW', 'IMPROVED', 
                  'STAY', 'LOCKED', 'WITH', 'STOCK', 'kilimall', 'special', 'MAKE', 'YOUR', 'HOUSE', 'FEEL', 'LIKE', 
                  'Super', 'deal', 'quality', 'RESTOCKED', 'Share', 'this', 'product', 'Best', 'ARRIVALS', 'HURRY', 
                  'AND', 'PICK', 'YOURS', 'LIMITED', 'AN', 'NO', 'OTHER', 'PRICE', 'REDUCED', 'NOWBLACK', 'Angry', 
                  'mama', 'kitchen','EXPERIENCE', 'New', 'Arrival', 'Classy', 'sale', 'offer', 'best', 'discount', 
                  'cheap', 'deal', 'SALE','Promotions','OFFER','offer','TRUSTED','SOURCE','UPGRADE','THESE','TOP','DURABLE','LISTING','ON','Cooking','End','Original','OF','ALL','affordable']

# Convert the unwanted_words list to lowercase for case-insensitive comparison
unwanted_words = set(word.lower() for word in unwanted_words)

# Function to remove unwanted words from a text
def remove_unwanted_words(text):
    if not isinstance(text, str):
        return text  # Return as is if not a string
    words = text.split()  # Split the text into individual words
    cleaned_words = [word for word in words if word.lower() not in unwanted_words]  # Filter out unwanted words
    return ' '.join(cleaned_words)  # Join the cleaned words back into a string

file_path =(r"C:\Users\Vivian.Obino1\Desktop\e-commerce analysis\data\scraped\kilimall_microwaves_scraped.csv") # Replace with your actual file path
df = pd.read_csv(file_path)

# Apply the function to the relevant column (e.g., 'microwaves_name')
df['microwaves_name'] = df['microwaves_name'].apply(remove_unwanted_words)

# Save the updated DataFrame to a new CSV file
output_file_path = 'kilimall_clean_microwaves.csv'
df.to_csv(output_file_path, index=False)
# Load the CSV file into a DataFrame
file_path = 'kilimall_clean_microwaves.csv'  # Replace with your CSV file path
df = pd.read_csv(file_path)

# Function to clean text
def clean_text(text):
    # Remove punctuation, emojis, and parentheses
    text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
    text = re.sub(r'\s*\([^)]*\)\s*', '', text)  # Remove parentheses and their content
    text = re.sub(r'[^\x00-\x7F]+', '', text)  # Remove emojis and non-ASCII characters
    return text.strip()
  
# Extract brand name and clean it
def extract_brand_name(text):
    words = clean_text(text).split()[:5]  # Get first five words
    return ' '.join(words)
def extract_capacity(text):
    if not isinstance(text, str):
        return None
    match = re.search(r'\b(\d+\.?\d*)\s*(L|Ltrs|Liters|Litres)\b', text, re.IGNORECASE)
    return match.group(1) if match else None
def remove_parentheses(text):
    if not isinstance(text, str):
        return text
    return re.sub(r'\s*\([^)]*\)\s*', '', text)

# Apply the function to the 'microwave_reviews' column


# Apply the brand name extraction
df['microwaves_name'] = df['microwaves_name'].apply(extract_brand_name)  # Replace 'ColumnName' with the relevant column name
df['Capacity'] = df['microwaves_name'].apply(extract_capacity) 
# Lines to remove
lines_to_remove = [*range(12, 19), 83, 84, 127, 128, 131, *range(155, 160), *range(608, 624), 
                   *range(652, 658), *range(675, 683), *range(686, 692), 461, 463, *range(187, 197), 
                   *range(257, 267), 275, 342, 343, *range(769, 806), *range(832, 839)]

# Drop specified rows (subtract 1 to account for zero-based index in Python)
df.drop(index=[i-1 for i in lines_to_remove], inplace=True, errors='ignore')
# Rename the columns
df.columns = ['brand', 'reviews', 'price', 'microwaves_url','capacity']   
# Save the cleaned DataFrame to a new CSV file
output_file_path = 'kilimall_clean_microwaves.csv'
df.to_csv(output_file_path, index=False)
df['price']= df['price'].str.strip('KSh,')
def retain_text_in_parentheses(text):
    if not isinstance(text, str):
        return text  # Return as is if not a string
    return re.sub(r'[()]', '', text)  # Remove only parentheses

# Apply the function to the desired column
df['reviews'] = df['reviews'].apply(retain_text_in_parentheses)
df['price'] = df['price'].replace({',': ''}, regex=True)
df.to_csv('kilimall_clean_microwaves.csv', index=False)


