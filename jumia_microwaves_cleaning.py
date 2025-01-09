def microwaves_cleaning(csv_path):    
    data = pd.read_csv(csv_path)
    data["Price"] = data["price"].str.replace("KSh", "").str.replace(",","")
    data["Reviews"] = data["ratings"].str.replace(" out of 5", "")
    data["Old_price"] = data["old_price"].str.replace("KSh ", "").str.replace(",", "")
    data = data.drop(columns = ["price", "ratings", "old_price"])
    pattern = r"^[a-zA-Z]+"
    data["brand"] = data["descriptions"].str.extract(f"({pattern})")
    pattern_cap = r'(\d+)\s*(?=litres|l|L)'
    result = data["descriptions"].str.extract(f"({pattern_cap})")
    data["capacity"] = result[0]
    columns = ["descriptions", "brand", "Price", "Old_price", "capacity", "Reviews"]
    data = data[columns]
    data.to_csv("cleaned_microwaves.csv", index = False)
    return data