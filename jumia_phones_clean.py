def phones_cleaning(csv_path):
    dataset = pd.read_csv(csv_path)
    dataset["price"] = dataset["Price"].str.replace("KSh", "").str.replace(",", "")
    # dataset.drop("Price", axis = 1)
    dataset["old_price"] = dataset["Old Price"].str.replace("KSh", "").str.replace(",", "")
    # dataset.drop("Old Price", axis = 1)
    dataset["reviews"] = dataset["Reviews"].str.replace("out of 5", "")
    # dataset.drop("Reviews", axis = 1)
    pattern_brand = r"^[a-zA-Z0-9\s]+"
    dataset["brand"] = dataset["Description"].str.extract(f"({pattern_brand})")
    pattern_ram = r"\d\s*[GB]+\s+RAM"
    dataset["RAM"] = dataset["Description"].str.extract(f"({pattern_ram})")
    pattern_rom = r"\b(128GB|64GB|256GB)\b"
    values = dataset["Description"].str.extract(f"({pattern_rom})").fillna("Unknown")
    dataset["storage"] = values[0]
    pattern_bat = r"[0-9]+\s*(mah|MAH|MaH|mAh|MAh)"
    result = dataset["Description"].str.extract(f"({pattern_bat})")
    dataset["Battery"] = result[0]
    dataset = dataset.drop(columns = ["Price", "Old Price", "Reviews"])
    columns = ["Description", "brand", "price", "old_price", "reviews", "RAM", "storage", "Battery"]
    dataset = dataset[columns]
    dataset.to_csv("cleaned_phones_jumia", index = False)
    
    return dataset
