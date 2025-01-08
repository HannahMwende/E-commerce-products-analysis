-- Create table jumia_laptops --
CREATE TABLE IF NOT EXISTS jumia_laptops (
    name VARCHAR,
    brand VARCHAR(100),
    ram VARCHAR(10),
    rom VARCHAR(20),
    processor VARCHAR(50),
    screen_size VARCHAR(10),
    price FLOAT,
    reviews INT,
    ratings FLOAT
);

-- Create table jumia_fridges --
  CREATE TABLE IF NOT EXISTS jumia_fridges(
    name VARCHAR,
    brand VARCHAR(100),
    size_litres NUMERIC,
    doors NUMERIC,
    color VARCHAR(20),
    warranty_years NUMERIC,
    price FLOAT,
    reviews INT,
    ratings FLOAT
);