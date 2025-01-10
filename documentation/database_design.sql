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

CREATE TABLE jumia_phones (
Description  VARCHAR (500),
brand VARCHAR (500),
price VARCHAR (500),
old_price VARCHAR (500),
reviews VARCHAR (500),
RAM VARCHAR (500),
storage VARCHAR (500),
Battery VARCHAR(500)
)


CREATE TABLE IF NOT EXISTS jumia_microwaves (
descriptions  VARCHAR (500),
brand VARCHAR (500),
price VARCHAR (500),
Old_price VARCHAR (500),
capacity VARCHAR (500),
Reviews VARCHAR (500)
);