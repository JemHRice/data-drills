import sqlite3
import pandas as pd

# Read CSV
print("Reading CSV file...")
df = pd.read_csv("coffee_shop_sales.csv")

# Create database
conn = sqlite3.connect("coffee_shop.db")

# Import data
df.to_sql("sales", conn, if_exists="replace", index=False)

conn.close()
