# Task is to take data, and find each stores month on month change in sales
# Query 2 finds the month on month change for Astoria in May, but can be used for all months and all stores
# Query 3 orders total sales by store, allowing for analysis of products within stores
# Query 4 orders total sales across all stores, allowing for analysis of products across franchise

import sqlite3
import pandas as pd

# Create a connection to the SQLite database
conn = sqlite3.connect("coffee_shop.db")
cursor = conn.cursor()

# Query for column names
cursor.execute("PRAGMA table_info(sales)")
columns = cursor.fetchall()
print("Column information:")
for col in columns:
    print(f"  {col[1]} ({col[2]})")  # col[1] = name, col[2] = type


# Query to get the total sales by month by store, and then for Astoria as per the task
query2 = """
WITH monthly_totals AS (
    SELECT
        SUBSTR(date, -4) || '-' || SUBSTR(date, INSTR(date, '/') + 1, 2) as month_year,
        store,
        SUM(sales) as total_sales
    FROM sales
    GROUP BY month_year, store
)
SELECT
    month_year,
    store,
    total_sales,
    LAG(total_sales) OVER (PARTITION BY store ORDER BY month_year) as prev_month_sales,
    total_sales - LAG(total_sales) OVER (PARTITION BY store ORDER BY month_year) as mom_change
FROM monthly_totals
WHERE store = 'Astoria' AND month_year IN ('2023-04', '2023-05')
ORDER BY store, month_year
"""
result2 = pd.read_sql_query(query2, conn)
print(result2)

# As extra task, let's find top 3 highest selling items for each store

# Query to get total sales by product by store, sort and present each store's top 3

query3 = """
WITH product_totals AS (
    SELECT
        store,
        product,
        SUM(sales) as total_sales
    FROM sales
    GROUP BY store, product
),
ranked_products AS (
    SELECT
        store,
        product,
        total_sales,
        ROW_NUMBER() OVER (PARTITION BY store ORDER BY total_sales DESC) as rank
    FROM product_totals
)
SELECT store, product, total_sales, rank
FROM ranked_products
WHERE rank <=5
ORDER BY store, rank
"""
result3 = pd.read_sql_query(query3, conn)
print(result3)

# Assuming this is a franchise, let's find the top 10 selling products over all restuarants

query4 = """
WITH ranked_total_products as (
    SELECT
        product,
        SUM(sales) as total_product_sales,
        ROW_NUMBER() OVER (ORDER BY SUM(sales) DESC) as rank
    FROM sales
    GROUP BY product
)
SELECT product, total_product_sales, rank
FROM ranked_total_products
WHERE total_product_sales >= 50000
ORDER BY rank
"""
result4 = pd.read_sql_query(query4, conn)
print(result4)


# Close connection
conn.close()
