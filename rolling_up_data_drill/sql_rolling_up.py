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


# Query to get the total sales by month by store
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


# Close connection
conn.close()
