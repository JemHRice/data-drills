import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('coffee_shop.db')

# Read and execute your SQL file
with open('rolling_up_real.sql', 'r') as f:
    sql_content = f.read()

# Split by semicolons and execute each query
queries = [q.strip() for q in sql_content.split(';') if q.strip() and not q.strip().startswith('--')]

for query in queries:
    try:
        print(f"\n{'='*60}")
        print(f"Executing: {query[:100]}...")
        print('='*60)
        
        result = pd.read_sql_query(query, conn)
        print(result.to_string())
        
    except Exception as e:
        print(f"Error: {e}")

conn.close()
