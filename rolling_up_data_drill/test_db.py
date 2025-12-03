import sqlite3

try:
    conn = sqlite3.connect("coffee_shop.db")
    cursor = conn.cursor()

    # Check tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"Tables found: {tables}")

    # Check row count
    cursor.execute("SELECT COUNT(*) FROM sales")
    count = cursor.fetchone()[0]
    print(f"Row count in sales table: {count}")

    # Get first 5 rows
    cursor.execute("SELECT * FROM sales LIMIT 5")
    rows = cursor.fetchall()
    print(f"\nFirst 5 rows:")
    for row in rows:
        print(row)

    conn.close()
    print("\n✅ Database is valid and working!")

except Exception as e:
    print(f"❌ Error: {e}")
