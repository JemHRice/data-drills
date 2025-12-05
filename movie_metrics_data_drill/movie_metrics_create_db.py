import sqlite3
import pandas as pd

# Create a connection to the SQLite database
conn = sqlite3.connect("movie_metrics.db")

# Read the CSV files
users_df = pd.read_csv("users.csv")
activity_df = pd.read_csv("activity.csv")

# Write the DataFrames to SQLite tables
users_df.to_sql("users", conn, if_exists="replace", index=False)
activity_df.to_sql("activity", conn, if_exists="replace", index=False)

# Verify the data was loaded
cursor = conn.cursor()

# Check users table
cursor.execute("SELECT COUNT(*) FROM users")
users_count = cursor.fetchone()[0]
print(f"Users table: {users_count} rows")

# Check activity table
cursor.execute("SELECT COUNT(*) FROM activity")
activity_count = cursor.fetchone()[0]
print(f"Activity table: {activity_count} rows")

# Show table structures
print("\nUsers table structure:")
cursor.execute("PRAGMA table_info(users)")
for col in cursor.fetchall():
    print(f"  {col[1]} ({col[2]})")

print("\nActivity table structure:")
cursor.execute("PRAGMA table_info(activity)")
for col in cursor.fetchall():
    print(f"  {col[1]} ({col[2]})")

# Close connection
conn.close()

print("\nDatabase created successfully!")
