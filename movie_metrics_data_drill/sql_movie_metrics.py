import sqlite3
import pandas as pd

# Create a connection to the SQLite database
conn = sqlite3.connect("movie_metrics.db")
cursor = conn.cursor()

# Query for names of columns of both tables
cursor.execute("PRAGMA table_info(users)")
columns = cursor.fetchall()
print("Column information:")
for col in columns:
    print(f"  {col[1]} ({col[2]})")

cursor.execute("PRAGMA table_info(activity)")
columns = cursor.fetchall()
print("Column information:")
for col in columns:
    print(f"  {col[1]} ({col[2]})")

# Tables need joining - foreign key (user_id in 'activity') corresponds with id in users
# Then extrapolate information, export as Excel to work with in Tableau


query1 = """
WITH first_last_movies AS(
    SELECT DISTINCT
        user_id,
        FIRST_VALUE(movie_name) OVER(PARTITION BY user_id ORDER BY date) AS name_first_finished,
        FIRST_VALUE(movie_name) OVER(PARTITION BY user_id ORDER BY date DESC) AS name_last_finished
    FROM activity
    WHERE finished = 1
)
SELECT 
    u.id AS user_id,
    MIN(u.created_at) AS user_created_at,
    MIN(CASE WHEN finished = 1 THEN a.date ELSE NULL END) AS first_date_finished,
    MIN(flm.name_first_finished) AS name_first_finished,
    MAX(CASE WHEN finished = 1 THEN a.date ELSE NULL END) last_date_finished,
    MIN(flm.name_last_finished) AS name_last_finished,
    COUNT(DISTINCT a.id) AS movies_started,
    COUNT(CASE WHEN finished = 1 THEN a.id ELSE NULL END) AS movies_finished,
    ROUND((COUNT(CASE WHEN finished = 1 THEN a.id ELSE NULL END) * 1.0 / COUNT(DISTINCT a.id)), 2) AS movie_percentage_finished

FROM users u
    LEFT JOIN activity a
        ON u.id = a.user_id
    LEFT JOIN first_last_movies flm
        ON flm.user_id = u.id

GROUP BY u.id
ORDER BY movie_percentage_finished DESC
"""

result1 = pd.read_sql_query(query1, conn)
result1.to_csv("result1.csv", index=False)
print(result1)

# Lessons Learned 1: join as late as possible. Early queries joined at all available instances, way too many joins
# LL2: Have the goal in mind; label all columns, use existing data as much as possible to calculate, then use
#   window functions and cte's to fill in the rest
