import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('F:/New folder/New folder/db.sqlite')
cursor = conn.cursor()

# Query the database to get the top 10 players in playtime
query = '''
SELECT character, SUM(time_played_seconds) AS total_playtime
FROM character_activity_stats
GROUP BY character
ORDER BY total_playtime DESC
LIMIT 10
'''

# Execute the query
cursor.execute(query)

# Fetch all the results
results = cursor.fetchall()

# Output the top 10 players in playtime
for row in results:
    print(row[0], row[1])

# Close the database connection
conn.close()