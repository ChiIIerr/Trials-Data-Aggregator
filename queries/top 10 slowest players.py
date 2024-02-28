import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('F:/New folder/New folder/db.sqlite')
cursor = conn.cursor()

# Query the database to get the top 10 players with the most playtime per activity
query = '''
    SELECT character, activity, SUM(time_played_seconds) AS total_playtime
    FROM character_activity_stats
    GROUP BY character, activity
    HAVING COUNT(*) >= 7
    ORDER BY total_playtime DESC
    LIMIT 10
'''

cursor.execute(query)
results = cursor.fetchall()

# Output the results to the console
for row in results:
    character = row[0]
    activity = row[1]
    total_playtime = row[2]
    print(f'Character: {character}, Activity: {activity}, Total Playtime: {total_playtime}')

# Close the database connection
conn.close()