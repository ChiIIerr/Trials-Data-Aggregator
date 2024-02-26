import asyncio
import aiohttp
import sqlite3
import json

# Database path
DATABASE_PATH = 'F:/New folder/New folder/db.sqlite'

# API key
api_key = input("Enter your API key: ")

# Number of concurrent API calls
concurrent_calls = 40

# Semaphore for controlling concurrent API calls
semaphore = asyncio.Semaphore(concurrent_calls)

# Retry pool for failed activities
retry_pool = []

async def call_api(api_key, activity_id, semaphore, retry_pool):
    headers = {
        'X-API-Key': api_key
    }
    url = f'https://www.bungie.net/Platform/Destiny2/Stats/PostGameCarnageReport/{activity_id}/'
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    retry_pool.append(activity_id)
                    return None

async def process_activity(api_key, activity_id, semaphore, conn, retry_pool):
    json_data = await call_api(api_key, activity_id, semaphore, retry_pool)
    if json_data is not None:
        activity_details = json_data['Response']['activityDetails']
        if activity_details['mode'] == 84:
            print(f"Found activity with mode 84: {activity_id}")
            insert_data(conn, activity_id, json_data)

def create_schema(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS activity
                     (activity_id INTEGER PRIMARY KEY UNIQUE NOT NULL,
                      period TEXT NOT NULL,
                      mode INTEGER NOT NULL,
                      platform INTEGER NOT NULL,
                      director_activity_hash INTEGER NOT NULL,
                      reference_id INTEGER NOT NULL)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS character
                     (character_id INTEGER PRIMARY KEY UNIQUE NOT NULL,
                      member INTEGER NOT NULL,
                      class INTEGER NOT NULL,
                      FOREIGN KEY (member) REFERENCES member (member_id) ON DELETE CASCADE)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS character_activity_stats
                     (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                      character INTEGER,
                      activity INTEGER,
                      score INTEGER,
                      kills INTEGER,
                      deaths INTEGER,
                      completed INTEGER,
                      opponents_defeated INTEGER,
                      standing INTEGER,
                      team INTEGER,
                      time_played_seconds INTEGER,
                      team_score INTEGER,
                      precision_kills INTEGER,
                      weapon_kills_super INTEGER,
                      FOREIGN KEY (activity) REFERENCES activity (activity_id) ON DELETE CASCADE)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS member
                     (member_id INTEGER PRIMARY KEY UNIQUE NOT NULL,
                      platform_id INTEGER NOT NULL,
                      display_name TEXT,
                      bungie_display_name TEXT,
                      bungie_display_name_code TEXT)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS modes
                     (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                      mode INTEGER NOT NULL,
                      activity INTEGER NOT NULL,
                      UNIQUE(mode, activity),
                      FOREIGN KEY (activity) REFERENCES activity (activity_id) ON DELETE CASCADE)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS team_result
                     (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                      team_id INTEGER NOT NULL,
                      activity INTEGER NOT NULL,
                      score INTEGER NOT NULL,
                      standing INTEGER NOT NULL,
                      UNIQUE(team_id, activity),
                      FOREIGN KEY (activity) REFERENCES activity (activity_id) ON DELETE CASCADE)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS weapon_result
                     (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                      reference_id INTEGER NOT NULL,
                      kills INTEGER NOT NULL,
                      precision_kills INTEGER NOT NULL,
                      kills_precision_kills_ratio REAL NOT NULL,
                      character_activity_stats INTEGER NOT NULL,
                      UNIQUE(character_activity_stats, reference_id),
                      FOREIGN KEY (character_activity_stats) REFERENCES character_activity_stats (id) ON DELETE CASCADE)''')

def insert_data(conn, activity_id, json_data):
    activity_details = json_data['Response']['activityDetails']
    period = json_data['Response']['period']
    mode = activity_details.get('mode')
    director_activity_hash = activity_details.get('directorActivityHash')
    reference_id = activity_details.get('referenceId')
   
    conn.execute("INSERT OR IGNORE INTO activity (activity_id, period, mode, director_activity_hash, reference_id) VALUES (?, ?, ?, ?, ?)", (activity_id, period, mode, director_activity_hash, reference_id))

    for entry in json_data['Response']['entries']:
        player = entry['player']
        member_id = player['destinyUserInfo']['membershipId']
        display_name = player['destinyUserInfo']['displayName']
        
        # Insert character activity stats into the table
        character = entry['characterId']
        precision_kills = entry['extended']['values']['precisionKills']['basic']['value']
        weapon_kills_super = entry['extended']['values']['weaponKillsSuper']['basic']['value']
        kills = entry['values']['kills']['basic']['value']
        deaths = entry['values']['deaths']['basic']['value']
        opponents_defeated = entry['values']['opponentsDefeated']['basic']['value']
        time_played_seconds = entry['values']['timePlayedSeconds']['basic']['value']
        
        conn.execute("INSERT INTO character_activity_stats (activity, character, score, kills, deaths, completed, opponents_defeated, standing, team, time_played_seconds, team_score, precision_kills, weapon_kills_super) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (activity_id, character, 0, kills, deaths, 0, opponents_defeated, 0, 0, time_played_seconds, 0, precision_kills, weapon_kills_super))
        
        
    conn.commit()   
    

async def retry_activities(api_key, retry_pool, semaphore, conn):
    while True:
        await asyncio.sleep(60)  # Retry every 1 minute
        print("Retrying activities...")
        tasks = []
        for activity_id in retry_pool:
            tasks.append(process_activity(api_key, activity_id, semaphore, conn, retry_pool))
        retry_pool.clear()
        await asyncio.gather(*tasks)

async def main():
    conn = sqlite3.connect(DATABASE_PATH)
    create_schema(conn)

    activity_id = int(input("Enter the first activity ID: "))  # Move the variable assignment here

    asyncio.create_task(retry_activities(api_key, retry_pool, semaphore, conn))

    while True:
        tasks = []
        for _ in range(concurrent_calls):
            tasks.append(process_activity(api_key, activity_id, semaphore, conn, retry_pool))
            activity_id += 1
        await asyncio.gather(*tasks)

    conn.close()

if __name__ == '__main__':
    asyncio.run(main())
