import asyncio
import aiohttp
import sqlite3
import time

# Database path
DATABASE_PATH = 'F:/New folder/New folder/db.sqlite'

# API key
api_key = input("Enter your API key: ")

# Number of concurrent API calls
concurrent_calls = 50

# Semaphore for controlling concurrent API calls
semaphore = asyncio.Semaphore(concurrent_calls)

async def call_api(api_key, activity_id, semaphore):
    headers = {
        'X-API-Key': api_key,
        'Cache-Control': 'no-cache'  # Add cache control header
    }
    url = f'https://www.bungie.net/Platform/Destiny2/Stats/PostGameCarnageReport/{activity_id}/'
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            if 'ThrottleSeconds' in response.headers and int(response.headers['ThrottleSeconds']) > 0:
                                print(f"ThrottleSeconds is greater than 0 for activity: {activity_id}")
                            else:
                                continue
                            return await response.json()
                        else:
                            print(f"API call failed for activity: {activity_id}. Retrying in 10 seconds...")
                            await asyncio.sleep(10)
                            continue
                            # Check if the response header has a cache hit
                            if cache_status and 'public' in cache_status:
                                print(f"Cache hit for activity: {activity_id}")
                except aiohttp.ClientConnectionError:
                    print(f"Server disconnected. Retrying in 5 seconds...")
                    time.sleep(5)
                except asyncio.TimeoutError:
                    print(f"Timeout error occurred for activity: {activity_id}")
                    return None

async def process_activity(api_key, activity_id, semaphore, conn):
    json_data = await call_api(api_key, activity_id, semaphore)
    if json_data is not None:
        activity_details = json_data['Response']['activityDetails']
        if activity_details['mode'] == 84:
            print(f"Found activity with mode 84: {activity_id}")
            insert_data(conn, activity_id, json_data)
        # else:
        # print(f"Not trials, but ID is {activity_id}")

def create_schema(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS activity
                     (activity_id INTEGER PRIMARY KEY UNIQUE NOT NULL,
                      period TEXT NOT NULL,
                      mode INTEGER NOT NULL,
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
    conn.execute('''CREATE TABLE IF NOT EXISTS weapons
                     (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                      activity INTEGER NOT NULL,
                      FOREIGN KEY (activity) REFERENCES activity (activity_id) ON DELETE CASCADE),
                      weapon_reference_id INTEGER NOT NULL,
                      kills INTEGER NOT NULL,
                      precision_kills INTEGER NOT NULL,
                      kills_precision_kills_ratio REAL NOT NULL,
                      character INTEGER NOT NULL,
                      FOREIGN KEY (character) REFERENCES character_activity_stats (character) ON DELETE CASCADE)''')

def insert_data(conn, activity_id, json_data):
    activity_details = json_data['Response']['activityDetails']
    period = json_data['Response']['period']
    mode = activity_details['mode']
    director_activity_hash = activity_details['directorActivityHash']
    reference_id = activity_details['referenceId']
   
    # Check if activity_id already exists in activity table
    cursor = conn.execute("SELECT activity_id FROM activity WHERE activity_id = ?", (activity_id,))
    if cursor.fetchone() is not None:
        print(f"Activity with ID {activity_id} already exists in the activity table.")
        return

    conn.execute("INSERT INTO activity (activity_id, period, mode, director_activity_hash, reference_id) VALUES (?, ?, ?, ?, ?)", (activity_id, period, mode, director_activity_hash, reference_id))
    conn.commit()

    for entry in json_data['Response']['entries']:
        player = entry['player']

        # Insert character activity stats into the table
        character = entry['characterId']
        weapon_kills_super = entry['extended']['values']['weaponKillsSuper']['basic']['value'] if 'extended' in entry else None
        kills = entry['values']['kills']['basic']['value']
        deaths = entry['values']['deaths']['basic']['value']
        opponents_defeated = entry['values']['opponentsDefeated']['basic']['value']
        time_played_seconds = entry['values']['timePlayedSeconds']['basic']['value']

        conn.execute("INSERT INTO character_activity_stats (activity, character, score, kills, deaths, completed, opponents_defeated, standing, team, time_played_seconds, team_score, weapon_kills_super) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (activity_id, character, 0, kills, deaths, 0, opponents_defeated, 0, 0, time_played_seconds, 0, weapon_kills_super))
        
    conn.commit()  

    for entry in json_data['Response']['entries']:
        player = entry['player']

        # Insert character activity stats into the table
        character = entry['characterId']
        weapon_kills_super = entry['extended']['values']['weaponKillsSuper']['basic']['value'] if 'extended' in entry else None
        kills = entry['values']['kills']['basic']['value']
        deaths = entry['values']['deaths']['basic']['value']
        opponents_defeated = entry['values']['opponentsDefeated']['basic']['value']
        time_played_seconds = entry['values']['timePlayedSeconds']['basic']['value']

        conn.execute("INSERT INTO character_activity_stats (activity, character, score, kills, deaths, completed, opponents_defeated, standing, team, time_played_seconds, team_score, weapon_kills_super) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (activity_id, character, 0, kills, deaths, 0, opponents_defeated, 0, 0, time_played_seconds, 0, weapon_kills_super))
        
    conn.commit()  
    
     

async def main():
    conn = sqlite3.connect(DATABASE_PATH)
    create_schema(conn)

    activity_id = int(input("Enter the first activity ID: "))  # Move the variable assignment here

    while True:
        tasks = []
        for _ in range(concurrent_calls):
            tasks.append(process_activity(api_key, activity_id, semaphore, conn))
            activity_id += 1
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
