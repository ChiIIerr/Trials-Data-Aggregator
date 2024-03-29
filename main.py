import asyncio
import aiohttp
import sqlite3
import time
import weapon_api_call

# Database path
DATABASE_PATH = 'F:/New folder/New folder/db.sqlite'

# API key
api_key = input("Enter your API key: ")

# Number of concurrent API calls
concurrent_calls = 50

# Semaphore for controlling concurrent API calls
semaphore = asyncio.Semaphore(concurrent_calls)


# weapon_api_call.call_api(weapon_id, semaphore)
weapon_refrence_ids = []


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
                            json_data = await response.json()
                            if 'ThrottleSeconds' in json_data and json_data['ThrottleSeconds'] > 0:
                                print("Being throttled")
                            return json_data
                        else:
                            print(f"API call failed for activity: {activity_id}. Retrying in 10 seconds...")
                            await asyncio.sleep(10)
                            continue
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
        # Activity data
        if activity_details['mode'] == 84: # trials
            print(f"Found activity with mode 84: {activity_id}")
            if not is_activity_id_exists(conn, activity_id):
                weapon_refrence_ids.clear()
                insert_activity_data(conn, activity_id, json_data)
                # Weapon_manifest data
                for weapon_reference_id in weapon_refrence_ids:
                    if not weapon_api_call.is_weapon_reference_id_exists(conn, weapon_reference_id):
                        weapon_data = await weapon_api_call.call_api(api_key, weapon_reference_id, semaphore)
                        if weapon_data is not None:
                            weapon_api_call.insert_weapon_data(conn, weapon_reference_id, weapon_data)
                    else:
                        print(f"Weapon ID {weapon_reference_id} already exists in the manifest.")
            else:
                print(f"Activity ID {activity_id} already exists in the database.")
        else:
            print(f"Not trials, but ID is {activity_id}")

def is_activity_id_exists(conn, activity_id):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM activity WHERE activity_id = ?", (activity_id,))
    count = cursor.fetchone()[0]
    return count > 0

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
                      platform INTEGER,
                      light_level INTEGER,
                      membership_id INTEGER,
                      membership_type INTEGER,
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
                      weapon_reference_id INTEGER,
                      kills INTEGER,
                      precision_kills INTEGER NOT NULL,
                      kills_precision_kills_ratio REAL,
                      character INTEGER,
                      activity_id INTEGER NOT NULL,
                      FOREIGN KEY (weapon_reference_id) REFERENCES weapons_manifest (weapon_reference_id) ON DELETE CASCADE,
                      FOREIGN KEY (character) REFERENCES character_activity_stats (character) ON DELETE CASCADE,
                      FOREIGN KEY (activity_id) REFERENCES activity (activity_id) ON DELETE CASCADE)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS weapons_manifest
                      (weapon_reference_id INTEGER PRIMARY KEY UNIQUE NOT NULL,
                      weapon_type INTEGER,
                      ammo_type INTEGER)''')
    cursor = conn.execute("PRAGMA table_info(weapons_manifest)")
    columns = [column[1] for column in cursor.fetchall()]
    if 'weapon_type' not in columns:
        conn.execute('''ALTER TABLE weapons_manifest
                        ADD COLUMN weapon_type INTEGER''')

def insert_activity_data(conn, activity_id, json_data):
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
        # Insert character activity stats into the table
        character = entry['characterId']
        light_level = entry['player']['lightLevel'] 
        membership_id = entry['player']['destinyUserInfo']['membershipId']
        weapon_kills_super = entry['extended']['values']['weaponKillsSuper']['basic']['value']
        kills = entry['values']['kills']['basic']['value']
        deaths = entry['values']['deaths']['basic']['value']
        opponents_defeated = entry['values']['opponentsDefeated']['basic']['value']
        platform = entry['player'].get('membershipType', '')  # Use get() method with a default value
        time_played_seconds = entry['values']['timePlayedSeconds']['basic']['value']
        
        conn.execute("INSERT INTO character_activity_stats (activity, character, score, kills, deaths, completed, opponents_defeated, standing, team, time_played_seconds, team_score, weapon_kills_super, platform, light_level, membership_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (activity_id, character, 0, kills, deaths, 0, opponents_defeated, 0, 0, time_played_seconds, 0, weapon_kills_super, platform, light_level, membership_id))
        conn.commit() 

        # Check if 'weapons' field exists
        if 'weapons' in entry['extended']:
            for weapons in entry['extended']['weapons']:
                weapon_reference_id = weapons['referenceId']
                kills = weapons['values']['uniqueWeaponKills']['basic']['displayValue']
                precision_kills = weapons['values'].get('uniqueWeaponPrecisionKills', {}).get('basic', {}).get('value', 0)
                conn.execute("INSERT INTO weapons (weapon_reference_id, kills, precision_kills, activity_id, character) VALUES (?, ?, ?, ?, ?)", (weapon_reference_id, kills, precision_kills, activity_id, character)) 
                conn.commit()
                weapon_refrence_ids.append(weapon_reference_id)


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