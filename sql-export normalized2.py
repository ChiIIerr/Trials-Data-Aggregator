import asyncio
import aiohttp
import time
import sqlite3
import json
from collections import deque

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

def create_tables(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS activities
                     (id INTEGER PRIMARY KEY, activity_id INTEGER, data TEXT)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS players
                     (id INTEGER PRIMARY KEY, activity_id INTEGER, player_id INTEGER, player_name TEXT)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS teams
                     (id INTEGER PRIMARY KEY, activity_id INTEGER, team_id INTEGER, team_name TEXT)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS team_players
                     (id INTEGER PRIMARY KEY, team_id INTEGER, player_id INTEGER)''')

def insert_data(conn, activity_id, json_data):
    activity_data = {
        'activity_id': activity_id,
        'data': json.dumps(json_data)
    }
    activity_row_id = conn.execute("INSERT INTO activities (activity_id, data) VALUES (:activity_id, :data)", activity_data).lastrowid
    conn.commit()

    for entry in json_data['Response']['entries']:
        player = entry['player']
        player_data = {
            'activity_id': activity_id,
            'player_id': player['destinyUserInfo']['membershipId'],
            'player_name': player['destinyUserInfo']['displayName']
        }
        player_row_id = conn.execute("INSERT INTO players (activity_id, player_id, player_name) VALUES (:activity_id, :player_id, :player_name)", player_data).lastrowid
        conn.commit()

        if isinstance(entry['values']['team'], dict):  # Check if 'team' is a dictionary
            teams = [entry['values']['team']]  # Wrap the dictionary in a list
        else:
            teams = entry['values']['team']

        for team in teams:
            team_id = team.get('teamId', None) if isinstance(team, dict) else None  # Get the value of 'teamId' or set it to None if the key doesn't exist
            team_name = team.get('teamName', None) if isinstance(team, dict) else None  # Get the value of 'teamName' or set it to None if the key doesn't exist
            team_data = {
                'activity_id': activity_id,
                'team_id': team_id,
                'team_name': team_name
            }
            team_row_id = conn.execute("INSERT INTO teams (activity_id, team_id, team_name) VALUES (:activity_id, :team_id, :team_name)", team_data).lastrowid
            conn.commit()

            team_player_data = {
                'team_id': team_row_id,
                'player_id': player_row_id
            }
            conn.execute("INSERT INTO team_players (team_id, player_id) VALUES (:team_id, :player_id)", team_player_data)
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
    api_key = input("Enter your API key: ")
    activity_id = int(input("Enter the first activity ID: "))
    concurrent_calls = 40  # Adjust the number of concurrent API calls
    semaphore = asyncio.Semaphore(concurrent_calls)

    conn = sqlite3.connect('F:/New folder/New folder/db.sqlite')
    create_tables(conn)

    retry_pool = []

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
