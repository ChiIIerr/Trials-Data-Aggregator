import sqlite3
import aiohttp
import asyncio


async def call_api(api_key, weapon_reference_id, semaphore):
    headers = {
        'X-API-Key': api_key,
    }
    url = f'https://www.bungie.net/Platform/Destiny2/Manifest/DestinyInventoryItemDefinition/{weapon_reference_id}/'
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            return await response.json()
                        else:
                            print(f"API call failed for weapon_data: {weapon_reference_id}. Retrying in 10 seconds...")
                            await asyncio.sleep(10)
                            continue
                            # Check if the response header has a cache hit
                            cache_status = response.headers.get('cf-cache-status')
                            if cache_status and 'public' in cache_status:
                                print(f"Cache hit for activity: {activity_id}")
                except aiohttp.ClientConnectionError:
                    print(f"Server disconnected. Retrying in 5 seconds...")
                    time.sleep(5)
                except asyncio.TimeoutError:
                    print(f"Timeout error occurred for weapon_data: {weapon_reference_id}")
                    return None

def insert_weapon_data(conn, weapon_reference_id, weapon_data):
    print(f"Inserting weapon ID {weapon_reference_id} into the manifest.")
    weapon_json = weapon_data.get('Response', {})
    ammo_type = weapon_json['equippingBlock']['ammoType']
    cursor = conn.cursor()
    try:
        cursor.execute('INSERT INTO weapons_manifest (weapon_reference_id, ammo_type) VALUES (?, ?)', (weapon_reference_id, ammo_type))
    except sqlite3.IntegrityError:
        print(f"weapon_reference_id {weapon_reference_id} already exists in the database.")
        return None
    conn.commit()
                 

def is_weapon_reference_id_exists(conn, weapon_reference_id):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM weapons_manifest WHERE weapon_reference_id = ?", (weapon_reference_id,))
    count = cursor.fetchone()[0]
    return count > 0
