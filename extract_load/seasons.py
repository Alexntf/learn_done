import requests
import os
import duckdb
import json

def get_api_data(league_id):
    api_token = os.getenv('API_TOKEN')
    url = f"https://tennis.sportdevs.com/seasons-by-league?league_id=eq.{league_id}"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error during the request for league {league_id}: {response.status_code} - {response.text}")
        return None

def process_data(data):
    seasons = []
    for league in data:
        league_info = {
            "league_id": league.get("league_id"),
            "league_name": league["league_name"],
            "league_hash_image": league["league_hash_image"],
        }
        for season in league["seasons"]:
            season_info = league_info.copy()
            season_info.update({
                "id": season["id"],
                "name": season.get("name", None),
                "year": season.get("year", None),
                "start_time": season.get("start_time", None)
            })
            seasons.append(season_info)
    return seasons

def insert_data(conn, seasons):
    for season in seasons:
        conn.execute('''
            INSERT OR IGNORE INTO seasons (
                id, league_id, league_name, league_hash_image, name, year, start_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            season['id'], season['league_id'], season['league_name'], season['league_hash_image'],
            season['name'], season['year'], season['start_time']
        ))

def main():
    conn = duckdb.connect(database='/Users/alexnataf/learn_done/tennis.db')

    # Create seasons table if it doesn't exist
    conn.execute('''
        CREATE TABLE IF NOT EXISTS seasons (
            id STRING PRIMARY KEY,
            league_id STRING,
            league_name STRING,
            league_hash_image STRING,
            name STRING,
            year STRING,
            start_time DATE
        )
    ''')

    # Get all league_ids from the leagues table
    all_league_ids = set(id for (id,) in conn.execute('SELECT DISTINCT id FROM leagues').fetchall())

    # Get league_ids that already have seasons in the seasons table
    existing_league_ids = set(league_id for (league_id,) in conn.execute('SELECT DISTINCT league_id FROM seasons').fetchall())

    # Find league_ids that don't have seasons yet
    new_league_ids = all_league_ids - existing_league_ids

    print(f"Total leagues: {len(all_league_ids)}")
    print(f"Leagues with existing seasons: {len(existing_league_ids)}")
    print(f"New leagues to process: {len(new_league_ids)}")

    for league_id in new_league_ids:
        print(f"Processing league_id: {league_id}")
        data = get_api_data(league_id)
        if data:
            seasons = process_data(data)
            insert_data(conn, seasons)

    # Commit the changes
    conn.commit()

    # Verify data inserted into the table
    result = conn.execute("SELECT * FROM seasons LIMIT 5").fetchall()
    print("Sample data in seasons table:")
    for row in result:
        print(row)

    conn.close()

if __name__ == "__main__":
    main()