import requests
import os
import duckdb
import json
import sys
from requests.exceptions import RequestException

def get_api_data(season_id, max_retries=3, delay=5):
    api_token = os.getenv('API_TOKEN')
    url = f"https://tennis.sportdevs.com/seasons-rounds?season_id=eq.{season_id}"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()
    except RequestException as e:
        print(f"Error occurred while fetching data for season {season_id}: {str(e)}")
        print("Stopping the entire process due to error.")
        sys.exit(1)  # Exit the script with an error code

def process_data(data):
    rounds = []
    for season in data:
        season_info = {
            "season_id": season.get("season_id"),
            "season_name": season.get("season_name", None),
        }
        for round in season.get("rounds", []):
            round_info = season_info.copy()
            round_info.update({
                "id": round.get("id"),
                "name": round.get("name", None),
                "round": round.get("round", None),
                "end_time": round.get("end_time", None),
                "start_time": round.get("start_time", None),
            })
            rounds.append(round_info)
    return rounds

def insert_data(conn, rounds):
    for round in rounds:
        conn.execute('''
            INSERT OR IGNORE INTO rounds (
                id, season_id, season_name, name, round, end_time, start_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            round['id'], round['season_id'], round["season_name"], 
            round["name"], round["round"], round["end_time"], round["start_time"]
        ))

def main():
    conn = duckdb.connect(database='/Users/alexnataf/learn_done/tennis.db')

    # Create rounds table if it doesn't exist
    conn.execute('''
        CREATE TABLE IF NOT EXISTS rounds (
            id STRING PRIMARY KEY,
            season_id STRING,
            season_name STRING,
            name STRING,
            round STRING,
            end_time DATE,
            start_time DATE
        )
    ''')

    # Get all season_ids from the seasons table
    all_season_ids = set(id for (id,) in conn.execute("SELECT DISTINCT id FROM seasons").fetchall())

    # Get season_ids that already have rounds
    existing_season_ids = set(id for (id,) in conn.execute('SELECT DISTINCT season_id FROM rounds').fetchall())

    # Find season_ids that don't have rounds yet
    new_season_ids = all_season_ids - existing_season_ids

    print(f"Total seasons: {len(all_season_ids)}")
    print(f"Seasons with existing rounds: {len(existing_season_ids)}")
    print(f"New seasons to process: {len(new_season_ids)}")

    for season_id in new_season_ids:
        print(f"Processing season_id: {season_id}")
        data = get_api_data(season_id)
        rounds = process_data(data)
        insert_data(conn, rounds)

    # Verify data inserted into the table
    result = conn.execute("SELECT * FROM rounds LIMIT 5").fetchall()
    print("Sample data in rounds table:")
    for row in result:
        print(row)

    conn.close()

if __name__ == "__main__":
    main()