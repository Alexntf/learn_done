import requests
import os
import duckdb
import json

def get_api_data(season_id):
    api_token = os.getenv('API_TOKEN')
    url = f"https://tennis.sportdevs.com/seasons-rounds?season_id=eq.{season_id}"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error during the request for season {season_id}: {response.status_code} - {response.text}")
        return None

def process_data(data):
    rounds = []
    for season in data:
        season_info = {
            "season_id": season["season_id"],
            "season_name": season["season_name"],
        }
        for round in season["rounds"]:
            round_info = season_info.copy()
            round_info.update({
                "id": round["id"],
                "name": round["name"],
                "round": round["round"],
                "end_time": round["end_time"],
                "start_time": round["start_time"],
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

    # Create seasons table if it doesn't exist
    conn.execute('''
        CREATE TABLE IF NOT EXISTS rounds (
            id STRING PRIMARY KEY,
            season_id STRING,
            season_name STRING,
            name STRING,
            round STRING,
            end_time DATE,
            start_time DATE,
        )
    ''')

    # Get all league_ids from the leagues table
    season_ids = conn.execute('SELECT DISTINCT id FROM seasons').fetchall()

    for (season_id,) in season_ids:
        print(f"Processing season_id: {season_id}")
        data = get_api_data(season_id)
        if data:
            rounds = process_data(data)
            insert_data(conn, rounds)

    # Verify data inserted into the table
    result = conn.execute("SELECT * FROM rounds LIMIT 5").fetchall()
    print("Sample data in seasons table:")
    for row in result:
        print(row)

    conn.close()

if __name__ == "__main__":
    main()