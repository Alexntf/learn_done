import requests
import os
import duckdb
import json
import sys
from requests.exceptions import RequestException

def get_api_data(match_id, max_retries=3, delay=5):
    api_token = os.getenv('API_TOKEN')
    url = f"https://tennis.sportdevs.com/matches-statistics?match_id=eq.{match_id}"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()
    except RequestException as e:
        print(f"Error occurred while fetching data for season  {str(e)}")
        print("Stopping the entire process due to error.")
        
def process_data(data):
    if data is not None:
        statistics_data = []
        
        for match in data:
            match_id = match.get('match_id')
            statistics = match.get('statistics', [])
            
            # Pour chaque statistique dans le match, créer un événement
            for stat in statistics:
                event = {
                    'match_id': match_id,
                    'type': stat.get('type'),
                    'period': stat.get('period'),
                    'category': stat.get('category'),
                    'away_team': stat.get('away_team'),
                    'home_team': stat.get('home_team')
                }
                statistics_data.append(event)
                
        return statistics_data
    else:
        return

def insert_data(conn, statistics_data):
    if statistics_data is not None :
        for stat in statistics_data:
            conn.execute('''
                INSERT OR IGNORE INTO match_statistics (
                    match_id, type, period, category, away_team, home_team
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
             stat["match_id"], stat["type"], stat["period"], stat["category"], 
             stat["away_team"], stat["home_team"]
            ))
    else:
        print("No Data to insert")

def main():
    conn = duckdb.connect(database='/Users/alexnataf/learn_done/tennis.db')

    # Create matches table 
    conn.execute('''
        CREATE TABLE IF NOT EXISTS match_statistics (
            match_id STRING,
            type STRING,
            period STRING,
            category STRING,
            home_team STRING,
            away_team STRING,
            PRIMARY KEY (match_id, type, period)
        )
    ''')

    # Get all match_ids from the matches table, exemple on RG 2023
    all_match_ids = set(id for (id,) in conn.execute("SELECT DISTINCT id FROM matches WHERE season_id = '11893'").fetchall())

    # Get match_ids that already have matches stat in the statistics table
    existing_match_ids = set(id for (id,) in conn.execute('SELECT DISTINCT match_id FROM match_statistics').fetchall())

    # Find round_ids that don't have matches yet
    new_match_ids = all_match_ids - existing_match_ids

    print(f"Total match: {len(all_match_ids)}")
    print(f"Match with existing matches: {len(existing_match_ids)}")
    print(f"New matches to process: {len(new_match_ids)}")

    for match_id in new_match_ids:
        print(f"Processing match_id: {match_id}")
        data = get_api_data(match_id)
        matches = process_data(data)
        insert_data(conn, matches)

    # Commit the changes
    conn.commit()

    # Verify data inserted into the table
    result = conn.execute("SELECT * FROM match_statistics LIMIT 5").fetchall()
    print("Sample data in match statistics table:")
    for row in result:
        print(row)


if __name__ == "__main__":
    main()
