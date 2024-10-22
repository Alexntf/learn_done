import requests
import os
import duckdb
import json
import sys
from requests.exceptions import RequestException

def get_api_data(round_id, max_retries=3, delay=5):
    api_token = os.getenv('API_TOKEN')
    url = f"https://tennis.sportdevs.com/matches?round_id=eq.{round_id}"
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
    if data is not None :
        matches = []
        for match in data:
            match_info = {
                "id": match.get("id"),
                "tournament_id": match.get("tournament_id", None),
                "season_id": match.get("season_id", None),
                "round_id": match.get("round_id", None),
                "arena_id": match.get("arena_id", None),
                "home_team_id": match.get("home_team_id", None),
                "away_team_id": match.get("away_team_id", None),
                "class_id": match.get("class_id", None),
                "league_id": match.get("league_id", None),
                "name": match.get("name", None),
                "first_to_serve": match.get("first_to_serve", None),
                "ground_type": match.get("ground_type", None),
                "tournament_name": match.get("tournament_name", None),
                "tournament_importance": match.get("tournament_importance", None),
                "season_name": match.get("season_name", None),
                "status_type": match.get("status_type", None),
                "arena_name": match.get("arena_name", None),
                "arena_hash_image": match.get("arena_hash_image", None),
                "home_team_name": match.get("home_team_name", None),
                "home_team_hash_image": match.get("home_team_hash_image", None),
                "away_team_name": match.get("away_team_name", None),
                "away_team_hash_image": match.get("away_team_hash_image", None),
                "specific_start_time": match.get("specific_start_time", None),
                "start_time": match.get("start_time", None),
                "duration": match.get("duration", None),
                "class_name": match.get("class_name", None),
                "class_hash_image": match.get("class_hash_image", None),
                "league_name": match.get("league_name", None),
                "league_hash_image": match.get("league_hash_image", None)
            }
            # Rounds
            round_info = match.get("round", {})
            match_info.update({
                "round_name": round_info.get("name", {}),
            })
            # Status 
            status_info = match.get("status", {})
            match_info.update({
                "status": status_info.get("type", None),
                "status_reason": status_info.get("reason", None)
            })
            # Home team dict
            home_team_score_info = match.get("home_team_score", {})
            match_info.update({
                "home_team_score_current": home_team_score_info.get("current", None),
                "home_team_score_display": home_team_score_info.get("display", None),
                "home_team_score_set_1": home_team_score_info.get("period_1", None),
                "home_team_score_set_2": home_team_score_info.get("period_2", None),
                "home_team_score_set_3": home_team_score_info.get("period_3", None),
                "home_team_score_set_4": home_team_score_info.get("period_4", None),
                "home_team_score_set_5": home_team_score_info.get("period_5", None)
            })
            # Away team dict
            away_team_score_info = match.get("away_team_score", {})
            match_info.update({
                "away_team_score_current": away_team_score_info.get("current", None),
                "away_team_score_display": away_team_score_info.get("display", None),
                "away_team_score_set_1": away_team_score_info.get("period_1", None),
                "away_team_score_set_2": away_team_score_info.get("period_2", None),
                "away_team_score_set_3": away_team_score_info.get("period_3", None),
                "away_team_score_set_4": away_team_score_info.get("period_4", None),
                "away_team_score_set_5": away_team_score_info.get("period_5", None),
            })
            # Times 
            time_info = match.get("times", {})
            match_info.update({
                "time_set_1": time_info.get("period_1", None),
                "time_set_2": time_info.get("period_2", None),
                "time_set_3": time_info.get("period_3", None),
                "time_set_4": time_info.get("period_4", None),
                "time_set_5": time_info.get("period_5", None)
            })
            print(match_info)
            matches.append(match_info)
        return matches
    else: 
        return

def insert_data(conn, matches):
    if matches is not None :
        for match in matches:
            conn.execute('''
                INSERT OR IGNORE INTO matches (
                    id, tournament_id, season_id, round_id, arena_id, home_team_id, away_team_id,
                    class_id, league_id, name, first_to_serve, ground_type, tournament_name,
                    tournament_importance, season_name, status_type, arena_name, arena_hash_image,
                    home_team_name, home_team_hash_image, away_team_name, away_team_hash_image,
                    specific_start_time, start_time, duration, class_name, class_hash_image,
                    league_name, league_hash_image, round_name, status, status_reason,
                    home_team_score_current, home_team_score_display, home_team_score_set_1,
                    home_team_score_set_2, home_team_score_set_3, home_team_score_set_4,
                    home_team_score_set_5, away_team_score_current, away_team_score_display,
                    away_team_score_set_1, away_team_score_set_2, away_team_score_set_3,
                    away_team_score_set_4, away_team_score_set_5, time_set_1, time_set_2,
                    time_set_3, time_set_4, time_set_5
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                        ?, ?, ?)
            ''', (
                match['id'], match['tournament_id'], match['season_id'],
                match['round_id'], match['arena_id'], match['home_team_id'],
                match['away_team_id'], match['class_id'], match['league_id'],
                match['name'], match['first_to_serve'], match['ground_type'],
                match['tournament_name'], match['tournament_importance'],
                match['season_name'], match['status_type'], match['arena_name'],
                match['arena_hash_image'], match['home_team_name'],
                match['home_team_hash_image'], match['away_team_name'],
                match['away_team_hash_image'], match['specific_start_time'],
                match['start_time'], match['duration'], match['class_name'],
                match['class_hash_image'], match['league_name'],
                match['league_hash_image'], match['round_name'], match['status'],
                match['status_reason'], match['home_team_score_current'],
                match['home_team_score_display'], match['home_team_score_set_1'],
                match['home_team_score_set_2'], match['home_team_score_set_3'],
                match['home_team_score_set_4'], match['home_team_score_set_5'],
                match['away_team_score_current'], match['away_team_score_display'],
                match['away_team_score_set_1'], match['away_team_score_set_2'],
                match['away_team_score_set_3'], match['away_team_score_set_4'],
                match['away_team_score_set_5'], match['time_set_1'],
                match['time_set_2'], match['time_set_3'], match['time_set_4'],
                match['time_set_5']
            ))
    else:
        print("No Data to insert")
            
def main():
    conn = duckdb.connect(database='/Users/alexnataf/learn_done/tennis.db')

    # Create matches table 
    conn.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            id STRING PRIMARY KEY,
            tournament_id STRING,
            season_id STRING,
            round_id STRING,
            arena_id STRING,
            home_team_id STRING,
            away_team_id STRING,
            class_id STRING,
            league_id STRING,
            name STRING,
            first_to_serve INTEGER,
            ground_type STRING,
            tournament_name STRING,
            tournament_importance INTEGER,
            season_name STRING,
            status_type STRING,
            arena_name STRING,
            arena_hash_image STRING,
            home_team_name STRING,
            home_team_hash_image STRING,
            away_team_name STRING,
            away_team_hash_image STRING,
            specific_start_time TIMESTAMP,
            start_time TIMESTAMP,
            duration INTEGER,
            class_name STRING,
            class_hash_image STRING,
            league_name STRING,
            league_hash_image STRING,
            round_name STRING,
            status STRING,
            status_reason STRING,
            home_team_score_current INTEGER,
            home_team_score_display INTEGER,
            home_team_score_set_1 INTEGER,
            home_team_score_set_2 INTEGER,
            home_team_score_set_3 INTEGER,
            home_team_score_set_4 INTEGER,
            home_team_score_set_5 INTEGER,
            away_team_score_current INTEGER,
            away_team_score_display INTEGER,
            away_team_score_set_1 INTEGER,
            away_team_score_set_2 INTEGER,
            away_team_score_set_3 INTEGER,
            away_team_score_set_4 INTEGER,
            away_team_score_set_5 INTEGER,
            time_set_1 INTEGER,
            time_set_2 INTEGER,
            time_set_3 INTEGER,
            time_set_4 INTEGER,
            time_set_5 INTEGER
        )
    ''')

    # Get all round_ids from the rounds table only for RG 2023 for now
    all_round_ids = set(id for (id,) in conn.execute("SELECT DISTINCT id FROM rounds").fetchall())

    # Get round_ids that already have matches in the matches table
    existing_round_ids = set(id for (id,) in conn.execute('SELECT DISTINCT round_id FROM matches').fetchall())

    # Find round_ids that don't have matches yet
    new_round_ids = all_round_ids - existing_round_ids

    print(f"Total rounds: {len(all_round_ids)}")
    print(f"Rounds with existing matches: {len(existing_round_ids)}")
    print(f"New rounds to process: {len(new_round_ids)}")

    for round_id in new_round_ids:
        print(f"Processing round_id: {round_id}")
        data = get_api_data(round_id)
        matches = process_data(data)
        insert_data(conn, matches)

    # Commit the changes
    conn.commit()

    # Verify data inserted into the table
    result = conn.execute("SELECT * FROM matches LIMIT 5").fetchall()
    print("Sample data in matches table:")
    for row in result:
        print(row)


if __name__ == "__main__":
    main()









            


