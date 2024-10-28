import requests
import os
import duckdb
import json

def get_api_data(class_id, start_league):
    api_token = os.getenv('API_TOKEN')
    url = f"https://tennis.sportdevs.com/leagues?class_id=eq.{class_id}&start_league=gte.{start_league}"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error occurred while fetching data: {str(e)}")
        return None

def process_data(data):
    leagues = []
    if data is not None:
        for league in data:
            # Extract teams_most_titles
            teams_most_titles = []
            if "teams_most_titles" in league:
                for team in league["teams_most_titles"]:
                    teams_most_titles.append({
                        "team_id": team["team_id"],
                        "team_name": team["team_name"],
                        "team_hash_image": team["team_hash_image"]
                    })

            # Extract league information
            league_info = {
                "id": league["id"],
                "name": league["name"],
                "importance": league["importance"],
                "current_champion_team_id": league.get("current_champion_team_id"),
                "current_champion_team_name": league.get("current_champion_team_name"),
                "current_champion_team_hash_image": league.get("current_champion_team_hash_image"),
                "most_titles": league.get("most_titles"),
                "ground": league.get("ground"),
                "number_of_sets": league.get("number_of_sets"),
                "max_points": league.get("max_points"),
                "primary_color": league.get("primary_color"),
                "secondary_color": league.get("secondary_color"),
                "start_league": league.get("start_league"),
                "end_league": league.get("end_league"),
                "hash_image": league.get("hash_image"),
                "class_id": league.get("class_id"),
                "class_name": league.get("class_name"),
                "teams_most_titles": teams_most_titles
            }
            leagues.append(league_info)
    return leagues

def insert_data(conn, leagues):
    if leagues:
        for league in leagues:
            conn.execute('''
                INSERT OR IGNORE INTO leagues (
                    id, name, importance, current_champion_team_id, current_champion_team_name, 
                    current_champion_team_hash_image, most_titles, ground, number_of_sets, max_points, 
                    primary_color, secondary_color, start_league, end_league, hash_image, 
                    class_id, class_name, teams_most_titles
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                league['id'], league['name'], league['importance'], league['current_champion_team_id'], 
                league['current_champion_team_name'], league['current_champion_team_hash_image'], 
                league['most_titles'], league['ground'], league['number_of_sets'], 
                league['max_points'], league['primary_color'], league['secondary_color'], 
                league['start_league'], league['end_league'], league['hash_image'], 
                league['class_id'], league['class_name'], league['teams_most_titles']
            ))
    else:
        print("No leagues to insert")

def main():
    class_id = "415"  # ATP
    start_league = "2023-01-01"
    
    conn = duckdb.connect(database='/Users/alexnataf/learn_done/tennis.db')

    # Create table if not exists
    conn.execute('''
        CREATE TABLE IF NOT EXISTS leagues (
            id INTEGER PRIMARY KEY,
            name STRING,
            importance INTEGER,
            current_champion_team_id INTEGER,
            current_champion_team_name STRING,
            current_champion_team_hash_image STRING,
            most_titles INTEGER,
            ground STRING,
            number_of_sets INTEGER,
            max_points INTEGER,
            primary_color STRING,
            secondary_color STRING,
            start_league TIMESTAMP,
            end_league TIMESTAMP,
            hash_image STRING,
            class_id INTEGER,
            class_name STRING,
            teams_most_titles JSON
        )
    ''')

    # Get API data
    data = get_api_data(class_id, start_league)
    
    # Process data
    leagues = process_data(data)
    
    # Insert data
    insert_data(conn, leagues)

    # Verify data inserted into the table
    result = conn.execute('SELECT * FROM leagues LIMIT 5').fetchall()
    print("Data in leagues table:")
    for row in result:
        print(row)

    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()