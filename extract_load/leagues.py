import requests  # Import the requests library for making HTTP requests
import os  # Import os for accessing environment variables
import duckdb  # Import duckdb for database interactions
import json  # Import json for handling JSON data

#### EXTRACT #####

# CONFIG 
api_token = os.getenv('API_TOKEN')  # Get the API token from env var

class_id = "425" # ATP
start_league = "2024-01-01"



# URL 
url = f"https://tennis.sportdevs.com/leagues?class_id=eq.{415}&start_league=gte.{start_league}"
headers = {
    "Authorization": f"Bearer {api_token}",  
    "Content-Type": "application/json"  
}

# GET API
response = requests.get(url, headers=headers)


# CHECK RESPONSE
if response.status_code == 200:
    try:
        data = response.json()  # Convert the response to a dictionary
        
        # Add class_id & class_name
        leagues = []
        
        # Iterate over each tournament data in the response
        for league in data:
            
            # Extract the list of teams with the most titles
            teams_most_titles = []
            if "teams_most_titles" in league:
                for team in league["teams_most_titles"]:
                    teams_most_titles.append({
                        "team_id": team["team_id"],
                        "team_name": team["team_name"],
                        "team_hash_image": team["team_hash_image"]
                    })

            # Extract tournament information and organize it in a dictionary
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
                "class_id" : league.get("class_id"),
                "class_name": league.get("class_name"),
                "teams_most_titles": teams_most_titles  # Add teams_most_titles
            }

            # Add league information to the list
            leagues.append(league_info)

    except json.JSONDecodeError:
        print("Error while converting the response to JSON.")
else:
    print(f"Error during the request: {response.status_code} - {response.STRING}")


#### LOAD #####

conn = duckdb.connect(database='/Users/alexnataf/learn_done/tennis.db')

# create table
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
        teams_most_titles JSON  -- JSON column to store teams_most_titles
    )
''')

# Insert data 
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

# Verify data inserted into the table
result = conn.execute('SELECT * FROM leagues LIMIT 5').fetchall()
print("Data in leagues table:")
for row in result:
    print(row)

# Close the database connection
conn.close()