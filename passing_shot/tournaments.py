import requests  # Import the requests library for making HTTP requests
import os  # Import os for accessing environment variables
import duckdb  # Import duckdb for database interactions
import json  # Import json for handling JSON data

#### REQUEST DATA #####

# CONFIG 
api_token = os.getenv('API_TOKEN')  # Get the API token from env var

# URL 
url = "https://tennis.sportdevs.com/tournaments-by-class?class_id=eq.415"  # Replace with your URL
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
        tournaments_with_class_info = []
        for item in data:
            class_id = item["class_id"] 
            class_name = item["class_name"]  
            for tournament in item["tournaments"]:
                tournament_info = {
                    "id": tournament["id"],  
                    "name": tournament["name"],
                    "importance": tournament["importance"],
                    "class_id": class_id,
                    "class_name": class_name
                }
                tournaments_with_class_info.append(tournament_info)  # Tournament info to list

        print("Tournaments with class information:")


    except json.JSONDecodeError:
        print("Error while converting the response to JSON.")  
else:
    print(f"Error during the request: {response.status_code} - {response.text}")  


#### INGEST DATA TO DUCKDB #####

# CONNECT
conn = duckdb.connect(database='/Users/alexnataf/learn_done/tennis.db')

try:
    test_conn = duckdb.connect(database='/Users/alexnataf/learn_done/tennis.db')
    print("Database connection successful")
    test_conn.close()
except Exception as e:
    print(f"Database connection failed: {str(e)}")
    
conn.execute('''
    CREATE TABLE IF NOT EXISTS tournaments (
        id INTEGER PRIMARY KEY,
        name TEXT,
        importance INTEGER,
        current_champion_team_id INTEGER,
        current_champion_team_name TEXT,
        current_champion_team_hash_image TEXT,
        most_titles INTEGER,
        ground TEXT,
        number_of_sets INTEGER,
        max_points INTEGER,
        primary_color TEXT,
        secondary_color TEXT,
        start_league TIMESTAMP,
        end_league TIMESTAMP,
        hash_image TEXT,
        class_id INTEGER,
        class_name TEXT
    )