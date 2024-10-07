import requests  # Import the requests library for making HTTP requests
import os  # Import os for accessing environment variables
import duckdb  # Import duckdb for database interactions
import json  # Import json for handling JSON data

#### REQUEST DATA #####

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

# CHECK RESPONSE
if response.status_code == 200:
    try:
        data = response.json()  # Convert the response to a dictionary
        
        # Add class_id & class_name
        tournaments_with_class_info = []
        
        # Iterate over each tournament data in the response
        for tournament in data:
            # Extract the class_id and class_name
            class_id = tournament["class_id"]
            class_name = tournament["class_name"]
            
            # Extract the list of teams with the most titles
            teams_most_titles = []
            if "teams_most_titles" in tournament:
                for team in tournament["teams_most_titles"]:
                    teams_most_titles.append({
                        "team_id": team["team_id"],
                        "team_name": team["team_name"],
                        "team_hash_image": team["team_hash_image"]
                    })

            # Extract tournament information and organize it in a dictionary
            tournament_info = {
                "id": tournament["id"],
                "name": tournament["name"],
                "importance": tournament["importance"],
                "current_champion_team_id": tournament.get("current_champion_team_id"),
                "current_champion_team_name": tournament.get("current_champion_team_name"),
                "current_champion_team_hash_image": tournament.get("current_champion_team_hash_image"),
                "most_titles": tournament.get("most_titles"),
                "ground": tournament.get("ground"),
                "number_of_sets": tournament.get("number_of_sets"),
                "max_points": tournament.get("max_points"),
                "primary_color": tournament.get("primary_color"),
                "secondary_color": tournament.get("secondary_color"),
                "start_league": tournament.get("start_league"),
                "end_league": tournament.get("end_league"),
                "hash_image": tournament.get("hash_image"),
                "class_id": class_id,  # Add class_id
                "class_name": class_name,  # Add class_name
                "teams_most_titles": teams_most_titles  # Add teams_most_titles
            }

            # Add tournament information to the list
            tournaments_with_class_info.append(tournament_info)

        # Print the result
        print("Tournaments with class information:")
        for t in tournaments_with_class_info:
            print(t)

    except json.JSONDecodeError:
        print("Error while converting the response to JSON.")
else:
    print(f"Error during the request: {response.status_code} - {response.text}")


#### INGEST DATA TO DUCKDB #####


