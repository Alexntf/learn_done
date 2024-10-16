import requests
import os
import duckdb
import json
import sys
from requests.exceptions import RequestException

def get_api_data(max_retries=3, delay=5):
    api_token = os.getenv('API_TOKEN')
    url = f"https://tennis.sportdevs.com/matches?round_id=eq.217407"
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
        sys.exit(1)  # Exit the script with an error code

def process_data(data):
    matchs = []
    for match in data:
        match_info = {
            "id": match["id"],
            "tournament_id": match["tournament_id"],
            "season_id": match["season_id"],
            "round_id": match["round_id"],
            "arena_id": match["arena_id"],
            "home_team_id": match["home_team_id"],
            "away_team_id": match["away_team_id"],
            "class_id": match["class_id"],
            "league_id": match["league_id"],
            "name": match["name"],
            "first_to_serve": match["first_to_serve"],
            "ground_type": match["ground_type"],
            "tournament_name": match["tournament_name"],
            "tournament_importance": match["tournament_importance"],
            "season_name": match["season_name"],
            "status_type": match["status_type"],
            "arena_name": match["arena_name"],
            "arena_hash_image": match["arena_hash_image"],
            "home_team_name": match["home_team_name"],
            "home_team_hash_image": match["home_team_hash_image"],
            "away_team_name": match["away_team_name"],
            "away_team_hash_image": match["away_team_hash_image"],
            "specific_start_time": match["specific_start_time"],
            "start_time": match["start_time"],
            "duration": match["duration"],
            "class_name": match["class_name"],
            "class_hash_image": match["class_hash_image"],
            "league_name": match["league_name"],
            "league_hash_image": match["league_hash_image"]
        }
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
        print(match_info)
        matchs.append(match_info)
    return matchs
            
            
def main():
    data = get_api_data()
    processed_data = process_data(data)

if __name__ == "__main__":
    main()









            


