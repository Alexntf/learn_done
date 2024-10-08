import requests  # Import the requests library for making HTTP requests
import os  # Import os for accessing environment variables
import duckdb  # Import duckdb for database interactions
import json  # Import json for handling JSON data

#### EXTRACT #####

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


#### LOAD #####

# TEST DB
try:
    test_conn = duckdb.connect(database='/Users/alexnataf/learn_done/tennis.db')
    print("Database connection successful")
    test_conn.close()
except Exception as e:
    print(f"Database connection failed: {str(e)}")

# CONNECT
conn = duckdb.connect(database='/Users/alexnataf/learn_done/tennis.db')

# Create a table for the tournaments
conn.execute('''
    CREATE TABLE IF NOT EXISTS 
        tournaments (
            id STRING,
            name TEXT,
            importance INTEGER,
            class_id STRING,
            class_name TEXT
        )
''')

# Insert or update the data in the table
for tournament in tournaments_with_class_info:
    conn.execute('''
        INSERT INTO tournaments (id, name, importance, class_id, class_name) 
        VALUES (?, ?, ?, ?, ?)
    ''', (tournament['id'], tournament['name'], tournament['importance'], 
          tournament['class_id'], tournament['class_name']))

# Verify the inserted data
result = conn.execute('SELECT * FROM tournaments LIMIT 5').fetchall()
print("Data in the tournaments table:")
for row in result:
    print(row)  # Print each row of data

# Close the connection to the database
conn.close()


