import streamlit as st
import pandas as pd
import duckdb
import os
from utils import convert_to_int, format_time

# DuckDB connection configuration
@st.cache_resource
def init_connection():
    return duckdb.connect('tennis.db')

# Load data from DuckDB
@st.cache_data
def load_data():
    conn = init_connection()
    matches_query = "SELECT * FROM fact_matches"
    matches_result = conn.execute(matches_query).fetchdf()
    
    return matches_result

matches_data = load_data()

# Create two columns with different widths
col1, col2 = st.columns([2, 1])  

with col1:
    selected_season = st.selectbox(
        "Choose a tournament",
        options=matches_data['season_name'].unique(),
        key="tournament_select"
    )
with col2:
    selected_round = st.selectbox(
        "Round",
        options=matches_data['round_name'].unique(),
        key="round_select"
    )

# Filter data based on selected season and round
season_data = matches_data[matches_data['season_name'] == selected_season]
round_data = season_data[season_data['round_name'] == selected_round]

selected_match = st.selectbox(
    "Choose a match",
    options= matches_data["match_name"]
)


# Get data for the selected match
selected_match = round_data[round_data['match_name'] == selected_match].iloc[0]

def simple_tennis_score_table(match_data):
    # Create a DataFrame with match scores
    df = pd.DataFrame({
        'Player': [match_data['home_team_name'], match_data['away_team_name'], ''],
        'Set 1': [
            convert_to_int(match_data['home_team_score_set_1']),
            convert_to_int(match_data['away_team_score_set_1']),
            format_time(match_data['time_set_1_min'])
        ],
        'Set 2': [
            convert_to_int(match_data['home_team_score_set_2']),
            convert_to_int(match_data['away_team_score_set_2']),
            format_time(match_data['time_set_2_min'])
        ],
        'Set 3': [
            convert_to_int(match_data['home_team_score_set_3']),
            convert_to_int(match_data['away_team_score_set_3']),
            format_time(match_data['time_set_3_min'])
        ],
        'Set 4': [
            convert_to_int(match_data['home_team_score_set_4']),
            convert_to_int(match_data['away_team_score_set_4']),
            format_time(match_data['time_set_4_min'])
        ],
        'Set 5': [
            convert_to_int(match_data['home_team_score_set_5']),
            convert_to_int(match_data['away_team_score_set_5']),
            format_time(match_data['time_set_5_min'])
        ]
    })


    # Remove empty columns (sets not played)
    df = df.loc[:, (df != '').any(axis=0)]

    # Load CSS
    css_file_path = "css/matches_score.css"
    with open(css_file_path, 'r') as css_file:
        custom_css = f'<style>{css_file.read()}</style>'

    # Generate HTML table without index
    table_html = df.to_html(index=False)

    full_html = custom_css + table_html
    
    # Display the HTML table in Streamlit
    st.write(full_html, unsafe_allow_html=True)

st.header("Match score")
# Display the score table for the selected match
simple_tennis_score_table(selected_match)