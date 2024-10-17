import streamlit as st
import pandas as pd
import duckdb

# Configuration de la connexion DuckDB
@st.cache_resource
def init_connection():
    return duckdb.connect('../tennis.db')

# Charger les données depuis DuckDB
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

selected_match = st.selectbox(
    "Choose a match",
    options= matches_data["match_name"]
)

# You can now use selected_season and other_selection in your app
st.write(f"Selected tournament: {selected_season}")
st.write(f"Round: {selected_round}")
