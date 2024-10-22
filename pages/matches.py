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

# Filter rounds based on selected season
season_data = matches_data[matches_data['season_name'] == selected_season]

with col2:
    selected_round = st.selectbox(
        "Round",
        options=season_data['round_name'].unique(),
        key="round_select"
    )

# Filter data based on selected season and round
season_data = matches_data[matches_data['season_name'] == selected_season]
round_data = season_data[season_data['round_name'] == selected_round]

selected_match = st.selectbox(
    "Choose a match",
    options= round_data["match_name"]
)


# Get data for the selected match
selected_match = round_data[round_data['match_name'] == selected_match].sample(n=1).iloc[0]

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

def get_head_to_head(matches_data, player1, player2, current_match_id):
    # Filtrer les matchs où les deux joueurs s'affrontent
    h2h_matches = matches_data[
        (((matches_data['home_team_name'] == player1) & (matches_data['away_team_name'] == player2)) |
        ((matches_data['home_team_name'] == player2) & (matches_data['away_team_name'] == player1))) &
        (matches_data['match_id'] != current_match_id)  # Exclure le match actuel
    ].copy()
    
    if len(h2h_matches) == 0:
        return pd.DataFrame()
    
    # Trier par date de match décroissante
    h2h_matches = h2h_matches.sort_values('start_time', ascending=False)
    
    # Convertir les scores en entiers
    for idx, row in h2h_matches.iterrows():
        # Convertir en entier et formater comme string sans décimale
        home_score = str(int(float(row['home_team_score_display'])))
        away_score = str(int(float(row['away_team_score_display'])))
        
        # Stocker les scores comme strings d'entiers
        h2h_matches.at[idx, 'home_team_score_display'] = home_score
        h2h_matches.at[idx, 'away_team_score_display'] = away_score
        
        if int(home_score) > int(away_score):
            h2h_matches.at[idx, 'winner_team_name'] = row['home_team_name']
        else:
            h2h_matches.at[idx, 'winner_team_name'] = row['away_team_name']
    
    return h2h_matches.head(5)

def display_head_to_head(match_data, matches_data):
    player1 = match_data['home_team_name']
    player2 = match_data['away_team_name']
    
    h2h_data = get_head_to_head(matches_data, player1, player2, match_data['match_id'])
    
    if len(h2h_data) == 0:
        st.write("No previous matches found between these players.")
        return

    st.subheader("Previous Matches")
    
    # Charger le CSS depuis le fichier
    with open('css/head_to_head.css', 'r') as css_file:
        css = css_file.read()
    st.markdown(f'<style>{css}</style>', unsafe_allow_html=True)

    # Affichage des confrontations
    for _, match in h2h_data.iterrows():
        home_class = "winner-name" if match['winner_team_name'] == match['home_team_name'] else "loser-name"
        away_class = "winner-name" if match['winner_team_name'] == match['away_team_name'] else "loser-name"
        
        match_html = f"""
        <div class="match-history">
            <div class="player-name">
                <span class="{home_class}">{match['home_team_name']}</span>
            </div>
            <div class="score-display">
                {match['home_team_score_display']} - {match['away_team_score_display']}
            </div>
            <div class="player-name">
                <span class="{away_class}">{match['away_team_name']}</span>
            </div>
        </div>
        <div class="match-info">
            {match['season_name']} - {match['round_name']} ({match['match_date']})
        </div>
        """
        st.markdown(match_html, unsafe_allow_html=True)

st.header("Match score")
# Display the score table for the selected match
simple_tennis_score_table(selected_match)

st.header("Head to Head")
display_head_to_head(selected_match, matches_data)