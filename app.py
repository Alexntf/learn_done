import streamlit as st
import pandas as pd
import duckdb

# Configuration de la connexion DuckDB
@st.cache_resource
def init_connection():
    return duckdb.connect('tennis.db')

# Charger les données depuis DuckDB
@st.cache_data
def load_data():
    conn = init_connection()
    leagues_query = "SELECT * FROM dim_leagues"
    leagues_result = conn.execute(leagues_query).fetchdf()
    
    most_titles_query = "SELECT * FROM dim_most_titles"
    most_titles_result = conn.execute(most_titles_query).fetchdf()
    
    return leagues_result, most_titles_result

# Fonction pour formater les valeurs numériques
def format_value(value):
    if isinstance(value, (int, float)):
        return f"{value:.0f}"
    return value

# Chargement des données
leagues_data, most_titles_data = load_data()

# Ajout du CSS personnalisé corrigé
st.markdown("""
<style>
.info-container {
    background-color: #e8eae8;
    border-radius: 8px;
    padding: 12px 20px;
    margin-bottom: 10px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.info-label {
    font-weight: bold;
    font-size: 16px;
    color: #000000;
}

.info-value {
    font-size: 16px;
    color: #000000;
}
</style>
""", unsafe_allow_html=True)

# Fonction pour afficher une information dans un rectangle gris
def display_info(label, value):
    st.markdown(f"""
    <div class="info-container">
        <span class="info-label">{label}</span>
        <span class="info-value">{value}</span>
    </div>
    """, unsafe_allow_html=True)

def colored_subheader(text, color):
    st.markdown(f"""
    <h3 style="color: {color};">
        {text}
    </h3>
    """, unsafe_allow_html=True) 


# Sélection du tournoi
selected_tournament = st.selectbox(
    "Choisissez un tournoi",
    options=leagues_data['league_name'].unique()
)

# Filtrer les données pour le tournoi sélectionné
tournament_info = leagues_data[leagues_data['league_name'] == selected_tournament].iloc[0]


colored_subheader("Tournament Information", tournament_info['primary_color'])

# Affichage des détails du tournoi
col1, col2 = st.columns(2)

with col1:
    
    display_info("League Name", tournament_info['league_name'])
    display_info("Year", format_value(tournament_info['year']))
    display_info("Start Date", tournament_info['start_date'])
    display_info("End Date", tournament_info['end_date'])

with col2:
    display_info("Current Champion", tournament_info['current_champion_team_name'])
    display_info("Number of Sets", format_value(tournament_info['number_of_sets']))
    display_info("Max Points", format_value(tournament_info['max_points']))
    display_info("Ground", tournament_info['ground'])



# Affichage des joueurs les plus titrés
colored_subheader("Best Performers", tournament_info['secondary_color'])

# Filtrer les données pour le tournoi sélectionné et trier par nombre de titres
best_performers = most_titles_data[most_titles_data['league_name'] == selected_tournament].sort_values(by='most_titles', ascending=False)

for _, performer in best_performers.iterrows():
    display_info(performer['team_name'], f"Titles: {format_value(performer['most_titles'])}")
