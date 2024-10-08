import streamlit as st
import duckdb
from datetime import datetime

# Connexion à la base de données DuckDB
conn = duckdb.connect('tennis.db')

# Fonction pour obtenir la liste des tournois
def get_leagues():
    return conn.execute("SELECT DISTINCT league_name FROM dim_leagues ORDER BY league_name").fetchall()


# Fonction pour obtenir les informations du tournoi sélectionné pour une année spécifique
def get_league_info(league_name):
    query = """
    SELECT league_name, current_champion_team_name, primary_color
    FROM dim_leagues
    WHERE league_name = ?
    """
    return conn.execute(query, [league_name]).fetchone()

# Configuration de la page Streamlit
st.set_page_config(page_title="Champions des Tournois", layout="wide")

# Titre de l'application
st.title("Champions des Tournois")

# Sélection du tournoi
leagues = get_leagues()
selected_tournament = st.selectbox("Choisissez un tournoi", [league[0] for league in leagues])


# Obtention des informations du tournoi sélectionné pour l'année choisie
league_info = get_league_info(selected_tournament)

if league_info:
    league_name, champion, color = league_info
    
    # Définition de la couleur de fond
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {color};
        }}
        </style>
        """,
        unsafe_allow_html=True
    )
    
    # Affichage des informations
    st.header(f"Tournoi : {league_name}")
    st.subheader(f"Champion : {champion}")
else:
    st.error("Aucune information trouvée pour ce tournoi et cette année.")

# Fermeture de la connexion à la base de données
conn.close()