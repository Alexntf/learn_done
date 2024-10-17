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
    matches_query = "SELECT * FROM matches"
    matches_result = conn.execute(matches_query).fetchdf()
    
    return matches_result

