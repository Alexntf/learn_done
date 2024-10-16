import streamlit as st

st.set_page_config(page_title="Tennis Tournament App", page_icon="🎾")

st.write("# Welcome to the Tennis Tournament App! 🎾")

st.sidebar.success("Select a page above.")

st.markdown(
    """
    This application provides information about various tennis tournaments.
    
    ### What you can do:
    - View tournament details
    - See the best performers for each tournament
    - Analyze historical data
    
    To get started, select 'Tournament Info' from the sidebar.

    ### Warning 

    Data is not updated since the end of 2023 season.
    """
)