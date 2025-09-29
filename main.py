"""
Email Validator • Cleaner • Deduper
Main Streamlit application entry point
Deterministic-only email validation and cleaning
"""

import streamlit as st
from ui.streamlit_ui import EmailValidatorUI

def main():
    st.set_page_config(
        page_title="Email Validator • Cleaner • Deduper",
        page_icon="📧",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    # Initialize and run the UI
    ui = EmailValidatorUI()
    ui.run()

if __name__ == "__main__":
    main()