"""
Email Validator • Cleaner • Deduper (LLM-First)
Main Streamlit application entry point
"""

import streamlit as st
from app.ui import EmailValidatorUI

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
