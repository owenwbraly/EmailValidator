"""
Streamlit UI for Email Validator • Cleaner • Deduper
"""

import streamlit as st
import pandas as pd
import os
import io
from typing import Optional, Dict, Any, List, Tuple
import time
import json

from .pipeline import EmailValidationPipeline
from .llm_adapter import LLMAdapter


class EmailValidatorUI:
    def __init__(self):
        self.pipeline = None
        self.results = None
        
    def run(self):
        """Main UI rendering method"""
        # Header
        st.title("📧 Email Validator • Cleaner • Deduper (LLM-First)")
        st.markdown("**LLM-powered validation and cleaning for big spreadsheets.**")
        
        # Important notice
        st.info("🔍 This tool validates plausibility and hygiene. It does **not** guarantee SMTP deliverability.")
        
        # Check for API keys and show blocking message if missing
        if not self._check_api_keys():
            st.error("🚫 **API Key Required**: This tool requires an LLM API key to function. Please set OPENAI_API_KEY or ANTHROPIC_API_KEY in your environment.")
            st.stop()
        
        # File upload section
        uploaded_file = self._render_upload_section()
        
        # LLM settings section
        llm_config = self._render_llm_settings()
        
        # Options section
        options = self._render_options_section()
        
        # Process button and progress
        if uploaded_file is not None:
            if st.button("🚀 Process Email Data", type="primary", use_container_width=True):
                self._process_file(uploaded_file, llm_config, options)
        
        # Results and download section
        if self.results:
            self._render_results_section()
    
    def _check_api_keys(self) -> bool:
        """Check if at least one API key is available"""
        openai_key = os.getenv("OPENAI_API_KEY")
        anthropic_key = os.getenv("ANTHROPIC_API_KEY")
        return bool(openai_key or anthropic_key)
    
    def _render_upload_section(self):
        """Render file upload section"""
        st.subheader("📁 Upload Data File")
        
        uploaded_file = st.file_uploader(
            "Choose a CSV or Excel file",
            type=['csv', 'xlsx', 'xls'],
            help="Supports CSV and Excel files with multiple sheets. The tool will automatically detect email columns."
        )
        
        if uploaded_file:
            # Show file info
            file_size = len(uploaded_file.getvalue()) / (1024 * 1024)  # MB
            st.success(f"📊 File loaded: **{uploaded_file.name}** ({file_size:.2f} MB)")
            
            # Show preview
            if st.checkbox("👀 Preview file structure"):
                self._show_file_preview(uploaded_file)
        
        return uploaded_file
    
    def _show_file_preview(self, uploaded_file):
        """Show a preview of the uploaded file structure"""
        try:
            if uploaded_file.name.endswith('.csv'):
                df_preview = pd.read_csv(uploaded_file, nrows=5)
                st.write("**CSV Preview (first 5 rows):**")
                st.dataframe(df_preview)
            else:
                # Excel file - show sheet info
                sheets = pd.read_excel(uploaded_file, sheet_name=None, nrows=0)
                st.write("**Excel Sheets:**")
                for sheet_name, df in sheets.items():
                    st.write(f"- **{sheet_name}**: {len(df.columns)} columns")
                
                # Show preview of first sheet
                first_sheet = list(sheets.keys())[0]
                df_preview = pd.read_excel(uploaded_file, sheet_name=first_sheet, nrows=5)
                st.write(f"**Preview of '{first_sheet}' (first 5 rows):**")
                st.dataframe(df_preview)
        except Exception as e:
            st.warning(f"Could not preview file: {str(e)}")
    
    def _render_llm_settings(self) -> Dict[str, Any]:
        """Render LLM configuration section"""
        st.subheader("🤖 LLM Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Provider selection
            available_providers = []
            if os.getenv("OPENAI_API_KEY"):
                available_providers.append("OpenAI")
            if os.getenv("ANTHROPIC_API_KEY"):
                available_providers.append("Anthropic")
            
            provider = st.selectbox(
                "Provider",
                available_providers,
                help="Choose your LLM provider based on available API keys"
            )
        
        with col2:
            # Model selection
            if provider == "OpenAI":
                model = st.selectbox(
                    "Model",
                    ["gpt-5", "gpt-4o", "gpt-4"],
                    help="Latest OpenAI models for email validation"
                )
            else:  # Anthropic
                model = st.selectbox(
                    "Model",
                    ["claude-sonnet-4-20250514", "claude-3-7-sonnet-20250219", "claude-3-5-sonnet-20241022"],
                    help="Latest Anthropic models for email validation"
                )
        
        return {
            "provider": provider.lower(),
            "model": model
        }
    
    def _render_options_section(self) -> Dict[str, Any]:
        """Render processing options section"""
        st.subheader("⚙️ Processing Options")
        
        col1, col2 = st.columns(2)
        
        with col1:
            confidence_threshold = st.slider(
                "Confidence Threshold",
                min_value=0.50,
                max_value=0.99,
                value=0.85,
                step=0.05,
                help="Minimum confidence required for automatic fixes"
            )
            
            exclude_role_accounts = st.checkbox(
                "Exclude role accounts",
                value=True,
                help="Remove system emails like info@, sales@, admin@, etc."
            )
        
        with col2:
            provider_aware_dedup = st.checkbox(
                "Provider-aware de-duplication",
                value=True,
                help="Use Gmail dot/plus tag semantics for de-duplication"
            )
            
            export_reports = st.checkbox(
                "Export detailed reports",
                value=True,
                help="Generate rejected, changes, and duplicates reports"
            )
        
        return {
            "confidence_threshold": confidence_threshold,
            "exclude_role_accounts": exclude_role_accounts,
            "provider_aware_dedup": provider_aware_dedup,
            "export_reports": export_reports
        }
    
    def _process_file(self, uploaded_file, llm_config: Dict[str, Any], options: Dict[str, Any]):
        """Process the uploaded file with progress tracking"""
        try:
            # Initialize pipeline
            self.pipeline = EmailValidationPipeline(llm_config, options)
            
            # Create progress containers
            progress_container = st.container()
            counters_container = st.container()
            
            with progress_container:
                progress_bar = st.progress(0)
                status_text = st.empty()
            
            with counters_container:
                col1, col2, col3, col4 = st.columns(4)
                accepted_metric = col1.empty()
                fixed_metric = col2.empty()
                removed_metric = col3.empty()
                duplicates_metric = col4.empty()
            
            # Process file with progress callbacks
            def update_progress(step: str, progress: float, counters: Dict[str, int]):
                progress_bar.progress(progress)
                status_text.text(f"Status: {step}")
                
                accepted_metric.metric("✅ Accepted", counters.get('accepted', 0))
                fixed_metric.metric("🔧 Fixed", counters.get('fixed', 0))
                removed_metric.metric("❌ Removed", counters.get('removed', 0))
                duplicates_metric.metric("🔄 Duplicates", counters.get('duplicates', 0))
            
            # Process the file
            self.results = self.pipeline.process_file(uploaded_file, update_progress)
            
            # Show completion
            progress_bar.progress(1.0)
            status_text.text("✅ Processing complete!")
            
            st.success("🎉 **Done!** Cleaned dataset and reports are ready for download.")
            
        except Exception as e:
            st.error(f"❌ **Processing Error**: {str(e)}")
            st.exception(e)
    
    def _render_results_section(self):
        """Render results and download section"""
        if not self.results:
            return
        
        st.subheader("📊 Results Summary")
        
        # Summary metrics
        summary = self.results.get('summary', {})
        col1, col2, col3, col4 = st.columns(4)
        
        col1.metric("✅ Accepted", summary.get('accepted', 0))
        col2.metric("🔧 Fixed", summary.get('fixed', 0))
        col3.metric("❌ Removed", summary.get('removed', 0))
        col4.metric("🔄 Duplicates", summary.get('duplicates', 0))
        
        # Download section
        st.subheader("📥 Download Results")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Main cleaned dataset
            if 'cleaned_data' in self.results:
                cleaned_data = self.results['cleaned_data']
                filename = f"cleaned_{int(time.time())}"
                
                if isinstance(cleaned_data, dict):  # Excel with multiple sheets
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        for sheet_name, df in cleaned_data.items():
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    st.download_button(
                        "📊 Download Cleaned Dataset (Excel)",
                        data=excel_buffer.getvalue(),
                        file_name=f"{filename}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                else:  # CSV
                    csv_data = cleaned_data.to_csv(index=False)
                    st.download_button(
                        "📊 Download Cleaned Dataset (CSV)",
                        data=csv_data,
                        file_name=f"{filename}.csv",
                        mime="text/csv"
                    )
        
        with col2:
            # Rejected dataset
            if 'rejected_data' in self.results and not self.results['rejected_data'].empty:
                rejected_csv = self.results['rejected_data'].to_csv(index=False)
                st.download_button(
                    "🗑️ Download Rejected Rows",
                    data=rejected_csv,
                    file_name=f"rejected_{int(time.time())}.csv",
                    mime="text/csv"
                )
        
        # Additional reports
        if self.results.get('options', {}).get('export_reports', False):
            col3, col4 = st.columns(2)
            
            with col3:
                if 'changes_report' in self.results and not self.results['changes_report'].empty:
                    changes_csv = self.results['changes_report'].to_csv(index=False)
                    st.download_button(
                        "📝 Download Changes Report",
                        data=changes_csv,
                        file_name=f"changes_{int(time.time())}.csv",
                        mime="text/csv"
                    )
            
            with col4:
                if 'duplicates_report' in self.results and not self.results['duplicates_report'].empty:
                    duplicates_csv = self.results['duplicates_report'].to_csv(index=False)
                    st.download_button(
                        "🔄 Download Duplicates Report",
                        data=duplicates_csv,
                        file_name=f"duplicates_{int(time.time())}.csv",
                        mime="text/csv"
                    )
