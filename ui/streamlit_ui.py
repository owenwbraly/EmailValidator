"""
Streamlit UI for Email Validator • Cleaner • Deduper
Deterministic-only processing without AI functionality
"""

import streamlit as st
import pandas as pd
import io
from typing import Optional, Dict, Any, List, Tuple
import time

from core.pipeline import EmailValidationPipeline


class EmailValidatorUI:
    def __init__(self):
        self.pipeline = None
        # Initialize session state for persistent results
        if 'processing_results' not in st.session_state:
            st.session_state.processing_results = None
        if 'processing_options' not in st.session_state:
            st.session_state.processing_options = None
        
    def run(self):
        """Main UI rendering method"""
        # Header
        st.title("📧 Email Validator • Cleaner • Deduper")
        st.markdown("**Fast deterministic email validation and cleaning for large spreadsheets.**")
        
        # Add clear session button if results exist
        if st.session_state.processing_results:
            with st.sidebar:
                if st.button("🔄 Clear Results & Start New"):
                    st.session_state.processing_results = None
                    st.session_state.processing_options = None
                    st.rerun()
        
        # Important notice
        st.info("🔍 This tool validates email plausibility and hygiene. It does **not** guarantee SMTP deliverability.")
        
        # File upload section
        uploaded_file = self._render_upload_section()
        
        # Options section
        options = self._render_options_section()
        
        # Process button and progress
        if uploaded_file is not None:
            if st.button("🚀 Process Email Data", type="primary", use_container_width=True):
                self._process_file(uploaded_file, options)
        
        # Results and download section
        if st.session_state.processing_results:
            self._render_results_section()
    
    def _render_upload_section(self):
        """Render file upload section"""
        st.subheader("📁 Upload Data File")
        
        uploaded_file = st.file_uploader(
            "Choose a data file",
            type=['csv', 'xlsx', 'xls', 'json', 'tsv'],
            help="Supports CSV, Excel, JSON, and TSV files. The tool will automatically detect email columns."
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
        """Show enhanced preview with email column detection"""
        try:
            from utils.email_col_detector import EmailColumnDetector
            from utils.io_handler import FileHandler
            
            # Use lightweight preview for large files
            file_handler = FileHandler()
            file_data = file_handler.get_file_preview(uploaded_file)
            actual_row_counts = file_handler.get_file_row_counts(uploaded_file)
            
            # Show preview warning for large files
            file_size = len(uploaded_file.getvalue()) / (1024 * 1024)  # MB
            if file_size > 10:
                st.info(f"ℹ️ Large file detected ({file_size:.1f} MB). Showing preview of first {file_handler.preview_rows} rows per sheet.")
            detector = EmailColumnDetector()
            
            # Detect email columns across all sheets
            email_columns_found = {}
            for sheet_name, df in file_data.items():
                email_col = detector.detect_email_column(df)
                if email_col:
                    email_columns_found[sheet_name] = email_col
            
            # Display summary
            if email_columns_found:
                st.success(f"📧 **Email columns detected:** {len(email_columns_found)} sheet(s) with email data")
                
                for sheet_name, email_col in email_columns_found.items():
                    st.write(f"**{sheet_name}**: Email column '**{email_col}**' found")
                    
                    # Show sample emails from this sheet
                    df = file_data[sheet_name]
                    sample_emails = df[email_col].dropna().head(3).tolist()
                    if sample_emails:
                        st.write(f"  Sample emails: {', '.join(str(e) for e in sample_emails)}")
            else:
                st.warning("⚠️ **No email columns detected** - Please check your file has email data with proper headers")
            
            # Show sheet/file structure
            st.write("**📋 File Structure:**")
            
            if uploaded_file.name.endswith('.csv'):
                df = file_data['main']
                actual_rows = actual_row_counts.get('main', len(df))
                col1, col2, col3 = st.columns(3)
                col1.metric("Rows", f"{actual_rows:,}")
                col2.metric("Columns", len(df.columns))
                col3.metric("Email Column", "✅" if 'main' in email_columns_found else "❌")
                
                # Show preview notice if preview is limited
                if len(df) < actual_rows:
                    st.info(f"📋 Showing preview of first {len(df):,} rows out of {actual_rows:,} total rows")
                
                st.write("**Column Headers:**")
                email_col = email_columns_found.get('main')
                st.write(", ".join(f"**{col}**" if col == email_col else col for col in df.columns))
                
                # Preview data
                st.write("**First 5 rows:**")
                st.dataframe(df.head())
                
            else:
                # Multi-sheet file (Excel/JSON with multiple sections)
                actual_total_rows = sum(actual_row_counts.values())
                preview_total_rows = sum(len(df) for df in file_data.values())
                sheets_with_emails = len(email_columns_found)
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Sheets", len(file_data))
                col2.metric("Total Rows", f"{actual_total_rows:,}")
                col3.metric("Sheets with Emails", sheets_with_emails)
                
                # Show preview notice if data is limited
                if preview_total_rows < actual_total_rows:
                    st.info(f"📋 Showing preview of first rows per sheet. Total actual rows: {actual_total_rows:,}")
                
                # Show each sheet
                for sheet_name, df in file_data.items():
                    has_email = sheet_name in email_columns_found
                    email_icon = "📧" if has_email else "📄"
                    actual_sheet_rows = actual_row_counts.get(sheet_name, len(df))
                    
                    # Sheet title with actual row count
                    sheet_title = f"{email_icon} **{sheet_name}** ({actual_sheet_rows:,} rows, {len(df.columns)} columns)"
                    if len(df) < actual_sheet_rows:
                        sheet_title += f" (showing first {len(df):,})"
                    
                    with st.expander(sheet_title):
                        if has_email:
                            st.success(f"Email column: **{email_columns_found[sheet_name]}**")
                            
                            # Show sample emails
                            email_col = email_columns_found[sheet_name]
                            sample_emails = df[email_col].dropna().head(3).tolist()
                            if sample_emails:
                                st.write("Sample emails:", ", ".join(str(e) for e in sample_emails))
                        else:
                            st.info("No email column detected - this sheet will pass through unchanged")
                        
                        st.write("**Columns:**", ", ".join(df.columns))
                        st.dataframe(df.head())
            
        except Exception as e:
            st.error(f"Could not analyze file: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
    
    def _render_validation_charts(self, summary: Dict[str, int], total_processed: int):
        """Render validation statistics with charts"""
        col1, col2 = st.columns(2)
        
        with col1:
            # Pie chart of email processing results
            try:
                import plotly.express as px
                
                data = {
                    'Status': ['Accepted', 'Fixed', 'Removed', 'Duplicates'],
                    'Count': [
                        summary.get('accepted', 0),
                        summary.get('fixed', 0), 
                        summary.get('removed', 0),
                        summary.get('duplicates', 0)
                    ],
                    'Color': ['#28a745', '#ffc107', '#dc3545', '#17a2b8']
                }
                
                # Filter out zero counts
                filtered_data = {k: [v for i, v in enumerate(vs) if data['Count'][i] > 0] for k, vs in data.items()}
                
                if filtered_data['Count']:
                    fig = px.pie(
                        values=filtered_data['Count'],
                        names=filtered_data['Status'],
                        title="Email Processing Results",
                        color_discrete_sequence=filtered_data['Color']
                    )
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                # Fallback to simple metrics if plotly not available
                st.write("**Processing Results:**")
                st.write(f"✅ Accepted: {summary.get('accepted', 0)}")
                st.write(f"🔧 Fixed: {summary.get('fixed', 0)}")
                st.write(f"❌ Removed: {summary.get('removed', 0)}")
                st.write(f"🔄 Duplicates: {summary.get('duplicates', 0)}")
            
        with col2:
            # Processing efficiency metrics
            success_rate = (summary.get('accepted', 0) + summary.get('fixed', 0)) / total_processed * 100 if total_processed > 0 else 0
            fix_rate = summary.get('fixed', 0) / total_processed * 100 if total_processed > 0 else 0
            removal_rate = summary.get('removed', 0) / total_processed * 100 if total_processed > 0 else 0
            duplicate_rate = summary.get('duplicates', 0) / total_processed * 100 if total_processed > 0 else 0
            
            st.write("**📈 Processing Efficiency:**")
            st.write(f"Success Rate: {success_rate:.1f}%")
            st.write(f"Fix Rate: {fix_rate:.1f}%")
            st.write(f"Removal Rate: {removal_rate:.1f}%")
            st.write(f"Duplicate Rate: {duplicate_rate:.1f}%")
        
        # Detailed analysis
        col3, col4 = st.columns(2)
        
        with col3:
            st.write("**📋 Processing Analysis:**")
            quality_score = (summary.get('accepted', 0) + summary.get('fixed', 0)) / total_processed * 100 if total_processed > 0 else 0
            
            if quality_score >= 80:
                st.success(f"🎉 **Excellent quality!** {quality_score:.1f}% of emails were valid or successfully fixed")
            elif quality_score >= 60:
                st.warning(f"⚠️ **Good quality** with room for improvement. {quality_score:.1f}% success rate")
            else:
                st.error(f"🚨 **Quality concerns** detected. Only {quality_score:.1f}% success rate")
        
        with col4:
            # Report summaries - use session state for persistence
            results = st.session_state.processing_results
            changes_report = results.get('changes_report', pd.DataFrame()) if results else pd.DataFrame()
            rejected_data = results.get('rejected_data', pd.DataFrame()) if results else pd.DataFrame()
            changes_count = len(changes_report)
            rejected_count = len(rejected_data)
            
            st.write("**📈 Key Insights:**")
            if summary.get('fixed', 0) > 0:
                st.write(f"• Fixed {summary.get('fixed', 0)} typos and formatting issues")
            if summary.get('duplicates', 0) > 0:
                st.write(f"• Removed {summary.get('duplicates', 0)} duplicate entries")
            if rejected_count > 0:
                st.write(f"• Filtered out {rejected_count} invalid/risky emails")
            if changes_count > 0:
                st.write(f"• Made {changes_count} total modifications")
    
    def _render_options_section(self) -> Dict[str, Any]:
        """Render processing options section"""
        st.subheader("⚙️ Processing Options")
        
        col1, col2 = st.columns(2)
        
        with col1:
            exclude_role_accounts = st.checkbox(
                "Exclude role accounts",
                value=True,
                help="Remove system emails like info@, sales@, admin@, etc."
            )
            
            provider_aware_dedup = st.checkbox(
                "Provider-aware de-duplication",
                value=True,
                help="Use Gmail dot/plus tag semantics for de-duplication"
            )
        
        with col2:
            export_reports = st.checkbox(
                "Export detailed reports",
                value=True,
                help="Generate rejected, changes, and duplicates reports"
            )
        
        return {
            "exclude_role_accounts": exclude_role_accounts,
            "provider_aware_dedup": provider_aware_dedup,
            "export_reports": export_reports
        }
    
    def _process_file(self, uploaded_file, options: Dict[str, Any]):
        """Process the uploaded file with progress tracking"""
        try:
            # Initialize pipeline
            self.pipeline = EmailValidationPipeline(options)
            
            # Create progress container
            progress_container = st.container()
            
            with progress_container:
                progress_bar = st.progress(0)
                status_text = st.empty()
            
            # Process file with progress callbacks
            def update_progress(step: str, progress: float, counters: Dict[str, int]):
                progress_bar.progress(progress)
                status_text.text(f"Status: {step}")
            
            # Process the file
            results = self.pipeline.process_file(uploaded_file, update_progress)
            
            # Store results in session state to persist across reruns
            st.session_state.processing_results = results
            st.session_state.processing_options = options
            
            # Show completion
            progress_bar.progress(1.0)
            status_text.text("✅ Processing complete!")
            
            st.success("🎉 **Done!** Cleaned dataset and reports are ready for download.")
            
        except Exception as e:
            st.error(f"❌ **Processing Error**: {str(e)}")
            st.exception(e)
    
    def _render_results_section(self):
        """Render results and download section with enhanced analytics"""
        # Use results from session state to persist across reruns
        results = st.session_state.processing_results
        if not results:
            return
        
        st.subheader("📊 Results Summary")
        
        # Summary metrics  
        summary = results.get('summary', {})
        total_processed = summary.get('accepted', 0) + summary.get('fixed', 0) + summary.get('removed', 0) + summary.get('duplicates', 0)
        
        col1, col2, col3, col4 = st.columns(4)
        
        col1.metric("✅ Accepted", summary.get('accepted', 0))
        col2.metric("🔧 Fixed", summary.get('fixed', 0))
        col3.metric("❌ Removed", summary.get('removed', 0))
        col4.metric("🔄 Duplicates", summary.get('duplicates', 0))
        
        # Add validation statistics with charts
        if total_processed > 0:
            st.subheader("📈 Validation Statistics")
            self._render_validation_charts(summary, total_processed)
        
        # Download section
        st.subheader("📥 Download Results")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Main cleaned dataset
            if 'cleaned_data' in results:
                cleaned_data = results['cleaned_data']
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
            if 'rejected_data' in results and not results['rejected_data'].empty:
                rejected_csv = results['rejected_data'].to_csv(index=False)
                st.download_button(
                    "🗑️ Download Rejected Rows",
                    data=rejected_csv,
                    file_name=f"rejected_{int(time.time())}.csv",
                    mime="text/csv"
                )
        
        # Additional reports
        if results.get('options', {}).get('export_reports', False):
            col3, col4 = st.columns(2)
            
            with col3:
                if 'changes_report' in results:
                    changes_report = results['changes_report']
                    if not changes_report.empty:
                        changes_csv = changes_report.to_csv(index=False)
                        st.download_button(
                            "📝 Download Changes Report",
                            data=changes_csv,
                            file_name=f"changes_{int(time.time())}.csv",
                            mime="text/csv"
                        )
                    else:
                        st.info("📝 No changes made to email addresses")
            
            with col4:
                if 'duplicates_report' in results:
                    duplicates_report = results['duplicates_report']
                    if not duplicates_report.empty:
                        duplicates_csv = duplicates_report.to_csv(index=False)
                        st.download_button(
                            "🔄 Download Duplicates Report",
                            data=duplicates_csv,
                            file_name=f"duplicates_{int(time.time())}.csv",
                            mime="text/csv"
                        )
                        # Show preview of duplicates found
                        total_duplicates = len(duplicates_report[duplicates_report['status'] == 'REMOVED'])
                        if total_duplicates > 0:
                            st.caption(f"🔄 Found {total_duplicates} duplicate emails across {len(duplicates_report['canonical_key'].unique())} groups")
                    else:
                        st.info("🔄 No duplicate emails found")
