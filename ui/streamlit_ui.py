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
        st.title("👥 People Validator")
        st.markdown("**Clean and validate people data from CSV files with complete contact information.**")
        
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
            if st.button("🚀 Extract People Data", type="primary", width='stretch'):
                self._process_file(uploaded_file, options)
        
        # Results and download section
        if st.session_state.processing_results:
            self._render_people_results_section()
    
    def _render_upload_section(self):
        """Render file upload section"""
        st.subheader("📁 Upload Data File")
        
        uploaded_file = st.file_uploader(
            "Choose a data file",
            type=['csv', 'xlsx', 'xls', 'json', 'tsv'],
            help="Supports CSV, Excel, JSON, and TSV files. The tool will automatically detect person data columns."
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
            # Detect person columns across all sheets using semantic detection
            from utils.semantic_col_detector import SemanticColumnDetector
            semantic_detector = SemanticColumnDetector()
            
            person_columns_found = {}
            for sheet_name, df in file_data.items():
                person_cols = semantic_detector.detect_person_columns(df)
                if any(person_cols.values()):
                    person_columns_found[sheet_name] = person_cols
            
            # Display summary
            if person_columns_found:
                st.success(f"👥 **Person data detected:** {len(person_columns_found)} sheet(s) with person information")
                
                for sheet_name, person_cols in person_columns_found.items():
                    st.write(f"**{sheet_name}**: Person columns detected")
                    
                    # Show detected columns
                    detected_fields = []
                    for field_type, col_name in person_cols.items():
                        if col_name:
                            confidence = semantic_detector.get_detection_confidence(
                                file_data[sheet_name], field_type, col_name
                            )
                            detected_fields.append(f"**{field_type}**: {col_name} ({confidence:.0%})")
                    
                    if detected_fields:
                        st.write("  " + " | ".join(detected_fields))
                    
                    # Show sample data
                    df = file_data[sheet_name]
                    if person_cols.get('first_name') and person_cols.get('last_name'):
                        first_col = person_cols['first_name']
                        last_col = person_cols['last_name']
                        sample_names = []
                        for i in range(min(3, len(df))):
                            first = str(df.iloc[i][first_col]) if first_col in df.columns else ""
                            last = str(df.iloc[i][last_col]) if last_col in df.columns else ""
                            if first and last and first != 'nan' and last != 'nan':
                                sample_names.append(f"{first} {last}")
                        if sample_names:
                            st.write(f"  Sample names: {', '.join(sample_names)}")
            else:
                st.warning("⚠️ **No person data detected** - Please check your file has person information with recognizable column names")
            
            # Show sheet/file structure
            st.write("**📋 File Structure:**")
            
            if uploaded_file.name.endswith('.csv'):
                df = file_data['main']
                actual_rows = actual_row_counts.get('main', len(df))
                col1, col2, col3 = st.columns(3)
                col1.metric("Rows", f"{actual_rows:,}")
                col2.metric("Columns", len(df.columns))
                col3.metric("Person Data", "✅" if 'main' in person_columns_found else "❌")
                
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
                sheets_with_people = len(person_columns_found)
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Sheets", len(file_data))
                col2.metric("Total Rows", f"{actual_total_rows:,}")
                col3.metric("Sheets with People", sheets_with_people)
                
                # Show preview notice if data is limited
                if preview_total_rows < actual_total_rows:
                    st.info(f"📋 Showing preview of first rows per sheet. Total actual rows: {actual_total_rows:,}")
                
                # Show each sheet
                for sheet_name, df in file_data.items():
                    has_people = sheet_name in person_columns_found
                    people_icon = "👥" if has_people else "📄"
                    actual_sheet_rows = actual_row_counts.get(sheet_name, len(df))
                    
                    # Sheet title with actual row count
                    sheet_title = f"{people_icon} **{sheet_name}** ({actual_sheet_rows:,} rows, {len(df.columns)} columns)"
                    if len(df) < actual_sheet_rows:
                        sheet_title += f" (showing first {len(df):,})"
                    
                    with st.expander(sheet_title):
                        if has_people:
                            person_cols = person_columns_found[sheet_name]
                            detected_count = sum(1 for col in person_cols.values() if col)
                            st.success(f"Person data detected: {detected_count} fields")
                            
                            # Show detected fields
                            detected_fields = []
                            for field_type, col_name in person_cols.items():
                                if col_name:
                                    detected_fields.append(f"{field_type}: {col_name}")
                            if detected_fields:
                                st.write("Detected fields:", " | ".join(detected_fields))
                        else:
                            st.info("No person data detected - this sheet will be skipped")
                        
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
                    st.plotly_chart(fig, width='stretch')
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
    
    def _render_people_results_section(self):
        """Render people extraction results with 3-sheet output"""
        # Use results from session state to persist across reruns
        results = st.session_state.processing_results
        if not results:
            return
        
        st.subheader("👥 People Processing Results")
        
        # Summary metrics  
        summary = results.get('summary', {})
        people_count = summary.get('people_extracted', 0)
        accepted_count = summary.get('people_accepted', 0)
        rejected_count = summary.get('people_rejected', 0)
        sheets_count = summary.get('sheets_processed', 0)
        email_stats = summary.get('email_processing', {})
        
        col1, col2, col3, col4 = st.columns(4)
        
        col1.metric("👥 Total People", people_count)
        col2.metric("✅ Accepted", accepted_count)
        col3.metric("❌ Rejected", rejected_count)
        col4.metric("📊 Sheets", sheets_count)
        
        # Email processing statistics
        if email_stats:
            st.subheader("📧 Email Processing Results")
            col1, col2, col3, col4 = st.columns(4)
            
            col1.metric("✅ Accepted", email_stats.get('accepted', 0))
            col2.metric("🔧 Fixed", email_stats.get('fixed', 0))
            col3.metric("❌ Removed", email_stats.get('removed', 0))
            col4.metric("🔄 Duplicates", email_stats.get('duplicates', 0))
        
        # Advanced Analytics Section
        if people_count > 0:
            st.subheader("📊 Advanced Analytics")
            
            # Get data for analytics
            all_people_df = results.get('all_people_data', pd.DataFrame())
            accepted_people_df = results.get('accepted_people_data', pd.DataFrame())
            rejected_people_df = results.get('rejected_people_data', pd.DataFrame())
            
            # Create two columns for charts
            col1, col2 = st.columns(2)
            
            with col1:
                # People Data Completeness Chart
                if not all_people_df.empty:
                    st.write("**👥 People Data Completeness**")
                    
                    # Calculate completeness metrics
                    total_people = len(all_people_df)
                    has_first_name = len(all_people_df[all_people_df['First'].str.strip() != ''])
                    has_last_name = len(all_people_df[all_people_df['Last'].str.strip() != ''])
                    has_email = len(all_people_df[all_people_df['Email'].str.strip() != ''])
                    has_linkedin = len(all_people_df[all_people_df['LinkedIn'].str.strip() != ''])
                    has_title = len(all_people_df[all_people_df['Title'].str.strip() != ''])
                    has_company = len(all_people_df[all_people_df['Company'].str.strip() != ''])
                    
                    # Create completeness data
                    completeness_data = {
                        'Field': ['First Name', 'Last Name', 'Email', 'LinkedIn', 'Title', 'Company'],
                        'Count': [has_first_name, has_last_name, has_email, has_linkedin, has_title, has_company],
                        'Missing': [total_people - has_first_name, total_people - has_last_name, 
                                  total_people - has_email, total_people - has_linkedin, 
                                  total_people - has_title, total_people - has_company]
                    }
                    
                    # Create pie chart for completeness
                    try:
                        import plotly.express as px
                        import plotly.graph_objects as go
                        
                        # Create a stacked bar chart instead of pie for better readability
                        fig = go.Figure()
                        
                        fig.add_trace(go.Bar(
                            name='Has Data',
                            x=completeness_data['Field'],
                            y=completeness_data['Count'],
                            marker_color='#28a745'
                        ))
                        
                        fig.add_trace(go.Bar(
                            name='Missing',
                            x=completeness_data['Field'],
                            y=completeness_data['Missing'],
                            marker_color='#dc3545'
                        ))
                        
                        fig.update_layout(
                            barmode='stack',
                            title="Data Completeness by Field",
                            xaxis_title="Field",
                            yaxis_title="Number of People",
                            height=400
                        )
                        
                        st.plotly_chart(fig, width='stretch')
                        
                        # Show percentages
                        st.write("**Completeness Percentages:**")
                        for i, field in enumerate(completeness_data['Field']):
                            percentage = (completeness_data['Count'][i] / total_people) * 100
                            st.write(f"• {field}: {percentage:.1f}% ({completeness_data['Count'][i]}/{total_people})")
                            
                    except ImportError:
                        # Fallback to simple metrics
                        st.write("**Data Completeness:**")
                        for i, field in enumerate(completeness_data['Field']):
                            percentage = (completeness_data['Count'][i] / total_people) * 100
                            st.write(f"• {field}: {percentage:.1f}% ({completeness_data['Count'][i]}/{total_people})")
            
            with col2:
                # Email Processing Results Chart
                if email_stats and any(email_stats.values()):
                    st.write("**📧 Email Processing Results**")
                    
                    # Prepare email processing data for pie chart
                    email_data = {
                        'Status': ['Accepted', 'Fixed', 'Removed', 'Duplicates'],
                        'Count': [
                            email_stats.get('accepted', 0),
                            email_stats.get('fixed', 0),
                            email_stats.get('removed', 0),
                            email_stats.get('duplicates', 0)
                        ],
                        'Color': ['#28a745', '#ffc107', '#dc3545', '#17a2b8']
                    }
                    
                    # Filter out zero counts
                    filtered_data = {k: [v for i, v in enumerate(vs) if email_data['Count'][i] > 0] for k, vs in email_data.items()}
                    
                    if filtered_data['Count']:
                        try:
                            import plotly.express as px
                            
                            fig = px.pie(
                                values=filtered_data['Count'],
                                names=filtered_data['Status'],
                                title="Email Processing Results",
                                color_discrete_sequence=filtered_data['Color']
                            )
                            fig.update_traces(textposition='inside', textinfo='percent+label')
                            fig.update_layout(height=400)
                            
                            st.plotly_chart(fig, width='stretch')
                            
                        except ImportError:
                            # Fallback to simple metrics
                            st.write("**Email Processing Results:**")
                            for i, status in enumerate(email_data['Status']):
                                count = email_data['Count'][i]
                                if count > 0:
                                    st.write(f"• {status}: {count}")
                    else:
                        st.info("No email processing data available")
                else:
                    st.info("No email processing data available")
        
        # Show sample data from each sheet
        if people_count > 0:
            st.subheader("📋 Data Preview")
            
            # Tabs for different sheets
            tab1, tab2, tab3 = st.tabs(["All People", "Cleaned People", "Rejected People"])
            
            with tab1:
                all_people_df = results.get('all_people_data', pd.DataFrame())
                if not all_people_df.empty:
                    st.write(f"**All People ({len(all_people_df)} total)**")
                    st.dataframe(all_people_df.head(10), width='stretch')
                else:
                    st.info("No people data available")
            
            with tab2:
                accepted_people_df = results.get('accepted_people_data', pd.DataFrame())
                if not accepted_people_df.empty:
                    st.write(f"**Cleaned & Deduplicated People ({len(accepted_people_df)} total)**")
                    st.dataframe(accepted_people_df.head(10), width='stretch')
                else:
                    st.info("No accepted people data available")
            
            with tab3:
                rejected_people_df = results.get('rejected_people_data', pd.DataFrame())
                if not rejected_people_df.empty:
                    st.write(f"**Rejected People ({len(rejected_people_df)} total)**")
                    st.caption("People rejected due to invalid emails, duplicates, or missing email addresses. Sorted by source sheet and row number.")
                    st.dataframe(rejected_people_df.head(10), width='stretch')
                    
                    # Show rejection reasons breakdown
                    if 'Rejection Reason' in rejected_people_df.columns:
                        st.write("**Rejection Reasons:**")
                        rejection_counts = rejected_people_df['Rejection Reason'].value_counts()
                        for reason, count in rejection_counts.head(5).items():
                            if reason:  # Skip empty reasons
                                st.write(f"• {reason}: {count}")
                else:
                    st.info("No rejected people data available")
        
        # Download section
        st.subheader("📥 Download Results")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # 3-Sheet Excel Download
            if all([results.get('all_people_data') is not None, 
                   results.get('accepted_people_data') is not None,
                   results.get('rejected_people_data') is not None]):
                try:
                    from core.pipeline import EmailValidationPipeline
                    pipeline = EmailValidationPipeline({})
                    
                    excel_data = pipeline.create_3_sheet_excel(
                        results['all_people_data'],
                        results['accepted_people_data'], 
                        results['rejected_people_data']
                    )
                    
                    st.download_button(
                        "📊 Download 3-Sheet Excel",
                        data=excel_data,
                        file_name=f"people_analysis_{int(time.time())}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                except Exception as e:
                    st.error(f"Error creating Excel file: {str(e)}")
            else:
                st.warning("Excel download not available")
        
        with col2:
            # Individual CSV downloads
            if results.get('all_people_data') is not None and not results['all_people_data'].empty:
                all_csv = results['all_people_data'].to_csv(index=False)
                st.download_button(
                    "📄 All People CSV",
                    data=all_csv,
                    file_name=f"all_people_{int(time.time())}.csv",
                    mime="text/csv"
                )
