"""
Email validation pipeline orchestrator
Coordinates all processing steps from file loading to output generation
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Callable, List, Tuple, Optional, Union
import time
from io import BytesIO

from utils.io_handler import FileHandler
from utils.email_col_detector import EmailColumnDetector
from utils.output_processor import OutputProcessor
from models.EmailEntry import EmailEntry, EmailArrayExtractor
from .processor import EmailProcessor
from .email_deduper import EmailDeduplicator


class EmailValidationPipeline:
    def __init__(self, options: Dict[str, Any]):
        self.options = options
        
        # Initialize components
        self.file_handler = FileHandler()
        self.detector = EmailColumnDetector()
        self.array_extractor = EmailArrayExtractor(self.detector)
        self.array_processor = EmailProcessor(options)
        self.array_deduplicator = EmailDeduplicator(options)
        self.output_processor = OutputProcessor()
        
        # Progress tracking
        self.counters = {
            'accepted': 0,
            'fixed': 0,
            'removed': 0,
            'duplicates': 0,
            'total_processed': 0
        }
    
    def process_file(self, uploaded_file, progress_callback: Callable = None) -> Dict[str, Any]:
        """
        Main processing pipeline using array mode
        Returns dict with cleaned_data, rejected_data, changes_report, duplicates_report, summary
        """
        self._update_progress("Loading file...", 0.05, progress_callback)
        
        # Step 1: Load file
        file_data = self.file_handler.load_file(uploaded_file)
        
        self._update_progress("Extracting emails as arrays...", 0.15, progress_callback)
        
        # Step 2: Extract all emails as arrays with position tracking
        extraction_results = self.array_extractor.extract_all_emails(file_data)
        
        if extraction_results['non_empty_entries'] == 0:
            raise ValueError("No email addresses found in the uploaded file")
        
        all_entries = extraction_results['email_entries']
        
        self._update_progress("Processing emails with deterministic engine...", 0.40, progress_callback)
        
        # Step 3: Process emails with deterministic engine
        processing_results = self.array_processor.process_email_entries(all_entries)
        
        # Update counters
        self.counters.update(processing_results['results'])
        
        self._update_progress("Running deduplication...", 0.70, progress_callback)
        
        # Step 4: Run deduplication on processed entries
        dedupe_results = self.array_deduplicator.deduplicate_entries(all_entries)
        
        # Update duplicate counter
        self.counters['duplicates'] = dedupe_results['total_duplicates_removed']
        
        self._update_progress("Creating duplicates file and cleaning data...", 0.85, progress_callback)
        
        # Step 5: Create duplicates DataFrame
        duplicates_df = self.array_deduplicator.create_duplicates_dataframe(
            file_data, dedupe_results['duplicate_positions']
        )
        
        # Step 6: Update DataFrames with cleaned emails
        cleaned_data_with_emails = self.array_processor.update_dataframes_with_cleaned_emails(
            file_data, all_entries
        )
        
        # Step 7: Blank duplicate rows in cleaned data
        final_cleaned_data = self.array_deduplicator.blank_duplicate_rows(
            cleaned_data_with_emails, dedupe_results['duplicate_positions']
        )
        
        self._update_progress("Generating reports...", 0.95, progress_callback)
        
        # Step 8: Generate final results
        results = self._generate_results(
            final_cleaned_data, duplicates_df, processing_results['changes_report'],
            processing_results.get('rejected_report', []), dedupe_results['duplicates_report'], 
            uploaded_file.name, extraction_results
        )
        
        self._update_progress("Complete!", 1.0, progress_callback)
        
        return results
    
    def _generate_results(self, final_cleaned_data: Dict[str, pd.DataFrame], 
                         duplicates_df: pd.DataFrame, changes_report: List[Dict],
                         rejected_report: List[Dict], duplicates_report: List[Dict], 
                         filename: str, extraction_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate final result package"""
        
        # Consolidate columns (remove empty/unnamed columns)
        consolidated_data = self.output_processor.consolidate_columns(final_cleaned_data)
        
        # Determine output format based on input
        if filename.endswith('.csv'):
            # For CSV, return single dataframe
            cleaned_data = list(consolidated_data.values())[0] if consolidated_data else pd.DataFrame()
        else:
            # For Excel, return dict of sheets
            cleaned_data = consolidated_data
        
        # Convert lists to DataFrames
        changes_df = pd.DataFrame(changes_report) if changes_report else pd.DataFrame()
        rejected_df = pd.DataFrame(rejected_report) if rejected_report else pd.DataFrame()
        
        # Combine rejected emails with duplicates for comprehensive rejected data
        if not rejected_df.empty and not duplicates_df.empty:
            # Add action column to duplicates for consistency
            duplicates_with_action = duplicates_df.copy()
            duplicates_with_action['action'] = 'duplicate'
            duplicates_with_action['reason'] = 'Duplicate email detected'
            
            # Combine both rejected datasets
            combined_rejected = pd.concat([rejected_df, duplicates_with_action], ignore_index=True)
        elif not rejected_df.empty:
            combined_rejected = rejected_df
        elif not duplicates_df.empty:
            duplicates_with_action = duplicates_df.copy()
            duplicates_with_action['action'] = 'duplicate'
            duplicates_with_action['reason'] = 'Duplicate email detected'
            combined_rejected = duplicates_with_action
        else:
            combined_rejected = pd.DataFrame()
        
        # Format duplicates report for readability
        duplicates_formatted_df = self._format_duplicates_report(duplicates_report)
        
        return {
            'cleaned_data': cleaned_data,
            'rejected_data': combined_rejected,  # Combined rejected and duplicate data
            'changes_report': changes_df,
            'duplicates_report': duplicates_formatted_df,
            'duplicates_file': duplicates_df,  # Separate duplicates file
            'summary': self.counters.copy(),
            'options': self.options,
            'arrays_by_sheet': extraction_results['arrays_by_sheet'],  # For UI display
            'email_columns_by_sheet': extraction_results['email_columns_by_sheet']
        }
    
    def _format_duplicates_report(self, duplicates_report: List[Dict]) -> pd.DataFrame:
        """Format duplicates report into a clear, flat DataFrame"""
        if not duplicates_report:
            return pd.DataFrame()
        
        formatted_rows = []
        for group in duplicates_report:
            canonical_key = group.get('canonical_key', 'Unknown')
            keeper = group.get('keeper', {})
            duplicates = group.get('duplicates', [])
            
            # Add the keeper row
            formatted_rows.append({
                'canonical_key': canonical_key,
                'status': 'KEPT',
                'sheet': keeper.get('sheet', ''),
                'row_index': keeper.get('row_number', ''),
                'email_address': keeper.get('email', ''),
                'duplicate_group_size': len(duplicates) + 1
            })
            
            # Add each duplicate row
            for dup in duplicates:
                formatted_rows.append({
                    'canonical_key': canonical_key,
                    'status': 'REMOVED',
                    'sheet': dup.get('sheet', ''),
                    'row_index': dup.get('row_number', ''),
                    'email_address': dup.get('email', ''),
                    'duplicate_group_size': len(duplicates) + 1
                })
        
        return pd.DataFrame(formatted_rows)
    
    def _update_progress(self, status: str, progress: float, callback: Callable):
        """Update progress and counters"""
        if callback:
            callback(status, progress, self.counters)
