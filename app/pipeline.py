"""
Email validation pipeline orchestrator
Coordinates all processing steps from file loading to output generation
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Callable, List, Tuple, Optional, Union
import time
from io import BytesIO

from .io_utils import FileHandler
from .detect import EmailColumnDetector
from .features import FeatureExtractor
from .normalize import EmailNormalizer
from .llm_adapter import LLMAdapter
from .routing import DecisionRouter
from .dedupe import EmailDeduplicator


class EmailValidationPipeline:
    def __init__(self, llm_config: Dict[str, Any], options: Dict[str, Any]):
        self.llm_config = llm_config
        self.options = options
        
        # Initialize components
        self.file_handler = FileHandler()
        self.detector = EmailColumnDetector()
        self.feature_extractor = FeatureExtractor()
        self.normalizer = EmailNormalizer()
        self.llm_adapter = LLMAdapter(llm_config)
        self.router = DecisionRouter(options)
        self.deduplicator = EmailDeduplicator(options)
        
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
        Main processing pipeline
        Returns dict with cleaned_data, rejected_data, changes_report, duplicates_report, summary
        """
        self._update_progress("Loading file...", 0.05, progress_callback)
        
        # Step 1: Load file
        file_data = self.file_handler.load_file(uploaded_file)
        
        self._update_progress("Detecting email columns...", 0.10, progress_callback)
        
        # Step 2: Detect email columns per sheet
        email_columns = {}
        for sheet_name, df in file_data.items():
            email_col = self.detector.detect_email_column(df)
            if email_col:
                email_columns[sheet_name] = email_col
        
        if not email_columns:
            raise ValueError("No email columns found in the uploaded file")
        
        # Initialize result containers
        all_results = []
        changes_report = []
        rejected_rows = []
        processed_sheets = {}
        
        total_rows = sum(len(df) for sheet, df in file_data.items() if sheet in email_columns)
        processed_rows = 0
        
        # Step 3-6: Process each sheet with email columns
        for sheet_name, df in file_data.items():
            if sheet_name not in email_columns:
                # Pass through sheets without email columns unchanged
                processed_sheets[sheet_name] = df
                continue
            
            email_col = email_columns[sheet_name]
            sheet_results = self._process_sheet(
                df, sheet_name, email_col, processed_rows, total_rows, progress_callback
            )
            
            all_results.extend(sheet_results['processed_rows'])
            changes_report.extend(sheet_results['changes'])
            rejected_rows.extend(sheet_results['rejected'])
            processed_sheets[sheet_name] = sheet_results['cleaned_df']
            
            processed_rows += len(df)
        
        self._update_progress("De-duplicating records...", 0.90, progress_callback)
        
        # Step 7: De-duplication across all processed data
        dedupe_results = self.deduplicator.deduplicate_records(all_results)
        
        # Update counters
        self.counters['duplicates'] = len(dedupe_results['duplicates_report'])
        
        # Step 8: Apply de-duplication results back to sheets
        final_sheets = self._apply_deduplication(processed_sheets, dedupe_results, email_columns)
        
        self._update_progress("Generating reports...", 0.95, progress_callback)
        
        # Step 9: Generate final reports
        results = self._generate_final_results(
            final_sheets, rejected_rows, changes_report, 
            dedupe_results['duplicates_report'], uploaded_file.name
        )
        
        self._update_progress("Complete!", 1.0, progress_callback)
        
        return results
    
    def _process_sheet(self, df: pd.DataFrame, sheet_name: str, email_col: str, 
                      base_processed: int, total_rows: int, progress_callback: Callable) -> Dict[str, Any]:
        """Process a single sheet with email column"""
        processed_rows = []
        changes = []
        rejected = []
        
        # Create a copy of the dataframe to modify
        cleaned_df = df.copy()
        
        for idx, row in df.iterrows():
            email = str(row[email_col]).strip()
            
            if pd.isna(email) or email in ['', 'nan', 'None']:
                # Skip empty emails but keep the row
                processed_rows.append({
                    'sheet': sheet_name,
                    'row_index': idx,
                    'original_email': email,
                    'processed_email': email,
                    'action': 'skip_empty',
                    'confidence': 1.0
                })
                continue
            
            # Step 3: Extract deterministic features
            features = self.feature_extractor.extract_features(email)
            
            # Step 4: Normalize email
            normalized = self.normalizer.normalize_email(email, features)
            
            # Step 5: LLM classification
            llm_result = self.llm_adapter.classify_email(email, features)
            
            # Step 6: Make routing decision
            decision = self.router.decide(email, normalized, llm_result, features)
            
            # Track results
            processed_rows.append({
                'sheet': sheet_name,
                'row_index': idx,
                'original_email': email,
                'processed_email': decision['output_email'],
                'action': decision['action'],
                'confidence': decision['confidence'],
                'canonical_key': self.deduplicator.generate_canonical_key(decision['output_email'])
            })
            
            # Update counters
            if decision['action'] == 'accept':
                self.counters['accepted'] += 1
            elif decision['action'] == 'fix':
                self.counters['fixed'] += 1
            elif decision['action'] == 'remove':
                self.counters['removed'] += 1
            
            # Record changes
            if decision['changed']:
                changes.append({
                    'sheet': sheet_name,
                    'row_index': idx,
                    'original_email': email,
                    'new_email': decision['output_email'],
                    'reason': decision['reason'],
                    'confidence': decision['confidence']
                })
            
            # Handle removed rows
            if decision['action'] == 'remove':
                rejected.append({
                    'sheet': sheet_name,
                    'row_index': idx,
                    'original_email': email,
                    'reason': decision['reason'],
                    'confidence': decision['confidence'],
                    **{col: row[col] for col in df.columns}  # Include all original columns
                })
                # Remove from cleaned dataframe
                cleaned_df = cleaned_df.drop(idx)
            else:
                # Update email in cleaned dataframe
                cleaned_df.at[idx, email_col] = decision['output_email']
            
            # Update progress
            current_progress = 0.10 + 0.75 * (base_processed + idx + 1) / total_rows
            self._update_progress(f"Processing {sheet_name}...", current_progress, progress_callback)
        
        return {
            'processed_rows': processed_rows,
            'changes': changes,
            'rejected': rejected,
            'cleaned_df': cleaned_df
        }
    
    def _apply_deduplication(self, processed_sheets: Dict[str, pd.DataFrame], 
                           dedupe_results: Dict[str, Any], 
                           email_columns: Dict[str, str]) -> Dict[str, pd.DataFrame]:
        """Apply de-duplication results to remove duplicate rows from sheets"""
        duplicates_to_remove = set()
        
        # Build set of (sheet, row_index) tuples to remove
        for duplicate_group in dedupe_results['duplicates_report']:
            if isinstance(duplicate_group, dict) and 'duplicates' in duplicate_group:
                for dup in duplicate_group['duplicates'][1:]:  # Keep first, remove rest
                    duplicates_to_remove.add((dup['sheet'], dup['row_index']))
        
        # Remove duplicates from each sheet
        final_sheets = {}
        for sheet_name, df in processed_sheets.items():
            if sheet_name in email_columns:
                # Remove rows marked as duplicates
                rows_to_remove = [idx for sheet, idx in duplicates_to_remove if sheet == sheet_name]
                if rows_to_remove:
                    df_cleaned = df.drop(rows_to_remove, errors='ignore')
                    final_sheets[sheet_name] = df_cleaned
                    # Update duplicate counter
                    self.counters['duplicates'] += len(rows_to_remove)
                else:
                    final_sheets[sheet_name] = df
            else:
                final_sheets[sheet_name] = df
        
        return final_sheets
    
    def _generate_final_results(self, final_sheets: Dict[str, pd.DataFrame], 
                              rejected_rows: List[Dict], changes: List[Dict],
                              duplicates_report: List[Dict], filename: str) -> Dict[str, Any]:
        """Generate final result package"""
        
        # Determine output format based on input
        if filename.endswith('.csv'):
            # For CSV, return single dataframe
            cleaned_data = list(final_sheets.values())[0] if final_sheets else pd.DataFrame()
        else:
            # For Excel, return dict of sheets
            cleaned_data = final_sheets
        
        # Convert lists to DataFrames
        rejected_df = pd.DataFrame(rejected_rows) if rejected_rows else pd.DataFrame()
        changes_df = pd.DataFrame(changes) if changes else pd.DataFrame()
        duplicates_df = pd.DataFrame(duplicates_report) if duplicates_report else pd.DataFrame()
        
        return {
            'cleaned_data': cleaned_data,
            'rejected_data': rejected_df,
            'changes_report': changes_df,
            'duplicates_report': duplicates_df,
            'summary': self.counters.copy(),
            'options': self.options
        }
    
    def _update_progress(self, status: str, progress: float, callback: Callable):
        """Update progress and counters"""
        if callback:
            callback(status, progress, self.counters)
