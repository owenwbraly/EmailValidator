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
from utils.semantic_col_detector import SemanticColumnDetector
from utils.output_processor import OutputProcessor
from models.EmailEntry import EmailEntry, EmailArrayExtractor
from models.Person import Person, PersonExtractor
from .email_processing.person_email_processor import PersonEmailProcessor


class EmailValidationPipeline:
    def __init__(self, options: Dict[str, Any]):
        self.options = options
        
        # Initialize components
        self.file_handler = FileHandler()
        self.detector = SemanticColumnDetector()
        self.array_extractor = EmailArrayExtractor(self.detector)
        self.person_extractor = PersonExtractor()
        self.person_email_processor = PersonEmailProcessor(options)
        self.output_processor = OutputProcessor()
        
        # Progress tracking
        self.counters = {
            'accepted': 0,
            'fixed': 0,
            'removed': 0,
            'duplicates': 0,
            'total_processed': 0,
            'people_extracted': 0
        }
    
    def process_file(self, uploaded_file, progress_callback: Callable = None) -> Dict[str, Any]:
        """
        Main processing pipeline - now focused on person extraction
        Returns dict with people_data, summary
        """
        self._update_progress("Loading file...", 0.1, progress_callback)
        
        # Step 1: Load file
        file_data = self.file_handler.load_file(uploaded_file)
        
        self._update_progress("Extracting people from all sheets...", 0.5, progress_callback)
        
        # Step 2: Extract all people from all sheets
        all_people = self.person_extractor.extract_all_people(file_data)
        
        if not all_people:
            raise ValueError("No people found in the uploaded file")
        
        # Update counter
        self.counters['people_extracted'] = len(all_people)
        
        self._update_progress("Processing emails and validating...", 0.6, progress_callback)
        
        # Step 3: Process emails and categorize people
        accepted_people, rejected_people, email_summary = self.person_email_processor.process_people_emails(all_people)
        
        self._update_progress("Generating 3-sheet CSV...", 0.9, progress_callback)
        
        # Step 4: Generate 3-sheet CSV
        all_people_df = self._create_people_dataframe(all_people)
        accepted_people_df = self._create_people_dataframe(accepted_people)
        rejected_people_df = self._create_rejected_people_dataframe(rejected_people)
        
        self._update_progress("Complete!", 1.0, progress_callback)
        
        return {
            'all_people_data': all_people_df,
            'accepted_people_data': accepted_people_df,
            'rejected_people_data': rejected_people_df,
            'all_people_list': all_people,
            'accepted_people_list': accepted_people,
            'rejected_people_list': rejected_people,
            'summary': {
                'people_extracted': len(all_people),
                'people_accepted': len(accepted_people),
                'people_rejected': len(rejected_people),
                'sheets_processed': len(file_data),
                'email_processing': email_summary
            },
            'options': self.options
        }
    
    def _create_people_dataframe(self, people: List[Person]) -> pd.DataFrame:
        """Create DataFrame from list of Person objects"""
        if not people:
            return pd.DataFrame()
        
        # Convert people to dictionaries
        people_data = [person.to_dict() for person in people]
        
        # Create DataFrame
        df = pd.DataFrame(people_data)
        
        # Reorder columns for better readability
        column_order = [
            'First', 'Last', 'Title', 'Company', 'Company Domain', 
            'Email', 'LinkedIn', '', 'Source Sheet', 'Source Row', 'Person Group'
        ]
        
        # Only include columns that exist
        available_columns = [col for col in column_order if col in df.columns]
        df = df[available_columns]
        
        return df
    
    def _create_rejected_people_dataframe(self, people: List[Person]) -> pd.DataFrame:
        """Create DataFrame from list of rejected Person objects with validation info"""
        if not people:
            return pd.DataFrame()
        
        # Sort rejected people by source sheet and row number for better organization
        sorted_people = sorted(people, key=lambda p: (p.source_sheet, p.source_row, p.source_column_group))
        
        # Convert people to dictionaries with validation info
        people_data = [person.to_dict_with_validation() for person in sorted_people]
        
        # Create DataFrame
        df = pd.DataFrame(people_data)
        
        # Reorder columns for better readability
        column_order = [
            'First', 'Last', 'Title', 'Company', 'Company Domain', 
            'Email', 'LinkedIn', '', 'Source Sheet', 'Source Row', 'Person Group',
            'Email Action', 'Rejection Reason', 'Email Confidence'
        ]
        
        # Only include columns that exist
        available_columns = [col for col in column_order if col in df.columns]
        df = df[available_columns]
        
        return df
    
    def create_3_sheet_excel(self, all_people_df: pd.DataFrame, 
                            accepted_people_df: pd.DataFrame, 
                            rejected_people_df: pd.DataFrame) -> bytes:
        """Create a 3-sheet Excel file with all people, accepted people, and rejected people"""
        from io import BytesIO
        
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Sheet 1: All People
            all_people_df.to_excel(writer, sheet_name='All People', index=False)
            
            # Sheet 2: Accepted People (Cleaned & Deduplicated)
            accepted_people_df.to_excel(writer, sheet_name='Cleaned People', index=False)
            
            # Sheet 3: Rejected People
            rejected_people_df.to_excel(writer, sheet_name='Rejected People', index=False)
        
        output.seek(0)
        return output.getvalue()
    
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
