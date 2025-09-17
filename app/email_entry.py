"""
Email entry data model for array-based email processing
Tracks emails with position metadata for simplified processing
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any
import pandas as pd


@dataclass
class EmailEntry:
    """
    Single email entry with position and processing metadata
    """
    sheet: str
    row_number: int  # 1-based row number
    col_number: int  # 1-based column number  
    col_name: str
    raw: str
    cleaned: str = ""
    canonical_key: str = ""
    action: str = ""
    confidence: float = 0.0
    changed: bool = False
    reason: str = ""
    
    def is_empty(self) -> bool:
        """Check if this entry represents an empty cell"""
        return self.raw == "" or self.raw in ['nan', 'None']
    
    def is_duplicate(self) -> bool:
        """Check if this entry is marked as a duplicate"""
        return self.action == "duplicate"


class EmailArrayExtractor:
    """
    Extracts emails from DataFrames as arrays with position tracking
    """
    
    def __init__(self, detector):
        self.detector = detector
    
    def extract_all_emails(self, file_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Extract all emails from all sheets as arrays with position tracking
        Returns dict with email_entries, arrays_by_sheet, and metadata
        """
        all_entries = []
        arrays_by_sheet = {}
        email_columns_by_sheet = {}
        
        for sheet_name, df in file_data.items():
            # Reset index to ensure consistent row numbering
            df_reset = df.reset_index(drop=True)
            
            # Find all email columns in this sheet
            email_columns = self.detector.get_all_email_columns(df_reset)
            
            if not email_columns:
                # No email columns in this sheet, skip
                arrays_by_sheet[sheet_name] = []
                continue
                
            email_columns_by_sheet[sheet_name] = email_columns
            sheet_arrays = []
            
            for email_col in email_columns:
                # Get column position - use simple list approach to avoid get_loc complexity
                try:
                    col_number = list(df_reset.columns).index(email_col) + 1  # 1-based
                except ValueError:
                    continue  # Column not found, skip
                
                # Extract email array for this column, preserving row spacing
                email_array = []
                
                for row_idx in range(len(df_reset)):
                    row_number = row_idx + 1  # 1-based
                    raw_value = str(df_reset.iloc[row_idx][email_col]).strip()
                    
                    # Handle empty/null values
                    if raw_value in ['nan', 'None', ''] or pd.isna(df_reset.iloc[row_idx][email_col]):
                        raw_value = ""
                    
                    entry = EmailEntry(
                        sheet=sheet_name,
                        row_number=row_number,
                        col_number=col_number,
                        col_name=email_col,
                        raw=raw_value
                    )
                    
                    email_array.append(raw_value)
                    all_entries.append(entry)
                
                sheet_arrays.append({
                    'column_name': email_col,
                    'column_number': col_number,
                    'emails': email_array
                })
            
            arrays_by_sheet[sheet_name] = sheet_arrays
        
        return {
            'email_entries': all_entries,
            'arrays_by_sheet': arrays_by_sheet,
            'email_columns_by_sheet': email_columns_by_sheet,
            'total_entries': len(all_entries),
            'non_empty_entries': len([e for e in all_entries if not e.is_empty()])
        }
    
    def flatten_entries_for_processing(self, all_entries: List[EmailEntry]) -> List[EmailEntry]:
        """
        Return only non-empty entries for processing while maintaining position tracking
        """
        return [entry for entry in all_entries if not entry.is_empty()]