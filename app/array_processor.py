"""
Array-based email processing with deterministic cleaning
Processes arrays of EmailEntry objects efficiently
"""

from typing import List, Dict, Any
from .email_entry import EmailEntry
from .deterministic_email_engine import validate_email_deterministic
from .decision_engine import DeterministicDecisionEngine


class ArrayEmailProcessor:
    """
    Processes arrays of EmailEntry objects using deterministic engine
    """
    
    def __init__(self, decision_engine: DeterministicDecisionEngine):
        self.decision_engine = decision_engine
    
    def process_email_entries(self, entries: List[EmailEntry]) -> Dict[str, Any]:
        """
        Process all email entries using deterministic engine
        Updates entries in-place with cleaned values and metadata
        """
        results = {
            'accepted': 0,
            'fixed': 0,
            'removed': 0,
            'total_processed': 0
        }
        
        changes_report = []
        
        for entry in entries:
            if entry.is_empty():
                continue
                
            # Process with deterministic engine
            decision = self.decision_engine.process_email(entry.raw)
            
            # Update entry with results
            entry.cleaned = decision['output_email']
            entry.action = decision['action']
            entry.confidence = decision['confidence']
            entry.changed = decision['changed']
            entry.reason = decision['reason']
            entry.canonical_key = self.decision_engine.get_canonical_key(entry.cleaned)
            
            # Update counters
            if decision['action'] in ['accept', 'fix_auto']:
                if decision['changed']:
                    results['fixed'] += 1
                else:
                    results['accepted'] += 1
            elif decision['action'] == 'remove':
                results['removed'] += 1
            
            results['total_processed'] += 1
            
            # Record changes
            if decision['changed']:
                changes_report.append({
                    'sheet': entry.sheet,
                    'row_number': entry.row_number,
                    'col_number': entry.col_number,
                    'col_name': entry.col_name,
                    'original_email': entry.raw,
                    'new_email': entry.cleaned,
                    'reason': entry.reason,
                    'confidence': entry.confidence
                })
        
        return {
            'results': results,
            'changes_report': changes_report
        }
    
    def update_dataframes_with_cleaned_emails(self, file_data: Dict[str, Any], 
                                            entries: List[EmailEntry]) -> Dict[str, Any]:
        """
        Update original DataFrames with cleaned email values
        Only updates non-duplicate entries
        """
        updated_data = {}
        
        for sheet_name, df in file_data.items():
            df_reset = df.reset_index(drop=True)
            updated_df = df_reset.copy()
            
            # Find entries for this sheet
            sheet_entries = [e for e in entries if e.sheet == sheet_name and not e.is_duplicate()]
            
            for entry in sheet_entries:
                if not entry.is_empty() and entry.action != 'remove':
                    # Update with cleaned email
                    row_idx = entry.row_number - 1  # Convert to 0-based
                    if 0 <= row_idx < len(updated_df):
                        updated_df.iloc[row_idx, updated_df.columns.get_loc(entry.col_name)] = entry.cleaned
                elif entry.action == 'remove':
                    # Remove the email but keep the row
                    row_idx = entry.row_number - 1  # Convert to 0-based
                    if 0 <= row_idx < len(updated_df):
                        updated_df.iloc[row_idx, updated_df.columns.get_loc(entry.col_name)] = ""
            
            updated_data[sheet_name] = updated_df
        
        return updated_data