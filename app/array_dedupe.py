"""
Array-based email deduplication with position tracking
Simplified approach working with EmailEntry objects
"""

from typing import Dict, List, Any, Tuple, Set
from collections import defaultdict
import pandas as pd
from .email_entry import EmailEntry
from .deterministic_email_engine import canonical_key


class ArrayEmailDeduplicator:
    """
    Array-based email deduplication using EmailEntry objects
    """
    
    def __init__(self, options: Dict[str, Any]):
        self.options = options
        self.provider_aware = options.get('provider_aware_dedup', True)
    
    def deduplicate_entries(self, entries: List[EmailEntry]) -> Dict[str, Any]:
        """
        Perform deduplication on EmailEntry objects
        Returns duplicate report and marks duplicate entries
        """
        # Group by canonical key
        canonical_groups = defaultdict(list)
        
        # Only process non-empty entries that aren't already removed
        valid_entries = [e for e in entries if not e.is_empty() and e.action != 'remove']
        
        for entry in valid_entries:
            # Generate canonical key if not already set
            if not entry.canonical_key and entry.cleaned:
                entry.canonical_key = canonical_key(entry.cleaned, provider_aware=self.provider_aware) or entry.cleaned.lower()
            elif not entry.canonical_key and entry.raw:
                entry.canonical_key = canonical_key(entry.raw, provider_aware=self.provider_aware) or entry.raw.lower()
            
            if entry.canonical_key:
                canonical_groups[entry.canonical_key].append(entry)
        
        # Find duplicates and mark them
        duplicates_report = []
        duplicate_positions = set()
        
        for canonical_key_val, group in canonical_groups.items():
            if len(group) > 1:
                # Sort by sheet, row_number, col_number to get consistent keeper
                sorted_group = sorted(group, key=lambda x: (x.sheet, x.row_number, x.col_number))
                keeper = sorted_group[0]
                duplicates = sorted_group[1:]
                
                # Mark duplicates
                for dup in duplicates:
                    dup.action = "duplicate"
                    duplicate_positions.add((dup.sheet, dup.row_number))
                
                # Build duplicate report entry
                duplicate_entry = {
                    'canonical_key': canonical_key_val,
                    'keeper': {
                        'sheet': keeper.sheet,
                        'row_number': keeper.row_number,
                        'col_number': keeper.col_number,
                        'col_name': keeper.col_name,
                        'email': keeper.cleaned or keeper.raw
                    },
                    'duplicates': [{
                        'sheet': dup.sheet,
                        'row_number': dup.row_number,
                        'col_number': dup.col_number,
                        'col_name': dup.col_name,
                        'email': dup.cleaned or dup.raw
                    } for dup in duplicates],
                    'duplicate_count': len(duplicates)
                }
                
                duplicates_report.append(duplicate_entry)
        
        return {
            'duplicates_report': duplicates_report,
            'duplicate_positions': duplicate_positions,  # Set of (sheet, row_number) tuples
            'total_duplicates_removed': sum(len(group['duplicates']) for group in duplicates_report)
        }
    
    def create_duplicates_dataframe(self, file_data: Dict[str, pd.DataFrame], 
                                   duplicate_positions: Set[Tuple[str, int]]) -> pd.DataFrame:
        """
        Create a DataFrame containing all duplicate rows with metadata
        """
        duplicate_rows = []
        
        for sheet_name, df in file_data.items():
            df_reset = df.reset_index(drop=True)
            
            for row_idx in range(len(df_reset)):
                row_number = row_idx + 1  # 1-based
                
                if (sheet_name, row_number) in duplicate_positions:
                    # This row has duplicates, copy it
                    row_data = df_reset.iloc[row_idx].to_dict()
                    row_data['__duplicate_metadata__'] = {
                        'original_sheet': sheet_name,
                        'original_row_number': row_number,
                        'duplicate_reason': 'Email duplicate detected'
                    }
                    
                    # Add metadata columns
                    row_data['_Source_Sheet'] = sheet_name
                    row_data['_Source_Row'] = row_number
                    row_data['_Duplicate_Reason'] = 'Email duplicate detected'
                    
                    duplicate_rows.append(row_data)
        
        return pd.DataFrame(duplicate_rows) if duplicate_rows else pd.DataFrame()
    
    def blank_duplicate_rows(self, file_data: Dict[str, pd.DataFrame], 
                           duplicate_positions: Set[Tuple[str, int]]) -> Dict[str, pd.DataFrame]:
        """
        Blank all columns in rows that contain duplicates while preserving row spacing
        """
        cleaned_data = {}
        
        for sheet_name, df in file_data.items():
            df_reset = df.reset_index(drop=True)
            cleaned_df = df_reset.copy()
            
            for row_idx in range(len(df_reset)):
                row_number = row_idx + 1  # 1-based
                
                if (sheet_name, row_number) in duplicate_positions:
                    # Blank all columns in this row
                    for col in cleaned_df.columns:
                        cleaned_df.iloc[row_idx, cleaned_df.columns.get_loc(col)] = ""
            
            cleaned_data[sheet_name] = cleaned_df
        
        return cleaned_data