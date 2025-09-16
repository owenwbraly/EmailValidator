"""
Email de-duplication utilities
Handles exact and near-duplicate detection with provider-aware canonicalization
"""

import pandas as pd
from typing import Dict, Any, List, Tuple, Set
from collections import defaultdict
import difflib
from .deterministic_email_engine import canonical_key


class EmailDeduplicator:
    def __init__(self, options: Dict[str, Any]):
        self.options = options
        self.provider_aware = options.get('provider_aware_dedup', True)
    
    def generate_canonical_key(self, email: str) -> str:
        """
        Generate canonical key for de-duplication
        Uses new deterministic engine's canonical key generation
        """
        return canonical_key(email, provider_aware=self.provider_aware) or email.lower()
    
    def deduplicate_records(self, processed_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Perform de-duplication on processed email records
        Returns dict with duplicates_report and canonical_mapping
        """
        # Group by canonical key
        canonical_groups = defaultdict(list)
        
        for row in processed_rows:
            if row['action'] != 'remove':  # Only consider non-removed emails
                canonical_key = row.get('canonical_key')
                if not canonical_key:
                    canonical_key = self.generate_canonical_key(row['processed_email'])
                    row['canonical_key'] = canonical_key
                
                canonical_groups[canonical_key].append(row)
        
        # Find duplicates and build report
        duplicates_report = []
        canonical_mapping = {}
        
        for canonical_key, group in canonical_groups.items():
            if len(group) > 1:
                # Sort to get consistent "keeper" selection
                sorted_group = sorted(group, key=lambda x: (x['sheet'], x['row_index']))
                keeper = sorted_group[0]
                duplicates = sorted_group[1:]
                
                # Record duplicate group
                duplicate_entry = {
                    'canonical_key': canonical_key,
                    'keeper': {
                        'sheet': keeper['sheet'],
                        'row_index': keeper['row_index'],
                        'email': keeper['processed_email']
                    },
                    'duplicates': [{
                        'sheet': dup['sheet'],
                        'row_index': dup['row_index'],
                        'email': dup['processed_email']
                    } for dup in duplicates],
                    'duplicate_count': len(duplicates)
                }
                
                duplicates_report.append(duplicate_entry)
                
                # Map canonical key to keeper
                canonical_mapping[canonical_key] = keeper
        
        # Find near-duplicates
        near_duplicates = self._find_near_duplicates(processed_rows)
        
        return {
            'duplicates_report': duplicates_report,
            'near_duplicates': near_duplicates,
            'canonical_mapping': canonical_mapping
        }
    
    def _find_near_duplicates(self, processed_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Find near-duplicate emails using edit distance
        Returns list of near-duplicate groups
        
        For large datasets (>1000 emails), this is disabled for performance
        """
        near_duplicates = []
        emails = [(row['processed_email'], row) for row in processed_rows if row['action'] != 'remove']
        
        # Skip near-duplicate analysis for large datasets to prevent hanging
        if len(emails) > 1000:
            return near_duplicates
        
        # Compare each pair of emails
        for i, (email1, row1) in enumerate(emails):
            for j, (email2, row2) in enumerate(emails[i+1:], i+1):
                if self._are_near_duplicates(email1, email2):
                    near_duplicates.append({
                        'email1': email1,
                        'email2': email2,
                        'row1': {'sheet': row1['sheet'], 'row_index': row1['row_index']},
                        'row2': {'sheet': row2['sheet'], 'row_index': row2['row_index']},
                        'similarity': self._calculate_similarity(email1, email2)
                    })
        
        return near_duplicates
    
    def _are_near_duplicates(self, email1: str, email2: str) -> bool:
        """
        Check if two emails are near-duplicates
        Uses edit distance on domain and local parts
        """
        if '@' not in email1 or '@' not in email2:
            return False
        
        local1, domain1 = email1.rsplit('@', 1)
        local2, domain2 = email2.rsplit('@', 1)
        
        # Check domain similarity
        domain_distance = self._edit_distance(domain1.lower(), domain2.lower())
        if domain_distance <= 2 and abs(len(domain1) - len(domain2)) <= 2:
            return True
        
        # Check local part similarity (only if domains are same or very similar)
        if domain1.lower() == domain2.lower():
            local_distance = self._edit_distance(local1.lower(), local2.lower())
            if local_distance <= 2 and abs(len(local1) - len(local2)) <= 2:
                return True
        
        return False
    
    def _edit_distance(self, s1: str, s2: str) -> int:
        """Calculate edit distance between two strings"""
        if len(s1) < len(s2):
            return self._edit_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    def _calculate_similarity(self, email1: str, email2: str) -> float:
        """Calculate similarity score between two emails"""
        return difflib.SequenceMatcher(None, email1.lower(), email2.lower()).ratio()
    
    def remove_duplicates_from_dataset(self, df: pd.DataFrame, email_col: str, 
                                     duplicates_report: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, List[int]]:
        """
        Remove duplicate rows from DataFrame based on duplicates report
        Returns cleaned DataFrame and list of removed row indices
        """
        rows_to_remove = set()
        
        # Collect all duplicate row indices (keeping first occurrence)
        for duplicate_group in duplicates_report:
            if 'duplicates' in duplicate_group:
                for dup in duplicate_group['duplicates']:
                    rows_to_remove.add(dup['row_index'])
        
        # Remove duplicate rows
        if rows_to_remove:
            cleaned_df = df.drop(list(rows_to_remove), errors='ignore')
            return cleaned_df, list(rows_to_remove)
        
        return df, []
