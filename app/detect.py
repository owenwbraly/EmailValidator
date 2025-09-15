"""
Email column detection utilities
Finds email columns in DataFrames using header name matching
"""

import pandas as pd
from typing import Optional, List
import re


class EmailColumnDetector:
    def __init__(self):
        # Email column header patterns (case-insensitive)
        self.email_patterns = [
            r'^email$',
            r'^email[_\s]address$',
            r'^e[-_\s]?mail$',
            r'^work[_\s]email$',
            r'^business[_\s]email$',
            r'^contact[_\s]email$',
            r'^primary[_\s]email$',
            r'^email[_\s]1$'
        ]
    
    def detect_email_column(self, df: pd.DataFrame) -> Optional[str]:
        """
        Detect email column in DataFrame
        Returns column name if found, None otherwise
        Prefers exact 'email' match if multiple candidates exist
        """
        if df.empty:
            return None
        
        candidates = []
        
        # Check each column header against patterns
        for col in df.columns:
            col_clean = str(col).strip().lower()
            
            for pattern in self.email_patterns:
                if re.match(pattern, col_clean):
                    candidates.append((col, pattern))
                    break
        
        if not candidates:
            # Try content-based detection as fallback
            return self._detect_by_content(df)
        
        # Prefer exact 'email' match
        for col, pattern in candidates:
            if pattern == r'^email$':
                return col
        
        # Return first candidate
        return candidates[0][0]
    
    def _detect_by_content(self, df: pd.DataFrame) -> Optional[str]:
        """
        Fallback detection based on column content
        Looks for columns containing email-like strings
        """
        email_regex = re.compile(r'\S+@\S+\.\S+', re.IGNORECASE)
        
        for col in df.columns:
            if df[col].dtype == 'object':  # Only check text columns
                # Sample first 100 non-null values
                sample = df[col].dropna().head(100)
                
                if len(sample) == 0:
                    continue
                
                # Count email-like patterns
                email_count = sum(1 for val in sample if email_regex.search(str(val)))
                
                # If more than 50% look like emails, consider it an email column
                if email_count / len(sample) > 0.5:
                    return col
        
        return None
    
    def get_all_email_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Get all potential email columns in DataFrame
        Useful for datasets with multiple email fields
        """
        email_cols = []
        
        for col in df.columns:
            col_clean = str(col).strip().lower()
            
            for pattern in self.email_patterns:
                if re.match(pattern, col_clean):
                    email_cols.append(col)
                    break
        
        return email_cols
