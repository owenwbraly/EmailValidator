"""
Simple semantic column detector for person data extraction.
Only looks at column headers - no content analysis.
"""

import pandas as pd
from typing import Dict, List, Optional


class SemanticColumnDetector:
    """
    Simple column detector that only looks at column headers.
    Assumes standard naming conventions: First, Last, Title, Company, etc.
    """
    
    def __init__(self):
        pass
    
    def detect_first_name_column(self, df: pd.DataFrame) -> Optional[str]:
        """Detect first name column - looks for 'First' or 'first'"""
        for col in df.columns:
            col_str = str(col).strip()
            if col_str.lower() == 'first':
                return col
        return None
    
    def detect_last_name_column(self, df: pd.DataFrame) -> Optional[str]:
        """Detect last name column - looks for 'Last' or 'last'"""
        for col in df.columns:
            col_str = str(col).strip()
            if col_str.lower() == 'last':
                return col
        return None
    
    def detect_title_column(self, df: pd.DataFrame) -> Optional[str]:
        """Detect title column - looks for 'Title' or 'title'"""
        for col in df.columns:
            col_str = str(col).strip()
            if col_str.lower() == 'title':
                return col
        return None
    
    def detect_company_column(self, df: pd.DataFrame) -> Optional[str]:
        """Detect company column - looks for 'Company' or 'company'"""
        for col in df.columns:
            col_str = str(col).strip()
            if col_str.lower() == 'company':
                return col
        return None
    
    def detect_company_domain_column(self, df: pd.DataFrame) -> Optional[str]:
        """Detect company domain column - looks for 'Company Domain' or 'company domain'"""
        for col in df.columns:
            col_str = str(col).strip()
            if col_str.lower() in ['company domain', 'companydomain']:
                return col
        return None
    
    def detect_email_column(self, df: pd.DataFrame) -> Optional[str]:
        """Detect email column - looks for 'Email' or 'email'"""
        for col in df.columns:
            col_str = str(col).strip()
            if col_str.lower() == 'email':
                return col
        return None
    
    def detect_linkedin_column(self, df: pd.DataFrame) -> Optional[str]:
        """Detect LinkedIn column - looks for 'LinkedIn' or 'linkedin'"""
        for col in df.columns:
            col_str = str(col).strip()
            if col_str.lower() in ['linkedin', 'linkedin profile']:
                return col
        return None
    
    def is_person_field(self, column_name: str) -> Optional[str]:
        """
        Check if a column is a person field and return the field type.
        Returns None if not a person field.
        Uses substring matching to handle pandas renamed columns (e.g., First.1)
        """
        col_str = str(column_name).strip().lower()
        
        if 'first' in col_str:
            return 'first_name'
        elif 'last' in col_str:
            return 'last_name'
        elif col_str == 'title' or col_str.startswith('title.'):
            return 'title'
        elif 'company' in col_str and 'domain' not in col_str:
            return 'company'
        elif 'company domain' in col_str or 'companydomain' in col_str:
            return 'company_domain'
        elif 'email' in col_str:
            return 'email'
        elif 'linkedin' in col_str:
            return 'linkedin'
        
        return None
    
    # Legacy methods for compatibility
    def detect_single_field(self, df: pd.DataFrame, field_type: str) -> Optional[str]:
        """Detect a single field type"""
        if field_type == 'first_name':
            return self.detect_first_name_column(df)
        elif field_type == 'last_name':
            return self.detect_last_name_column(df)
        elif field_type == 'title':
            return self.detect_title_column(df)
        elif field_type == 'company':
            return self.detect_company_column(df)
        elif field_type == 'company_domain':
            return self.detect_company_domain_column(df)
        elif field_type == 'email':
            return self.detect_email_column(df)
        elif field_type == 'linkedin':
            return self.detect_linkedin_column(df)
        return None
    
    def detect_email_columns(self, df: pd.DataFrame) -> List[str]:
        """Get all email columns"""
        email_col = self.detect_email_column(df)
        return [email_col] if email_col else []
    
    def get_all_email_columns(self, df: pd.DataFrame) -> List[str]:
        """Get all email columns (alias for detect_email_columns)"""
        return self.detect_email_columns(df)
    
    def detect_best_email_column(self, df: pd.DataFrame) -> Optional[str]:
        """Detect the best email column"""
        return self.detect_email_column(df)