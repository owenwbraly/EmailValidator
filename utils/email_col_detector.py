"""
Email column detection utilities
Finds email columns in DataFrames using header name matching
"""

import pandas as pd
from typing import Optional, List
import re


class EmailColumnDetector:
    def __init__(self):
        # Safe email column header patterns (case-insensitive) - exact matches only to avoid false positives
        self.email_patterns = [
            r'^email$',
            r'^email[_\s]address$',
            r'^e[-_\s]?mail$',
            r'^work[_\s]email$',
            r'^business[_\s]email$',
            r'^contact[_\s]email$',
            r'^primary[_\s]email$',
            r'^email[_\s]1$',
            r'^email[_\s]2$',
            r'^personal[_\s]email$',
            r'^home[_\s]email$',
            r'^office[_\s]email$',
            r'^customer[_\s]email$',
            r'^client[_\s]email$',
            r'^user[_\s]email$',
            r'^employee[_\s]email$',
            r'^member[_\s]email$',
            r'^subscriber[_\s]email$',
            r'^emailaddress$',
            r'^email_addr$',
            r'^electronic[_\s]mail$'
        ]
    
    def detect_email_column(self, df: pd.DataFrame) -> Optional[str]:
        """
        Detect email column in DataFrame
        Returns column name if found, None otherwise
        Uses priority-based matching for best results
        """
        if df.empty:
            return None
        
        candidates = []
        
        # Check each column header against patterns with priority scoring
        for col in df.columns:
            col_clean = str(col).strip().lower()
            
            for priority, pattern in enumerate(self.email_patterns):
                if re.match(pattern, col_clean):
                    candidates.append((col, pattern, priority))
                    break
        
        if candidates:
            # Sort by priority (lower number = higher priority)
            candidates.sort(key=lambda x: x[2])
            return candidates[0][0]
        
        # Try content-based detection as fallback
        return self._detect_by_content(df)
    
    def _detect_by_content(self, df: pd.DataFrame) -> Optional[str]:
        """
        Fallback detection based on column content
        Looks for columns containing email-like strings with enhanced validation
        """
        # More sophisticated email pattern
        email_regex = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        
        candidates = []
        
        for col in df.columns:
            if df[col].dtype == 'object':  # Only check text columns
                # Sample more values for better accuracy
                sample = df[col].dropna().head(200)
                
                if len(sample) == 0:
                    continue
                
                # Count valid email-like patterns
                email_count = sum(1 for val in sample if email_regex.search(str(val)))
                
                if len(sample) > 0:
                    email_ratio = email_count / len(sample)
                    
                    # Lower threshold but require at least some emails
                    if email_ratio >= 0.3 and email_count >= min(3, len(sample)):
                        candidates.append((col, email_ratio, email_count))
        
        if candidates:
            # Sort by email ratio (highest first) then by email count
            candidates.sort(key=lambda x: (x[1], x[2]), reverse=True)
            return candidates[0][0]
        
        return None
    
    def get_all_email_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Get all potential email columns in DataFrame
        Useful for datasets with multiple email fields
        Combines header-based and content-based detection
        """
        email_cols = []
        
        # Header-based detection
        for col in df.columns:
            col_clean = str(col).strip().lower()
            
            for pattern in self.email_patterns:
                if re.match(pattern, col_clean):
                    email_cols.append(col)
                    break
        
        # Content-based detection for missed columns
        email_regex = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        
        for col in df.columns:
            if col not in email_cols and df[col].dtype == 'object':
                sample = df[col].dropna().head(50)
                if len(sample) > 0:
                    email_count = sum(1 for val in sample if email_regex.search(str(val)))
                    email_ratio = email_count / len(sample)
                    
                    # More lenient for multi-column detection
                    if email_ratio >= 0.2 and email_count >= 2:
                        email_cols.append(col)
        
        return email_cols
    
    def detect_best_email_column(self, df: pd.DataFrame) -> Optional[str]:
        """
        Advanced email column detection that considers both headers and content quality
        Returns the most likely email column based on comprehensive scoring
        """
        if df.empty:
            return None
            
        candidates = []
        email_regex = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        
        for col in df.columns:
            score = 0
            col_clean = str(col).strip().lower()
            
            # Header scoring (higher = better match)
            for priority, pattern in enumerate(self.email_patterns):
                if re.match(pattern, col_clean):
                    # Exact matches get higher scores
                    if priority < 8:  # First 8 patterns are exact matches
                        score += 100 - priority
                    else:
                        score += 50 - (priority - 8)
                    break
            
            # Content scoring
            if df[col].dtype == 'object':
                sample = df[col].dropna().head(100)
                if len(sample) > 0:
                    email_count = sum(1 for val in sample if email_regex.search(str(val)))
                    email_ratio = email_count / len(sample)
                    
                    # Boost score based on email content
                    if email_ratio >= 0.8:
                        score += 50
                    elif email_ratio >= 0.5:
                        score += 30
                    elif email_ratio >= 0.3:
                        score += 20
                    elif email_ratio >= 0.1:
                        score += 10
            
            if score > 0:
                candidates.append((col, score))
        
        if candidates:
            # Sort by score (highest first)
            candidates.sort(key=lambda x: x[1], reverse=True)
            return candidates[0][0]
        
        return None
