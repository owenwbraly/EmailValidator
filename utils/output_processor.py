"""
Output processing utilities for data formatting and optimization
Handles final data cleanup before export
"""

import pandas as pd
from typing import Dict, Any, Union


class OutputProcessor:
    """
    Processes final data for clean output formatting
    Handles column consolidation, empty data removal, and export optimization
    """
    
    def __init__(self):
        pass
    
    def consolidate_columns(self, data: Union[Dict[str, pd.DataFrame], pd.DataFrame]) -> Union[Dict[str, pd.DataFrame], pd.DataFrame]:
        """
        Remove unnamed and empty columns from data
        Works with both single DataFrames and dict of DataFrames (multi-sheet)
        """
        if isinstance(data, pd.DataFrame):
            # Single DataFrame (CSV case)
            return self._clean_single_dataframe(data)
        elif isinstance(data, dict):
            # Multiple DataFrames (Excel case)
            consolidated = {}
            for sheet_name, df in data.items():
                consolidated[sheet_name] = self._clean_single_dataframe(df)
            return consolidated
        else:
            return data
    
    def _clean_single_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean a single DataFrame by removing problematic columns
        """
        if df.empty:
            return df
        
        df_clean = df.copy()
        
        # Step 1: Remove unnamed columns (Unnamed: 0, Unnamed: 1, etc.)
        df_clean = df_clean.loc[:, ~df_clean.columns.str.contains('^Unnamed', na=False)]
        
        # Step 2: Remove completely empty columns (all NaN/None)
        df_clean = df_clean.dropna(axis=1, how='all')
        
        # Step 3: Remove columns that contain only whitespace
        columns_to_drop = []
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                # Check if column contains only whitespace or empty strings
                non_null_values = df_clean[col].dropna()
                if not non_null_values.empty:
                    # Check if all non-null values are just whitespace
                    if non_null_values.astype(str).str.strip().eq('').all():
                        columns_to_drop.append(col)
                elif non_null_values.empty:
                    # Column has no non-null values, already handled by dropna above
                    pass
        
        if columns_to_drop:
            df_clean = df_clean.drop(columns=columns_to_drop)
        
        # Step 4: Remove duplicate columns (same name and content)
        df_clean = self._remove_duplicate_columns(df_clean)
        
        return df_clean
    
    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove columns that have identical names and content
        """
        if df.empty:
            return df
        
        # Check for duplicate column names
        duplicate_cols = df.columns[df.columns.duplicated()].tolist()
        
        if duplicate_cols:
            # Keep only the first occurrence of each duplicate column
            df_clean = df.loc[:, ~df.columns.duplicated()]
            return df_clean
        
        return df
    
    def optimize_for_export(self, data: Union[Dict[str, pd.DataFrame], pd.DataFrame]) -> Union[Dict[str, pd.DataFrame], pd.DataFrame]:
        """
        Optimize data for export by cleaning up formatting and data types
        Future enhancement: could include column reordering, data type optimization, etc.
        """
        # For now, just consolidate columns, but this method can be expanded
        return self.consolidate_columns(data)
    
    def remove_empty_rows(self, data: Union[Dict[str, pd.DataFrame], pd.DataFrame]) -> Union[Dict[str, pd.DataFrame], pd.DataFrame]:
        """
        Remove completely empty rows from the data
        Future enhancement method
        """
        if isinstance(data, pd.DataFrame):
            return data.dropna(how='all')
        elif isinstance(data, dict):
            cleaned = {}
            for sheet_name, df in data.items():
                cleaned[sheet_name] = df.dropna(how='all')
            return cleaned
        else:
            return data
    
    def reorder_columns(self, data: Union[Dict[str, pd.DataFrame], pd.DataFrame], 
                       priority_columns: list = None) -> Union[Dict[str, pd.DataFrame], pd.DataFrame]:
        """
        Reorder columns to put important ones first
        Future enhancement method
        
        Args:
            data: DataFrame(s) to reorder
            priority_columns: List of column names to put first (e.g., ['name', 'email'])
        """
        if priority_columns is None:
            priority_columns = ['name', 'email', 'first_name', 'last_name', 'company']
        
        def reorder_single_df(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return df
            
            # Find priority columns that exist in the DataFrame
            existing_priority = [col for col in priority_columns if col in df.columns]
            
            # Get remaining columns
            remaining_cols = [col for col in df.columns if col not in existing_priority]
            
            # Reorder: priority columns first, then the rest
            new_order = existing_priority + remaining_cols
            
            return df[new_order]
        
        if isinstance(data, pd.DataFrame):
            return reorder_single_df(data)
        elif isinstance(data, dict):
            reordered = {}
            for sheet_name, df in data.items():
                reordered[sheet_name] = reorder_single_df(df)
            return reordered
        else:
            return data
