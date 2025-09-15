"""
File I/O utilities for handling CSV and Excel files
Supports chunked reading and memory-efficient processing
"""

import pandas as pd
import io
from typing import Dict, Any, Union, List
import openpyxl


class FileHandler:
    def __init__(self, chunk_size: int = 20000):
        self.chunk_size = chunk_size
    
    def load_file(self, uploaded_file) -> Dict[str, pd.DataFrame]:
        """
        Load CSV or Excel file and return dict of sheet_name -> DataFrame
        For CSV files, uses 'main' as the sheet name
        """
        file_extension = uploaded_file.name.lower().split('.')[-1]
        
        if file_extension == 'csv':
            return self._load_csv(uploaded_file)
        elif file_extension in ['xlsx', 'xls']:
            return self._load_excel(uploaded_file)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
    
    def _load_csv(self, uploaded_file) -> Dict[str, pd.DataFrame]:
        """Load CSV file with chunked reading for large files"""
        try:
            # Reset file pointer
            uploaded_file.seek(0)
            
            # Try to detect file size and use chunked reading for large files
            file_size = len(uploaded_file.getvalue())
            
            if file_size > 10 * 1024 * 1024:  # 10MB threshold
                # Use chunked reading
                chunks = []
                for chunk in pd.read_csv(uploaded_file, chunksize=self.chunk_size):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
            else:
                # Read entire file
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file)
            
            return {'main': df}
            
        except Exception as e:
            raise ValueError(f"Error reading CSV file: {str(e)}")
    
    def _load_excel(self, uploaded_file) -> Dict[str, pd.DataFrame]:
        """Load Excel file with all sheets"""
        try:
            # Reset file pointer
            uploaded_file.seek(0)
            
            # Read all sheets
            sheets_dict = pd.read_excel(uploaded_file, sheet_name=None, engine='openpyxl')
            
            return sheets_dict
            
        except Exception as e:
            raise ValueError(f"Error reading Excel file: {str(e)}")
    
    def save_to_excel(self, sheets_dict: Dict[str, pd.DataFrame]) -> bytes:
        """Save multiple sheets to Excel format and return bytes"""
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for sheet_name, df in sheets_dict.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        return output.getvalue()
    
    def save_to_csv(self, df: pd.DataFrame) -> str:
        """Save DataFrame to CSV format and return string"""
        return df.to_csv(index=False)
