"""
Person data model and extraction logic.
"""

from dataclasses import dataclass
from typing import List, Optional, Dict
import pandas as pd
from models.EmailEntry import EmailEntry


@dataclass
class Person:
    """
    Represents a single person with their associated data.
    """
    first_name: str
    last_name: str
    title: str
    company: str
    company_domain: str
    email: Optional[EmailEntry] = None
    linkedin: str = ""
    source_sheet: str = ""
    source_row: int = 0
    source_column_group: int = 1
    # Email validation results
    email_validated: bool = False
    email_action: str = ""  # "accepted", "fixed", "removed", "duplicate"
    email_rejection_reason: str = ""
    email_confidence: float = 0.0
    
    def __post_init__(self):
        """Post-initialization processing"""
        # Clean up string fields
        self.first_name = str(self.first_name).strip() if self.first_name else ""
        self.last_name = str(self.last_name).strip() if self.last_name else ""
        self.title = str(self.title).strip() if self.title else ""
        self.company = str(self.company).strip() if self.company else ""
        self.company_domain = str(self.company_domain).strip() if self.company_domain else ""
        self.linkedin = str(self.linkedin).strip() if self.linkedin else ""
    
    def is_empty(self) -> bool:
        """Check if person has no meaningful data"""
        return (
            not self.first_name and 
            not self.last_name and 
            not self.title and 
            not self.company and 
            not self.has_email() and 
            not self.linkedin
        )
    
    def get_full_name(self) -> str:
        """Get full name combining first and last"""
        parts = [self.first_name, self.last_name]
        return " ".join(part for part in parts if part).strip()
    
    def has_email(self) -> bool:
        """Check if person has a valid email"""
        return self.email is not None and not self.email.is_empty()
    
    def is_email_accepted(self) -> bool:
        """Check if email was accepted during validation"""
        return self.email_validated and self.email_action in ["accepted", "fixed"]
    
    def is_email_rejected(self) -> bool:
        """Check if email was rejected during validation"""
        return self.email_validated and self.email_action in ["removed", "duplicate"]
    
    def get_rejection_reason(self) -> str:
        """Get the reason why the person was rejected"""
        if self.is_email_rejected():
            return self.email_rejection_reason
        return ""
    
    def to_dict(self) -> dict:
        """Convert to dictionary for CSV export"""
        return {
            'First': self.first_name,
            'Last': self.last_name,
            'Title': self.title,
            'Company': self.company,
            'Company Domain': self.company_domain,
            'Email': self.email.raw if self.has_email() else "",
            'LinkedIn': self.linkedin,
            '': '',  # Empty column
            'Source Sheet': self.source_sheet,
            'Source Row': self.source_row,
            'Person Group': self.source_column_group
        }
    
    def to_dict_with_validation(self) -> dict:
        """Convert to dictionary for CSV export including validation info"""
        return {
            'First': self.first_name,
            'Last': self.last_name,
            'Title': self.title,
            'Company': self.company,
            'Company Domain': self.company_domain,
            'Email': self.email.raw if self.has_email() else "",
            'LinkedIn': self.linkedin,
            '': '',  # Empty column
            'Source Sheet': self.source_sheet,
            'Source Row': self.source_row,
            'Person Group': self.source_column_group,
            'Email Action': self.email_action,
            'Rejection Reason': self.get_rejection_reason(),
            'Email Confidence': self.email_confidence
        }


class PersonExtractor:
    """
    Extracts Person objects from DataFrames, handling both single and dual person rows
    """
    
    def __init__(self):
        # Import the semantic column detector
        from utils.semantic_col_detector import SemanticColumnDetector
        self.semantic_detector = SemanticColumnDetector()
    
    def map_columns_to_field_types(self, df: pd.DataFrame) -> Dict[str, List[int]]:
        """
        Scan the first row (column headers) and map each column index to its field type.
        Returns a dictionary like: {'first_name': [0, 3], 'last_name': [1, 4], ...}
        """
        field_column_map = {
            'first_name': [],
            'last_name': [],
            'title': [],
            'company': [],
            'company_domain': [],
            'email': [],
            'linkedin': []
        }
        
        print(f"Total columns in DataFrame: {len(df.columns)}")
        print("All column names:")
        for col_idx, col_name in enumerate(df.columns):
            print(f"  Column {col_idx}: '{col_name}'")
        
        # Go through each column header and determine its field type
        for col_idx, col_name in enumerate(df.columns):
            field_type = self.semantic_detector.is_person_field(col_name)
            if field_type and field_type in field_column_map:
                field_column_map[field_type].append(col_idx)
                print(f"  -> Detected as {field_type}")
        
        print(f"Column mapping: {field_column_map}")
        return field_column_map
    
    def determine_people_per_row(self, field_column_map: Dict[str, List[int]]) -> int:
        """
        Determine how many people are in each row based on the column mapping.
        Uses the minimum count of essential fields.
        """
        # Count how many columns we have for each field type
        field_counts = {field: len(columns) for field, columns in field_column_map.items()}
        
        # Use the minimum count of essential fields to determine max people per row
        essential_fields = ['first_name', 'last_name', 'email']
        essential_counts = [field_counts[field] for field in essential_fields if field_counts[field] > 0]
        
        if not essential_counts:
            return 0
        
        max_people = min(essential_counts)
        
        print(f"Field counts: {field_counts}")
        print(f"Max people per row: {max_people}")
        
        return max_people
    
    def extract_people_from_dataframe(self, df, sheet_name: str) -> List[Person]:
        """
        Extract all people from a single DataFrame using batch extraction
        First pass: all Person 1s, then all Person 2s, etc.
        Returns list of Person objects
        """
        people = []
        
        # Map columns to field types using column indices
        field_column_map = self.map_columns_to_field_types(df)
        
        # Determine how many people per row
        people_per_row = self.determine_people_per_row(field_column_map)
        
        if people_per_row == 0:
            print(f"No person fields detected in sheet: {sheet_name}")
            return people
        
        # Batch extraction: Process all Person 1s first, then all Person 2s, etc.
        for person_num in range(1, people_per_row + 1):
            # Extract this person from all rows using column indices
            for row_idx in range(len(df)):
                row_number = row_idx + 1  # 1-based row numbering
                
                person = self._extract_person_by_column_indices(
                    df, row_idx, field_column_map, person_num, sheet_name, row_number
                )
                if person and not person.is_empty():
                    people.append(person)
        
        return people
    
    def _extract_person_by_column_indices(self, df: pd.DataFrame, row_idx: int, 
                                         field_column_map: Dict[str, List[int]], person_num: int,
                                         sheet_name: str, row_number: int) -> Optional[Person]:
        """
        Extract the Nth person from a row using column indices
        person_num is 1-based (1, 2, 3...)
        """
        # Convert to 0-based index for array access
        person_index = person_num - 1
        
        # Extract each field for this person using column indices
        first_name = ""
        last_name = ""
        title = ""
        company = ""
        company_domain = ""
        email = None
        linkedin = ""
        
        # Get first name
        if person_index < len(field_column_map['first_name']):
            col_idx = field_column_map['first_name'][person_index]
            first_name = self._get_cell_value_by_index(df, row_idx, col_idx)
        
        # Get last name
        if person_index < len(field_column_map['last_name']):
            col_idx = field_column_map['last_name'][person_index]
            last_name = self._get_cell_value_by_index(df, row_idx, col_idx)
        
        # Get title
        if person_index < len(field_column_map['title']):
            col_idx = field_column_map['title'][person_index]
            title = self._get_cell_value_by_index(df, row_idx, col_idx)
        
        # Get company
        if person_index < len(field_column_map['company']):
            col_idx = field_column_map['company'][person_index]
            company = self._get_cell_value_by_index(df, row_idx, col_idx)
        
        # Get company domain
        if person_index < len(field_column_map['company_domain']):
            col_idx = field_column_map['company_domain'][person_index]
            company_domain = self._get_cell_value_by_index(df, row_idx, col_idx)
        
        # Get email
        if person_index < len(field_column_map['email']):
            col_idx = field_column_map['email'][person_index]
            email_raw = self._get_cell_value_by_index(df, row_idx, col_idx)
            if email_raw:
                # Get column name for the email column
                email_col_name = df.columns[col_idx]
                email = EmailEntry(
                    sheet=sheet_name,
                    row_number=row_number,
                    col_number=col_idx + 1,  # Convert to 1-based
                    col_name=email_col_name,
                    raw=email_raw
                )
        
        # Get LinkedIn
        if person_index < len(field_column_map['linkedin']):
            col_idx = field_column_map['linkedin'][person_index]
            linkedin = self._get_cell_value_by_index(df, row_idx, col_idx)
        
        # Create Person object
        person = Person(
            first_name=first_name,
            last_name=last_name,
            title=title,
            company=company,
            company_domain=company_domain,
            email=email,
            linkedin=linkedin,
            source_sheet=sheet_name,
            source_row=row_number,
            source_column_group=person_num
        )
        
        return person
    
    def _get_cell_value_by_index(self, df: pd.DataFrame, row_idx: int, col_idx: int) -> str:
        """Get cell value safely using column index"""
        try:
            value = df.iloc[row_idx, col_idx]
            return str(value) if pd.notna(value) else ""
        except (KeyError, IndexError):
            return ""
    
    def extract_all_people(self, dataframes: Dict[str, pd.DataFrame]) -> List[Person]:
        """
        Extract people from all DataFrames
        """
        all_people = []
        
        for sheet_name, df in dataframes.items():
            print(f"Processing sheet: {sheet_name}")
            people = self.extract_people_from_dataframe(df, sheet_name)
            all_people.extend(people)
            print(f"Extracted {len(people)} people from {sheet_name}")
        
        return all_people