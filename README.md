# Email Validator • Cleaner • Deduper

A fast, deterministic email validation and cleaning tool for large spreadsheets. Built with Python and Streamlit for easy web-based processing.

## 🚀 Quick Start

### Installation & Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Run the application
streamlit run main.py --server.port 5000 --server.address 127.0.0.1
```

**Access the app at:** `http://127.0.0.1:5000`

---

## 📋 System Overview

This tool processes CSV/Excel files containing email addresses through a multi-stage pipeline:

1. **File Loading** → Detects and loads various file formats
2. **Email Detection** → Automatically finds email columns 
3. **Extraction** → Converts emails to structured data with position tracking
4. **Processing** → Validates and cleans emails using deterministic rules
5. **Deduplication** → Removes duplicates with provider-aware logic
6. **Output Generation** → Creates cleaned datasets and detailed reports

---

## 🔄 Complete Workflow & Data Structures

### Stage 1: File Input & Loading
**Files:** `utils/io_handler.py`

**Input:** CSV, Excel (.xlsx/.xls), JSON, TSV files

**Process:**
- Detects file format automatically
- Loads data with memory-efficient chunking for large files
- Handles multiple sheets in Excel files

**Data Structure:**
```python
Dict[str, pd.DataFrame]
# Example:
{
    "Sheet1": pd.DataFrame(...),
    "Sheet2": pd.DataFrame(...),
    # For CSV: {"main": pd.DataFrame(...)}
}
```

### Stage 2: Email Column Detection
**Files:** `utils/email_col_detector.py`

**Process:**
- Scans all columns for email-like content
- Uses regex patterns and heuristics
- Detects multiple email columns per sheet

**Data Structure:**
```python
Dict[str, List[str]]
# Example:
{
    "Sheet1": ["email", "contact_email"],
    "Sheet2": ["user_email"]
}
```

### Stage 3: Email Extraction & Structuring
**Files:** `models/EmailEntry.py` → `EmailArrayExtractor`

**Process:**
- Extracts emails from detected columns
- Creates `EmailEntry` objects with position tracking
- Maintains row/column metadata for reconstruction

**Data Structure:**
```python
@dataclass
class EmailEntry:
    sheet: str           # Sheet name
    row_number: int      # 1-based row number
    col_number: int      # 1-based column number
    col_name: str        # Column header name
    raw: str             # Original email value
    cleaned: str         # Processed email (filled during processing)
    canonical_key: str   # Normalized key for deduplication
    action: str          # Processing action taken
    confidence: float    # Validation confidence (0.0-1.0)
    changed: bool        # Whether email was modified
    reason: str          # Reason for action taken

# Extraction Result:
{
    'email_entries': List[EmailEntry],
    'arrays_by_sheet': Dict[str, List[Dict]],
    'email_columns_by_sheet': Dict[str, List[str]],
    'total_entries': int,
    'non_empty_entries': int
}
```

### Stage 4: Email Processing & Validation
**Files:** `core/processor.py` + `core/email_hygeine_engine.py`

**Process:**
- **Deterministic validation** (no network calls)
- **Format cleaning** (whitespace, case normalization)
- **Typo correction** (common domain/TLD mistakes)
- **Role account filtering** (info@, admin@, etc.)
- **Disposable email detection**
- **Unicode/IDNA handling**

**Validation Engine Features:**
```python
# Typo correction examples:
"user@gmial.com" → "user@gmail.com"
"test@domain.con" → "test@domain.com"

# Format cleaning:
" USER@DOMAIN.COM " → "user@domain.com"

# Role account detection:
"info@company.com" → FILTERED (if enabled)
```

**Processing Results:**
```python
{
    'results': {
        'accepted': int,    # Valid emails kept
        'fixed': int,       # Emails corrected
        'removed': int,     # Invalid emails filtered
        'total_processed': int
    },
    'changes_report': List[Dict],  # Details of all changes made
    'rejected_report': List[Dict]  # Details of rejected emails
}
```

### Stage 5: Deduplication
**Files:** `core/email_deduper.py`

**Process:**
- **Provider-aware canonicalization:**
  - Gmail: Ignores dots and +tags (`user.name+tag@gmail.com` → `username@gmail.com`)
  - Other providers: Standard normalization
- **Duplicate detection** using canonical keys
- **Position tracking** for removal from original data

**Canonical Key Examples:**
```python
# Gmail examples:
"john.doe+newsletter@gmail.com" → "johndoe@gmail.com"
"j.o.h.n.d.o.e@gmail.com" → "johndoe@gmail.com"

# Standard examples:
"User@Domain.COM" → "user@domain.com"
```

**Deduplication Results:**
```python
{
    'total_duplicates_removed': int,
    'duplicate_positions': List[Tuple[str, int, int]],  # (sheet, row, col)
    'duplicates_report': List[Dict],
    'canonical_groups': Dict[str, List[EmailEntry]]
}
```

### Stage 6: Data Reconstruction & Output
**Files:** `core/pipeline.py` (orchestration)

**Process:**
- **Updates original DataFrames** with cleaned emails
- **Blanks duplicate rows** while preserving other data
- **Generates multiple output formats**
- **Creates comprehensive reports**

**Final Output Structure:**
```python
{
    'cleaned_data': Union[pd.DataFrame, Dict[str, pd.DataFrame]],
    'rejected_data': pd.DataFrame,      # Combined rejected + duplicates
    'changes_report': pd.DataFrame,     # All modifications made
    'duplicates_report': pd.DataFrame,  # Duplicate analysis
    'summary': {
        'accepted': int,
        'fixed': int,
        'removed': int,
        'duplicates': int
    },
    'options': Dict[str, Any]  # Processing options used
}
```

---

## 🏗️ Architecture & File Structure

```
EmailValidator/
├── main.py                          # Application entry point
├── requirements.txt                 # Python dependencies
├── models/                          # Data models
│   ├── __init__.py
│   └── EmailEntry.py               # Core data structures
├── utils/                          # Utility modules
│   ├── __init__.py
│   ├── email_col_detector.py       # Email column detection
│   └── io_handler.py              # File I/O operations
├── core/                           # Core processing logic
│   ├── __init__.py
│   ├── pipeline.py                 # Main orchestration
│   ├── processor.py                # Email processing
│   ├── email_deduper.py           # Deduplication logic
│   └── email_hygeine_engine.py    # Validation engine
├── ui/                            # User interface
│   ├── __init__.py
│   └── streamlit_ui.py            # Streamlit web interface
└── config/                        # Configuration files
    ├── disposable_domains.txt     # Disposable email providers
    ├── role_locals.txt           # Role account patterns
    ├── top_domains.txt           # Popular domain list
    └── typo_maps.json           # Typo correction mappings
```

### Component Responsibilities

| Component | Purpose | Key Classes/Functions |
|-----------|---------|----------------------|
| **FileHandler** | File I/O operations | `load_file()`, `get_file_preview()` |
| **EmailColumnDetector** | Email column detection | `detect_email_column()`, `get_all_email_columns()` |
| **EmailArrayExtractor** | Data extraction | `extract_all_emails()` |
| **EmailProcessor** | Email validation/cleaning | `process_email_entries()` |
| **EmailDeduplicator** | Duplicate removal | `deduplicate_entries()` |
| **EmailValidationPipeline** | Orchestration | `process_file()` |
| **EmailValidatorUI** | Web interface | `run()`, `_process_file()` |

---

## ⚙️ Processing Options

### Available Settings

- **Exclude Role Accounts** (default: `True`)
  - Filters system emails: `info@`, `admin@`, `sales@`, etc.
  
- **Provider-Aware Deduplication** (default: `True`)
  - Uses Gmail dot/plus semantics for intelligent deduplication
  
- **Export Detailed Reports** (default: `True`)
  - Generates rejected, changes, and duplicates reports

### Configuration Files

Located in `config/` directory:
- `disposable_domains.txt` - List of temporary email providers
- `role_locals.txt` - Role account patterns to filter
- `top_domains.txt` - Popular domains for typo correction
- `typo_maps.json` - Domain and TLD typo correction mappings

---

## 📊 Output Files

### 1. Cleaned Dataset
- **Format:** Same as input (CSV/Excel)
- **Content:** Original data with cleaned emails
- **Changes:** Duplicate rows blanked, invalid emails removed

### 2. Rejected Rows Report
- **Format:** CSV
- **Content:** All removed emails with reasons
- **Columns:** `sheet`, `row_number`, `col_name`, `raw_email`, `action`, `reason`

### 3. Changes Report (Optional)
- **Format:** CSV  
- **Content:** All email modifications made
- **Columns:** `sheet`, `row_number`, `col_name`, `original`, `cleaned`, `reason`

### 4. Duplicates Report (Optional)
- **Format:** CSV
- **Content:** Duplicate analysis by canonical groups
- **Columns:** `canonical_key`, `total_count`, `kept_email`, `removed_emails`

---

## 🔍 Key Features

### ✅ **Deterministic Processing**
- No network calls or external API dependencies
- Consistent, reproducible results
- Fast processing of large datasets

### ✅ **Smart Email Detection**
- Automatically finds email columns in any sheet
- Handles multiple email columns per file
- Works with various column naming conventions

### ✅ **Advanced Deduplication**
- Provider-aware canonicalization (Gmail, Yahoo, etc.)
- Maintains data integrity while removing duplicates
- Detailed duplicate analysis reporting

### ✅ **Comprehensive Validation**
- Format validation and cleaning
- Common typo correction (domain/TLD)
- Role account and disposable email filtering
- Unicode/international domain support

### ✅ **Memory Efficient**
- Chunked processing for large files
- Preview mode for file analysis
- Optimized data structures

### ✅ **Detailed Reporting**
- Complete audit trail of all changes
- Separate reports for different action types
- Processing statistics and analytics

---

## 🛠️ Development & Extension

### Adding New Validation Rules
Extend `core/email_hygeine_engine.py`:
```python
def validate_email_deterministic(email: str, options: Dict) -> ValidationResult:
    # Add custom validation logic
    pass
```

### Adding New File Formats
Extend `utils/io_handler.py`:
```python
def load_file(self, uploaded_file) -> Dict[str, pd.DataFrame]:
    # Add new format handling
    pass
```

### Future Expansion
The modular architecture supports adding new validation types:
- LinkedIn profile validation
- Name standardization
- Phone number cleaning
- Address validation

Simply add new models to `models/` and corresponding processors to `core/`.

---

## 📈 Performance Characteristics

- **Small files** (< 1MB): Near-instant processing
- **Medium files** (1-50MB): 10-30 seconds
- **Large files** (50MB+): 1-5 minutes
- **Memory usage:** ~2-3x file size during processing
- **Throughput:** ~10,000-50,000 emails/second (depending on complexity)

---

## 🔧 Troubleshooting

### Common Issues

1. **"No module named 'core'"** 
   - Run from project root directory
   - Ensure all `__init__.py` files are present

2. **"No email columns detected"**
   - Check column headers contain recognizable email terms
   - Verify data actually contains email addresses

3. **Memory errors with large files**
   - Reduce `chunk_size` in `FileHandler`
   - Process files in smaller batches

4. **Slow processing**
   - Disable detailed reporting for faster processing
   - Use provider-aware deduplication selectively

### Support
For issues or questions, check the file structure and ensure all dependencies are installed correctly.
