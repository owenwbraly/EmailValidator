# Email Validator • Cleaner • Deduper (LLM-First)

## Overview

This is an advanced email validation and cleaning tool designed for processing large spreadsheets (CSV and Excel files) with comprehensive LLM-powered analysis. The application serves as a single-page web interface built with Streamlit that validates email plausibility and hygiene, applies smart repairs, removes bad emails, and performs de-duplication. The tool is specifically designed to handle large datasets (100k+ rows) efficiently while providing detailed reporting on all processing actions.

The system follows an "LLM-first" approach where every email address undergoes mandatory analysis by either OpenAI (GPT-5) or Anthropic (Claude Sonnet 4) models, augmented by deterministic validation rules. The tool focuses on email plausibility and hygiene validation rather than SMTP deliverability testing.

## User Preferences

Preferred communication style: Simple, everyday language.

## Recent Changes

- **[September 17, 2025]** Completed comprehensive array-based processing refactor as requested by user
  - Implemented simplified array-first data flow with EmailEntry model for position tracking
  - Added ArrayEmailExtractor for multi-column email extraction with sheet/row/column metadata
  - Built ArrayEmailProcessor for deterministic cleaning on email arrays
  - Created ArrayEmailDeduplicator with position-aware duplicate detection
  - Implemented row blanking for duplicates (preserves row spacing) with separate duplicates file export
  - Integrated new array mode into UI with default-enabled checkbox
  - Successfully tested end-to-end: Accepted=4, Duplicates=1, proper file generation
- Enhanced email column detection with better pattern matching, content validation, and false positive prevention
- Added comprehensive file preview with email column detection across all sheets
- Implemented memory-optimized processing for large files with chunked reading
- Fixed critical duplicate removal bug where all duplicates were being deleted instead of keeping one copy

## System Architecture

### Frontend Architecture
- **Framework**: Streamlit for single-page web application
- **UI Design**: Clean, accessible interface with drag-and-drop file upload, progress tracking, and comprehensive results display
- **User Experience**: Non-technical user friendly with clear status indicators, real-time progress counters, and downloadable reports

### Backend Architecture
- **Pipeline Pattern**: Orchestrated processing pipeline (`EmailValidationPipeline`) that coordinates all processing steps
- **Modular Components**: Separate modules for file I/O, email detection, feature extraction, normalization, LLM integration, decision routing, and de-duplication
- **Memory Optimization**: Chunked file reading and streaming processing to handle large datasets without memory overflow
- **Error Handling**: Comprehensive retry logic and fallback mechanisms for LLM API calls

### Core Processing Components

#### File Processing
- **Multi-format Support**: Handles CSV and Excel (multi-sheet) files with automatic format detection
- **Chunked Reading**: Uses configurable chunk sizes (default 20k rows) for memory-efficient processing of large files
- **Sheet Detection**: Automatically processes all sheets in Excel files, passing through non-email sheets unchanged

#### Email Column Detection
- **Pattern Matching**: Uses regex patterns to identify email columns with variations like "email", "email address", "e-mail", etc.
- **Content-based Fallback**: Secondary detection based on email-like content patterns when header matching fails
- **Case-insensitive**: Handles various header naming conventions

#### Feature Extraction and Normalization
- **Deterministic Cleanup**: Removes zero-width characters, smart quotes, angle brackets, fullwidth characters
- **Character Normalization**: Unicode normalization and character replacement
- **Domain Standardization**: Lowercase domains, punycode conversion, TLD validation
- **Typo Correction**: Configurable maps for common TLD and domain typos

#### LLM Integration
- **Dual Provider Support**: OpenAI and Anthropic APIs with consistent interface
- **JSON Schema Enforcement**: Structured responses with retry logic for malformed outputs
- **Rate Limiting**: Built-in throttling to respect API limits
- **Confidence Scoring**: All LLM decisions include confidence scores for decision making

#### Decision Routing
- **Authoritative Policy**: Clear decision hierarchy for accept/fix/remove actions
- **Confidence Thresholds**: Configurable thresholds for applying automated fixes
- **Safety Checks**: Multiple validation layers before applying changes

#### De-duplication Engine
- **Canonical Key Generation**: Provider-aware normalization (Gmail dot/plus-tag handling)
- **Exact Duplicate Detection**: Based on canonical email forms
- **Near-duplicate Analysis**: Edit distance algorithms for potential duplicate identification
- **Preservation Logic**: Keeps first occurrence or most complete records

### Data Flow Architecture
1. **Input Processing**: File upload → format detection → chunked loading
2. **Email Detection**: Column identification → content validation
3. **Feature Extraction**: Deterministic analysis → flag generation
4. **LLM Analysis**: Structured prompt → JSON response → confidence evaluation
5. **Decision Making**: Policy application → action determination
6. **De-duplication**: Canonical grouping → duplicate resolution
7. **Output Generation**: Multiple report formats → file downloads

### Configuration Management
- **External Config Files**: JSON typo maps, text-based domain lists
- **Environment Variables**: API key management
- **Runtime Options**: User-configurable processing parameters

## External Dependencies

### LLM Providers
- **OpenAI API**: GPT models for email classification and validation
- **Anthropic API**: Claude models as alternative LLM provider
- **API Key Management**: Environment variable based authentication

### Python Libraries
- **pandas**: Core data manipulation and CSV/Excel processing
- **openpyxl**: Excel file format handling and multi-sheet support
- **streamlit**: Web application framework and UI components
- **idna**: International domain name processing and punycode conversion
- **tld**: Top-level domain validation and extraction
- **unicodedata**: Unicode character normalization and processing

### Data Sources
- **Disposable Domains List**: Curated list of temporary/disposable email providers
- **Role Account Patterns**: Common role-based email prefixes (admin, sales, etc.)
- **Top Domains List**: Whitelist of legitimate email domains
- **Typo Correction Maps**: Common TLD and domain spelling corrections

### File Processing
- **CSV Support**: Standard CSV reading with chunked processing capability
- **Excel Support**: Multi-sheet Excel files with format preservation
- **Memory Management**: Streaming and batched processing for large files

### Configuration Files
- `config/typo_maps.json`: Domain and TLD correction mappings
- `config/disposable_domains.txt`: List of disposable email providers
- `config/role_locals.txt`: Role-based email local parts
- `config/top_domains.txt`: Legitimate domain whitelist

The system requires either an OpenAI or Anthropic API key to function, as LLM analysis is mandatory for all email processing. Without an API key, the application displays a blocking message and prevents processing.