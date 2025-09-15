# Email Validator • Cleaner • Deduper (LLM-First)

An advanced LLM-powered email validation and cleaning tool designed for processing large spreadsheets with comprehensive reporting capabilities.

## Features

### 🤖 LLM-Powered Validation
- **Mandatory LLM analysis** for every email address
- Support for OpenAI (GPT-5) and Anthropic (Claude Sonnet 4) models
- JSON schema enforcement with retry logic and fallback handling

### 📊 Comprehensive Processing
- **Multi-format support**: CSV and Excel (multi-sheet) files
- **Large dataset handling**: Efficient processing of 100k+ rows with chunked reading
- **Memory optimization**: Streaming and batched processing for scalability

### 🔧 Smart Email Repair
- **Deterministic corrections**: TLD typos (.con → .com), domain typos (gmial → gmail)
- **High-confidence fixes**: Only applies repairs when LLM confidence ≥ threshold
- **Normalization**: Removes zero-width chars, smart quotes, angle brackets, fullwidth characters

### 🧹 Advanced Cleaning
- **Bad email removal**: Invalid syntax, disposable domains, role accounts, suspicious patterns
- **Provider-aware de-duplication**: Gmail dot/plus-tag handling, canonical key generation
- **Near-duplicate detection**: Edit distance analysis for manual review

### 📈 Real-time Progress
- **Live counters**: Accepted, Fixed, Removed, Duplicates
- **Progress tracking**: Step-by-step status with percentage completion
- **Memory monitoring**: Efficient chunked processing prevents out-of-memory issues

### 📄 Comprehensive Reporting
- **Cleaned Dataset**: Original format preserved, only email column updated
- **Rejected Dataset**: All removed rows with reasons and confidence scores
- **Changes Report**: Detailed log of all email modifications
- **Duplicates Report**: Canonical groups and duplicate analysis

## Installation

1. **Environment Setup**
   ```bash
   # Set your API keys
   export OPENAI_API_KEY="your-openai-key"
   # OR
   export ANTHROPIC_API_KEY="your-anthropic-key"
   ```

2. **Run the Application**
   ```bash
   streamlit run app.py --server.port 5000
   ```

3. **Access the Interface**
   - Open your browser to the provided URL
   - The application will automatically bind to `0.0.0.0:5000`

## Usage Guide

### 1. Upload Data File
- **Supported formats**: CSV, Excel (.xlsx, .xls)
- **Multi-sheet support**: Automatically processes all sheets with email columns
- **File size**: Optimized for large files (tested with 100k+ rows)

### 2. Configure LLM Settings
- **Provider**: Choose OpenAI or Anthropic based on available API keys
- **Model selection**: Latest models (GPT-5, Claude Sonnet 4) recommended
- **API keys**: Must be set in environment variables

### 3. Set Processing Options
- **Confidence threshold**: 0.50-0.99 (default: 0.85) for automatic fixes
- **Exclude role accounts**: Remove admin@, sales@, info@ type emails
- **Provider-aware dedup**: Use Gmail/Outlook-specific de-duplication rules
- **Export reports**: Generate detailed change and duplicate reports

### 4. Process and Download
- **Real-time progress**: Watch live counters and status updates
- **Multiple outputs**: Download cleaned data, rejected rows, and reports
- **Format preservation**: Output matches input format (CSV→CSV, Excel→Excel)

## Email Processing Pipeline

### 1. File Loading & Detection
- Chunked CSV reading for memory efficiency
- Multi-sheet Excel processing
- Automatic email column detection (case-insensitive header matching)

### 2. Deterministic Feature Extraction
- **Syntax validation**: RFC compliance, character constraints, label length
- **Normalization**: Whitespace, zero-width characters, smart quotes, fullwidth
- **Domain analysis**: IDN/punycode conversion, TLD validation, confusables detection
- **Risk assessment**: Role accounts, disposable domains, test patterns

### 3. LLM Classification (Mandatory)
- **JSON-enforced responses** with schema validation
- **Privacy protection**: Only email string and boolean flags sent to LLM
- **Retry logic**: Automatic fallback to deterministic decisions on failure

### 4. Intelligent Routing
- **Accept**: Valid emails with optional normalization
- **Fix**: High-confidence repairs (typo corrections, safe normalizations)
- **Remove**: Invalid syntax, disposable domains, policy violations
- **Review**: Uncertain cases (preserved in output but flagged)

### 5. Advanced De-duplication
- **Canonical key generation**: Provider-aware normalization
- **Exact duplicates**: Automatic removal with detailed reporting
- **Near-duplicates**: Edit distance analysis for manual review
- **First-occurrence preference**: Consistent duplicate resolution

### 6. Report Generation
- **Changes tracking**: Every modification logged with confidence scores
- **Rejected analysis**: Detailed reasons for all removed emails
- **Duplicate grouping**: Canonical mappings and similarity analysis

## Configuration

### Domain Lists (`config/`)
- **`disposable_domains.txt`**: Known temporary email providers
- **`role_locals.txt`**: System/organizational email prefixes
- **`top_domains.txt`**: Popular domains for fuzzy matching
- **`typo_maps.json`**: TLD and domain correction mappings

### Processing Options
- **Confidence threshold**: Minimum LLM confidence for automatic fixes
- **Role account exclusion**: Policy-based removal of system emails
- **Provider-aware de-dup**: Gmail/Outlook-specific canonicalization
- **Report generation**: Detailed change and duplicate analysis

## Privacy & Security

### Data Protection
- **Minimal LLM exposure**: Only email strings and boolean flags sent to APIs
- **No PII transmission**: Names, companies, and other fields never sent to LLM
- **Local processing**: All feature extraction and normalization done locally

### API Key Security
- **Environment variables**: Keys stored securely outside application
- **Session-local storage**: UI temporary storage only (not persisted)
- **Provider flexibility**: Support for multiple LLM providers

## Performance Specifications

### Scalability
- **Large files**: Tested with 100k+ row datasets
- **Memory efficiency**: Chunked processing prevents out-of-memory errors
- **Concurrent processing**: Batched LLM calls with rate limiting

### Processing Speed
- **Deterministic features**: Milliseconds per email
- **LLM classification**: Rate-limited to prevent API throttling
- **Progress tracking**: Real-time updates every 1k processed rows

## Technical Architecture

### Core Components
- **`app/ui.py`**: Streamlit interface with drag-and-drop upload
- **`app/pipeline.py`**: Main orchestration and progress tracking
- **`app/io_utils.py`**: File handling with chunked reading
- **`app/features.py`**: Deterministic feature extraction
- **`app/llm_adapter.py`**: Provider-agnostic LLM integration
- **`app/routing.py`**: Decision logic and policy enforcement
- **`app/dedupe.py`**: Advanced de-duplication algorithms

### Dependencies
- **Core**: `pandas`, `openpyxl`, `streamlit`
- **Email**: `email-validator`, `idna`, `tld`
- **LLM**: `openai`, `anthropic`
- **Utilities**: `rapidfuzz`, `python-dotenv`

## Validation Categories

### Accept ✅
- Syntactically valid emails
- Plausible domains with valid TLDs
- Personal email addresses from known providers

### Fix 🔧
- Obvious typo corrections (.con → .com)
- Common domain misspellings (gmial → gmail)
- Normalization issues (smart quotes, zero-width chars)

### Remove ❌
- Invalid syntax (multiple @, empty parts)
- Disposable domains (tempmail, guerrillamail)
- Role accounts (admin@, sales@) when excluded
- Dangerous confusables and test patterns

### Review 🔍
- Uncertain cases requiring human judgment
- Low-confidence LLM classifications
- Near-duplicates flagged for manual review

## Support & Troubleshooting

### Common Issues
1. **No API key error**: Set `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` environment variable
2. **Large file timeout**: Use chunked processing (automatically enabled for 10MB+ files)
3. **Memory issues**: Restart application and process smaller chunks

### Error Handling
- **LLM failures**: Automatic fallback to deterministic decisions
- **File format errors**: Clear error messages with suggested fixes
- **Progress recovery**: Chunked processing allows partial completion

### Performance Tips
- Use latest models (GPT-5, Claude Sonnet 4) for best accuracy
- Adjust confidence threshold based on your data quality requirements
- Enable provider-aware de-duplication for better Gmail/Outlook handling

---

**Note**: This tool validates email plausibility and hygiene. It does **not** perform SMTP deliverability checks or guarantee that emails can actually receive messages.
