# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/claude-code) when working with code in this repository.

## Project Overview

This is a FastAPI-based file processing service that cleans and processes CSV and Excel files. It downloads files from URLs, removes duplicates and empty rows, cleans column names (by appending `_CHANGED`), and returns cleaned Excel files.

The application is designed to run on Railway and handles large files with memory-efficient processing.

## Running the Application

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run the server
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Using Docker
```bash
# Build the image
docker build -t excel-processor .

# Run the container
docker run -p 8000:8000 -e PORT=8000 excel-processor
```

### Environment Variables
- `RAILWAY_PUBLIC_DOMAIN`: The public domain for the service (optional, defaults to localhost:8000)
- `PORT`: The port to run the server on (defaults to 8000)

## Architecture

### Modular Design (Version 4.0)
The application has been refactored into a clean, modular structure for easy maintenance and modifications:

#### Folder Structure
```
Fast API/
├── main.py                          # Main application entry point
├── validations/                     # Input validation modules
│   ├── __init__.py
│   └── file_validator.py           # Validates file types and requests
├── cleaning/                        # Data cleaning modules
│   ├── __init__.py
│   └── data_cleaner.py             # Cleans DataFrames (EASY TO EDIT)
├── utils/                           # Utility modules
│   ├── __init__.py
│   ├── file_loader.py              # Downloads and loads files
│   └── file_manager.py             # Manages temp files and storage
└── routes/                          # API endpoint definitions
    ├── __init__.py
    └── file_routes.py              # All file processing endpoints
```

#### Module Responsibilities

**[validations/file_validator.py](validations/file_validator.py)**
- `FileValidator` class: Validates incoming requests and file types
- Checks file extensions (.csv, .xlsx, .xls, .xlsb)
- Easy to extend with additional validation rules

**[cleaning/data_cleaner.py](cleaning/data_cleaner.py)** ⭐ MAIN EDITING AREA
- `DataCleaner` class: Contains all data cleaning logic
- **Removes duplicate rows**
- **Removes empty rows**
- **Renames columns** (currently adds `_CHANGED` suffix)
- **Cleans cell values** (removes extra spaces)
- Well-documented methods make it easy for non-developers to modify cleaning rules
- Each cleaning operation is in its own method for clarity

**[utils/file_loader.py](utils/file_loader.py)**
- `FileLoader` class: Handles file downloading and loading
- Downloads files from URLs
- Auto-detects CSV encoding (chardet library)
- Loads different Excel formats with appropriate engines

**[utils/file_manager.py](utils/file_manager.py)**
- `FileManager` class: Manages temporary file storage
- Saves temporary input files
- Saves cleaned output files
- Cleans up old files (24-hour expiration)
- Provides storage information

**[routes/file_routes.py](routes/file_routes.py)**
- `FileProcessor` class: Orchestrates the entire workflow
- Coordinates between validator, loader, cleaner, and file manager
- Contains all API endpoint definitions
- Returns JSON responses with statistics

**[main.py](main.py)**
- Minimal entry point that sets up the FastAPI app
- Configures CORS middleware
- Initializes background scheduler
- Registers routes from the routes module

### File Processing Pipeline
The main endpoint `/process-file-from-url` follows this pipeline:

1. **Download**: Fetch file from provided URL (supports streaming for large files)
2. **Validate**: Check file extension (`.csv`, `.xlsx`, `.xls`, `.xlsb`)
3. **Save to temp**: Write to temporary directory with unique UUID
4. **Load**:
   - **CSV**: Auto-detect encoding using `chardet`, try multiple encodings with fallback
   - **Excel**: Use appropriate pandas engine (`openpyxl` for .xlsx, `xlrd` for .xls, `pyxlsb` for .xlsb)
5. **Clean**:
   - Remove duplicate rows
   - Remove empty rows (all NaN)
   - Strip whitespace from column names and append `_CHANGED` suffix
   - Strip whitespace from all cell values
6. **Save**: Export cleaned data to Excel (.xlsx) using `xlsxwriter` engine
7. **Return**: JSON response with download URL and processing statistics

### Memory Management
The application explicitly manages memory for large files:
- Uses `del` to remove large variables after use
- Calls `gc.collect()` to force garbage collection
- Processes files in stages to limit peak memory usage
- Cleans up temporary files immediately after loading

### Temporary File Storage
- Location: System temp directory + `/excel_processor/`
- Naming: `cleaned_{uuid}.xlsx` for outputs, `temp_input_{uuid}_{filename}` for inputs
- Cleanup: Background scheduler runs every 6 hours to delete files older than 24 hours
- All temp input files are deleted immediately after processing (in finally block)

### Encoding Detection (CSV Only)
CSV files use smart encoding detection:
1. Read first 100KB to detect encoding with `chardet`
2. Try detected encoding first
3. Fallback to common encodings: `utf-8`, `latin-1`, `iso-8859-1`, `cp1252`, `windows-1252`, `utf-16`
4. Use `on_bad_lines='skip'` and `encoding_errors='replace'` for robustness

### Background Tasks
A `BackgroundScheduler` from APScheduler runs cleanup every 6 hours to:
- Delete Excel output files older than 24 hours
- Delete leftover temporary input files (both .xlsx and .csv)

## API Endpoints

### Main Endpoints
- `POST /process-file-from-url`: Process CSV or Excel file from URL
- `POST /process-excel-from-url`: Legacy endpoint (redirects to above)
- `GET /download/{file_id}`: Download processed file by UUID

### Utility Endpoints
- `GET /`: API info and version
- `GET /health`: Health check with endpoint documentation
- `GET /storage-info`: Current storage usage and file list
- `GET /cat`: Returns a dog image (test endpoint)

## Dependencies

Key libraries and their purposes:
- `fastapi`: Web framework
- `pandas`: Data manipulation and cleaning
- `openpyxl`: Read/write .xlsx files
- `xlrd`: Read legacy .xls files
- `pyxlsb`: Read binary .xlsb files
- `xlsxwriter`: Fast Excel writing
- `chardet`: Automatic encoding detection for CSV files
- `APScheduler`: Background file cleanup
- `requests`: Download files from URLs

## Testing the API

```bash
# Test basic health
curl http://localhost:8000/health

# Process a file
curl -X POST http://localhost:8000/process-file-from-url \
  -H "Content-Type: application/json" \
  -d '{
    "file_url": "https://example.com/data.xlsx",
    "filename": "data.xlsx"
  }'

# Check storage
curl http://localhost:8000/storage-info
```

## How to Modify Data Cleaning Rules

All data cleaning logic is centralized in [cleaning/data_cleaner.py](cleaning/data_cleaner.py). This makes it very easy to modify how data is cleaned.

### Example Modifications

**To change how columns are renamed:**

Edit the `_clean_column_names` method in [cleaning/data_cleaner.py](cleaning/data_cleaner.py):

```python
# Current code (line ~124):
df.columns = [f"{col}_CHANGED" for col in df.columns]

# Change to uppercase:
df.columns = [f"{col.upper()}" for col in df.columns]

# Or add a different suffix:
df.columns = [f"{col}_CLEANED" for col in df.columns]

# Or remove suffix entirely:
df.columns = [f"{col}" for col in df.columns]
```

**To change how cell values are cleaned:**

Edit the `_clean_cell_values` method in [cleaning/data_cleaner.py](cleaning/data_cleaner.py):

```python
# Current code (line ~147):
df[col] = df[col].str.strip()

# Change to also convert to uppercase:
df[col] = df[col].str.strip().str.upper()

# Or convert to lowercase:
df[col] = df[col].str.strip().str.lower()

# Or replace specific values:
df[col] = df[col].str.strip().str.replace('old', 'new')
```

**To add new cleaning operations:**

Add a new method to the `DataCleaner` class and call it from the `clean_data` method:

```python
def _remove_specific_values(self, df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows containing specific values."""
    df_cleaned = df[df['column_name'] != 'unwanted_value']
    return df_cleaned
```

## Deployment Notes

- Designed for Railway deployment (uses `RAILWAY_PUBLIC_DOMAIN` env var)
- CORS configured for all origins (change `allow_origins=["*"]` for production)
- Files expire after 24 hours
- Extensive logging to stderr for debugging
- All processing times and statistics are tracked and returned
- All code is written to be readable and easy to follow for non-developers 