from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io
from typing import List, Dict, Any
import traceback
from datetime import datetime
import sys

app = FastAPI()

# Enable CORS for Bubble.io to make requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your Bubble.io domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Helper function to create detailed error responses
def create_error_response(error: Exception, context: str) -> Dict[str, Any]:
    """Create a detailed error response for debugging"""
    error_details = {
        "success": False,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "context": context,
        "timestamp": datetime.now().isoformat(),
        "traceback": traceback.format_exc()
    }
    print(f"ERROR [{context}]: {error_details}", file=sys.stderr)
    return error_details

@app.get("/")
async def root():
    return {
        "message": "Excel Processor API is running",
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "/process-excel": "POST - Process Excel file and return cleaned version",
            "/process-excel-debug": "POST - Process with detailed JSON response",
            "/health": "GET - Health check"
        }
    }

@app.post("/process-excel")
async def process_excel(file: UploadFile = File(...)):
    """
    Receives an Excel file, extracts column names, and returns a cleaned Excel file
    """
    print(f"=== Starting Excel Processing ===", file=sys.stderr)
    print(f"Received file: {file.filename}", file=sys.stderr)
    print(f"Content type: {file.content_type}", file=sys.stderr)
    
    try:
        # Step 1: Validate file type
        print("Step 1: Validating file type...", file=sys.stderr)
        if not file.filename.endswith(('.xlsx', '.xls')):
            error_msg = f"Invalid file type. Received: {file.filename}"
            print(f"ERROR: {error_msg}", file=sys.stderr)
            raise HTTPException(
                status_code=400, 
                detail={
                    "success": False,
                    "error": "Invalid file type",
                    "message": "File must be an Excel file (.xlsx or .xls)",
                    "received_filename": file.filename
                }
            )
        print("✓ File type validated", file=sys.stderr)
        
        # Step 2: Read the uploaded file into memory
        print("Step 2: Reading file contents...", file=sys.stderr)
        contents = await file.read()
        file_size = len(contents)
        print(f"✓ File read successfully. Size: {file_size} bytes", file=sys.stderr)
        
        if file_size == 0:
            raise HTTPException(
                status_code=400,
                detail={
                    "success": False,
                    "error": "Empty file",
                    "message": "The uploaded file is empty"
                }
            )
        
        # Step 3: Load Excel file into pandas DataFrame
        print("Step 3: Loading Excel into pandas...", file=sys.stderr)
        try:
            df = pd.read_excel(io.BytesIO(contents))
            print(f"✓ Excel loaded. Shape: {df.shape}", file=sys.stderr)
        except Exception as e:
            print(f"ERROR loading Excel: {str(e)}", file=sys.stderr)
            raise HTTPException(
                status_code=400,
                detail={
                    "success": False,
                    "error": "Excel parsing error",
                    "message": f"Could not parse Excel file: {str(e)}",
                    "file_size": file_size
                }
            )
        
        # Step 4: Extract column names
        print("Step 4: Extracting column names...", file=sys.stderr)
        column_names = df.columns.tolist()
        print(f"✓ Extracted {len(column_names)} columns: {column_names}", file=sys.stderr)
        
        # Step 5: Clean the data
        print("Step 5: Cleaning data...", file=sys.stderr)
        original_row_count = len(df)
        print(f"  Original rows: {original_row_count}", file=sys.stderr)
        
        # Remove duplicate rows
        df_cleaned = df.drop_duplicates()
        duplicates_removed = original_row_count - len(df_cleaned)
        print(f"  Removed {duplicates_removed} duplicate rows", file=sys.stderr)
        
        # Remove rows with all NaN values
        before_nan = len(df_cleaned)
        df_cleaned = df_cleaned.dropna(how='all')
        empty_rows_removed = before_nan - len(df_cleaned)
        print(f"  Removed {empty_rows_removed} empty rows", file=sys.stderr)
        
        # Strip whitespace from column names
        df_cleaned.columns = df_cleaned.columns.str.strip()
        print(f"  Stripped whitespace from column names", file=sys.stderr)
        
        # Strip whitespace from string columns
        string_columns = df_cleaned.select_dtypes(include=['object']).columns
        for col in string_columns:
            try:
                df_cleaned[col] = df_cleaned[col].str.strip()
            except:
                pass  # Skip if column can't be stripped
        print(f"  Stripped whitespace from {len(string_columns)} string columns", file=sys.stderr)
        
        final_row_count = len(df_cleaned)
        print(f"✓ Cleaning complete. Final rows: {final_row_count}", file=sys.stderr)
        
        # Step 6: Create output Excel file in memory
        print("Step 6: Creating output Excel file...", file=sys.stderr)
        output = io.BytesIO()
        try:
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_cleaned.to_excel(writer, index=False, sheet_name='Cleaned Data')
            output.seek(0)
            output_size = len(output.getvalue())
            print(f"✓ Output file created. Size: {output_size} bytes", file=sys.stderr)
        except Exception as e:
            print(f"ERROR creating output file: {str(e)}", file=sys.stderr)
            raise HTTPException(
                status_code=500,
                detail={
                    "success": False,
                    "error": "Output generation error",
                    "message": f"Could not create output Excel file: {str(e)}"
                }
            )
        
        # Step 7: Return the cleaned Excel file
        print("Step 7: Returning file to client...", file=sys.stderr)
        print("=== Processing Complete ===", file=sys.stderr)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=cleaned_{file.filename}",
                "X-Column-Names": ",".join(column_names),
                "X-Original-Rows": str(original_row_count),
                "X-Final-Rows": str(final_row_count),
                "X-Duplicates-Removed": str(duplicates_removed),
                "X-Empty-Rows-Removed": str(empty_rows_removed)
            }
        )
    
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        # Catch any unexpected errors
        error_response = create_error_response(e, "Unexpected error in process_excel")
        print(f"FATAL ERROR: {error_response}", file=sys.stderr)
        raise HTTPException(
            status_code=500,
            detail=error_response
        )

@app.post("/process-excel-debug")
async def process_excel_debug(file: UploadFile = File(...)):
    """
    Process Excel file and return detailed JSON response instead of file
    Useful for debugging - shows all processing steps and results
    """
    debug_log = []
    
    def log(message: str, level: str = "INFO"):
        """Helper to log messages"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "level": level,
            "message": message
        }
        debug_log.append(log_entry)
        print(f"[{level}] {message}", file=sys.stderr)
    
    try:
        log(f"Received file: {file.filename}")
        log(f"Content type: {file.content_type}")
        
        # Validate file type
        if not file.filename.endswith(('.xlsx', '.xls')):
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": "Invalid file type",
                    "received_filename": file.filename,
                    "logs": debug_log
                }
            )
        log("File type validated")
        
        # Read file
        contents = await file.read()
        file_size = len(contents)
        log(f"File read successfully. Size: {file_size} bytes")
        
        if file_size == 0:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": "Empty file",
                    "logs": debug_log
                }
            )
        
        # Load Excel
        try:
            df = pd.read_excel(io.BytesIO(contents))
            log(f"Excel loaded. Shape: {df.shape} (rows: {df.shape[0]}, columns: {df.shape[1]})")
        except Exception as e:
            log(f"ERROR loading Excel: {str(e)}", "ERROR")
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": "Excel parsing error",
                    "error_message": str(e),
                    "logs": debug_log
                }
            )
        
        # Extract column info
        column_names = df.columns.tolist()
        column_types = df.dtypes.astype(str).to_dict()
        log(f"Extracted {len(column_names)} columns")
        
        # Get data preview (first 5 rows)
        data_preview = df.head(5).to_dict('records')
        
        # Clean data
        original_row_count = len(df)
        log(f"Starting cleaning. Original rows: {original_row_count}")
        
        df_cleaned = df.drop_duplicates()
        duplicates_removed = original_row_count - len(df_cleaned)
        log(f"Removed {duplicates_removed} duplicate rows")
        
        before_nan = len(df_cleaned)
        df_cleaned = df_cleaned.dropna(how='all')
        empty_rows_removed = before_nan - len(df_cleaned)
        log(f"Removed {empty_rows_removed} empty rows")
        
        df_cleaned.columns = df_cleaned.columns.str.strip()
        log("Stripped whitespace from column names")
        
        final_row_count = len(df_cleaned)
        log(f"Cleaning complete. Final rows: {final_row_count}")
        
        # Return comprehensive debug information
        return JSONResponse(
            content={
                "success": True,
                "filename": file.filename,
                "file_size_bytes": file_size,
                "original_row_count": original_row_count,
                "final_row_count": final_row_count,
                "duplicates_removed": duplicates_removed,
                "empty_rows_removed": empty_rows_removed,
                "column_count": len(column_names),
                "columns": column_names,
                "column_types": column_types,
                "data_preview": data_preview,
                "logs": debug_log,
                "message": "File processed successfully. Use /process-excel endpoint to download the cleaned file."
            }
        )
        
    except Exception as e:
        log(f"FATAL ERROR: {str(e)}", "ERROR")
        log(f"Traceback: {traceback.format_exc()}", "ERROR")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": "Unexpected error",
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "logs": debug_log
            }
        )

@app.get("/health")
async def health_check():
    """Health check endpoint with system info"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "python_version": sys.version,
        "pandas_version": pd.__version__
    }

# Error handler for validation errors
@app.exception_handler(422)
async def validation_exception_handler(request, exc):
    print(f"Validation Error: {exc}", file=sys.stderr)
    return JSONResponse(
        status_code=422,
        content={
            "success": False,
            "error": "Validation error",
            "message": "The request format is invalid",
            "details": str(exc)
        }
    )
