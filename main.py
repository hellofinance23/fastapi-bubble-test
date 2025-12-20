from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io
import os
import uuid
from pathlib import Path
import tempfile
from datetime import datetime
import sys
import requests
from typing import Dict

app = FastAPI()

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production: ["https://tucourse.com"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Create temp directory for storing files
TEMP_DIR = Path(tempfile.gettempdir()) / "excel_processor"
TEMP_DIR.mkdir(exist_ok=True)

# Get Railway's public URL from environment
RAILWAY_PUBLIC_URL = os.getenv("RAILWAY_PUBLIC_DOMAIN", "localhost:8000")
if not RAILWAY_PUBLIC_URL.startswith("http"):
    RAILWAY_PUBLIC_URL = f"https://{RAILWAY_PUBLIC_URL}"

@app.get("/")
def index():
    return {"Hello": "World", "version": "2.0"}

@app.get("/cat")
def cat():
    return FileResponse("files/dog.jpg")

@app.post("/process-excel-from-url")
async def process_excel_from_url(data: Dict = Body(...)):
    """
    Process Excel file from a URL (Bubble file URL, Better Uploader URL, etc.)
    
    Expected JSON body:
    {
        "file_url": "https://...",
        "filename": "data.xlsx"
    }
    """
    print("=" * 50, file=sys.stderr)
    print("Processing Excel from URL", file=sys.stderr)
    print("=" * 50, file=sys.stderr)
    
    try:
        # Extract parameters
        file_url = data.get("file_url")
        filename = data.get("filename", "file.xlsx")
        
        if not file_url:
            raise HTTPException(
                status_code=400,
                detail="file_url is required in request body"
            )
        
        print(f"File URL: {file_url}", file=sys.stderr)
        print(f"Filename: {filename}", file=sys.stderr)
        
        # Step 1: Download file from URL
        print("Step 1: Downloading file from URL...", file=sys.stderr)
        
        try:
            response = requests.get(file_url, stream=True, timeout=300)
            response.raise_for_status()
            file_content = response.content
        except requests.exceptions.RequestException as e:
            print(f"ERROR downloading file: {str(e)}", file=sys.stderr)
            raise HTTPException(
                status_code=400,
                detail=f"Failed to download file from URL: {str(e)}"
            )
        
        file_size_mb = len(file_content) / (1024 * 1024)
        print(f"✓ Downloaded {file_size_mb:.2f} MB", file=sys.stderr)
        
        # Step 2: Validate file type
        print("Step 2: Validating file type...", file=sys.stderr)
        if not filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400,
                detail="Invalid file type. Must be .xlsx or .xls"
            )
        print("✓ File type validated", file=sys.stderr)
        
        # Step 3: Load Excel into pandas
        print("Step 3: Loading Excel into pandas...", file=sys.stderr)
        try:
            df = pd.read_excel(io.BytesIO(file_content))
            print(f"✓ Loaded: {len(df)} rows, {len(df.columns)} columns", file=sys.stderr)
        except Exception as e:
            print(f"ERROR parsing Excel: {str(e)}", file=sys.stderr)
            raise HTTPException(
                status_code=400,
                detail=f"Failed to parse Excel file: {str(e)}"
            )
        
        # Step 4: Extract original columns
        original_columns = df.columns.tolist()
        original_row_count = len(df)
        print(f"Original columns: {original_columns}", file=sys.stderr)
        
        # Step 5: Clean the data
        print("Step 4: Cleaning data...", file=sys.stderr)
        
        # Remove duplicates
        df_cleaned = df.drop_duplicates()
        duplicates_removed = original_row_count - len(df_cleaned)
        print(f"  Removed {duplicates_removed} duplicate rows", file=sys.stderr)
        
        # Remove empty rows
        before_nan = len(df_cleaned)
        df_cleaned = df_cleaned.dropna(how='all')
        empty_rows_removed = before_nan - len(df_cleaned)
        print(f"  Removed {empty_rows_removed} empty rows", file=sys.stderr)
        
        # Strip whitespace from column names
        df_cleaned.columns = df_cleaned.columns.str.strip()
        
        # Add "_CHANGED" to column names
        df_cleaned.columns = [f"{col}_CHANGED" for col in df_cleaned.columns]
        new_columns = df_cleaned.columns.tolist()
        print(f"  New columns: {new_columns}", file=sys.stderr)
        
        # Strip whitespace from string columns
        string_columns = df_cleaned.select_dtypes(include=['object']).columns
        for col in string_columns:
            try:
                df_cleaned[col] = df_cleaned[col].str.strip()
            except:
                pass
        print(f"  Stripped whitespace from {len(string_columns)} string columns", file=sys.stderr)
        
        final_row_count = len(df_cleaned)
        print(f"✓ Cleaning complete: {final_row_count} rows", file=sys.stderr)
        
        # Step 6: Save the cleaned Excel file
        print("Step 5: Saving cleaned file...", file=sys.stderr)
        file_id = str(uuid.uuid4())
        output_filename = f"cleaned_{file_id}.xlsx"
        output_path = TEMP_DIR / output_filename
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_cleaned.to_excel(writer, index=False, sheet_name='Cleaned Data')
        
        output_size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"✓ Output file created: {output_size_mb:.2f} MB", file=sys.stderr)
        
        # Step 7: Create download URL
        download_url = f"{RAILWAY_PUBLIC_URL}/download/{file_id}"
        print(f"Download URL: {download_url}", file=sys.stderr)
        
        print("=" * 50, file=sys.stderr)
        print("Processing Complete!", file=sys.stderr)
        print("=" * 50, file=sys.stderr)
        
        # Return JSON response
        return JSONResponse(content={
            "success": True,
            "download_url": download_url,
            "file_id": file_id,
            "original_filename": filename,
            "processed_filename": output_filename,
            "stats": {
                "original_rows": original_row_count,
                "final_rows": final_row_count,
                "duplicates_removed": duplicates_removed,
                "empty_rows_removed": empty_rows_removed,
                "original_columns": original_columns,
                "new_columns": new_columns,
                "input_size_mb": round(file_size_mb, 2),
                "output_size_mb": round(output_size_mb, 2)
            },
            "processing_time": "completed",
            "expires_in": "24 hours"
        })
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"FATAL ERROR: {str(e)}", file=sys.stderr)
        import traceback
        print(traceback.format_exc(), file=sys.stderr)
        raise HTTPException(
            status_code=500,
            detail=f"Processing error: {str(e)}"
        )

@app.get("/download/{file_id}")
async def download_file(file_id: str):
    """
    Download the processed Excel file by ID
    """
    print(f"Download request for file_id: {file_id}", file=sys.stderr)
    
    try:
        file_path = TEMP_DIR / f"cleaned_{file_id}.xlsx"
        
        if not file_path.exists():
            print(f"File not found: {file_path}", file=sys.stderr)
            raise HTTPException(
                status_code=404,
                detail="File not found or expired. Files are kept for 24 hours."
            )
        
        print(f"Serving file: {file_path}", file=sys.stderr)
        
        return FileResponse(
            path=file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"cleaned_data.xlsx",
            headers={
                "Content-Disposition": "attachment; filename=cleaned_data.xlsx"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR serving file: {str(e)}", file=sys.stderr)
        raise HTTPException(status_code=500, detail=str(e))

# Cleanup old files
from apscheduler.schedulers.background import BackgroundScheduler
import time

def cleanup_old_files():
    """Remove files older than 24 hours"""
    print("Running cleanup task...", file=sys.stderr)
    current_time = time.time()
    deleted_count = 0
    
    for file_path in TEMP_DIR.glob("cleaned_*.xlsx"):
        file_age_hours = (current_time - file_path.stat().st_mtime) / 3600
        
        if file_age_hours > 24:
            try:
                file_path.unlink()
                deleted_count += 1
                print(f"Deleted old file: {file_path.name}", file=sys.stderr)
            except Exception as e:
                print(f"Error deleting {file_path.name}: {e}", file=sys.stderr)
    
    print(f"Cleanup complete. Deleted {deleted_count} files.", file=sys.stderr)

scheduler = BackgroundScheduler()
scheduler.add_job(cleanup_old_files, 'interval', hours=6)
scheduler.start()

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "version": "2.0",
        "endpoint": "/process-excel-from-url",
        "temp_dir": str(TEMP_DIR),
        "files_count": len(list(TEMP_DIR.glob("cleaned_*.xlsx")))
    }

@app.get("/storage-info")
def storage_info():
    """Check storage usage"""
    files = list(TEMP_DIR.glob("cleaned_*.xlsx"))
    total_size = sum(f.stat().st_size for f in files)
    
    return {
        "files_count": len(files),
        "total_size_mb": round(total_size / (1024 * 1024), 2),
        "files": [
            {
                "name": f.name,
                "size_mb": round(f.stat().st_size / (1024 * 1024), 2),
                "age_hours": round((time.time() - f.stat().st_mtime) / 3600, 1)
            }
            for f in files
        ]
    }