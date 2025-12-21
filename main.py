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
import time
import gc

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
    return {
        "message": "Excel/CSV Processor API",
        "version": "3.0",
        "supported_formats": ["CSV", "Excel (.xlsx, .xls)"]
    }

@app.get("/cat")
def cat():
    return FileResponse("files/dog.jpg")

@app.post("/process-file-from-url")
async def process_file_from_url(data: Dict = Body(...)):
    """
    Process CSV or Excel file from a URL
    Automatically detects format and uses optimal loading method
    
    Expected JSON body:
    {
        "file_url": "https://...",
        "filename": "data.xlsx" or "data.csv"
    }
    """
    print("=" * 50, file=sys.stderr)
    print("Processing File from URL (CSV/Excel)", file=sys.stderr)
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
        print("Step 1: Downloading file...", file=sys.stderr)
        download_start = time.time()
        
        try:
            response = requests.get(file_url, stream=True, timeout=600)
            response.raise_for_status()
            file_content = response.content
        except requests.exceptions.RequestException as e:
            print(f"ERROR downloading file: {str(e)}", file=sys.stderr)
            raise HTTPException(
                status_code=400,
                detail=f"Failed to download file from URL: {str(e)}"
            )
        
        file_size_mb = len(file_content) / (1024 * 1024)
        download_time = time.time() - download_start
        print(f"✓ Downloaded {file_size_mb:.2f} MB in {download_time:.1f}s", file=sys.stderr)
        
        # Step 2: Validate file type
        print("Step 2: Validating file type...", file=sys.stderr)
        
        is_csv = filename.lower().endswith('.csv')
        is_excel = filename.lower().endswith(('.xlsx', '.xls', '.xlsb'))
        
        if not (is_csv or is_excel):
            raise HTTPException(
                status_code=400,
                detail="Invalid file type. Must be .csv, .xlsx, .xls, or .xlsb"
            )
        
        file_type = "CSV" if is_csv else "Excel"
        print(f"✓ File type validated: {file_type}", file=sys.stderr)
        
        # Step 3: Save to temp file
        print("Step 3: Saving to temporary file...", file=sys.stderr)
        temp_input_path = TEMP_DIR / f"temp_input_{uuid.uuid4()}_{filename}"
        
        with open(temp_input_path, 'wb') as f:
            f.write(file_content)
        
        print(f"✓ Saved to: {temp_input_path.name}", file=sys.stderr)
        
        # Free memory
        del file_content
        gc.collect()
        
        # Step 4: Load file (CSV or Excel)
        print(f"Step 4: Loading {file_type}...", file=sys.stderr)
        load_start = time.time()
        
        try:
            if is_csv:
                # FAST PATH: Load CSV
                print("  Using CSV reader (fast)...", file=sys.stderr)
                df = pd.read_csv(
                    temp_input_path,
                    dtype=str,  # Read as strings for consistency
                    encoding='utf-8',
                    low_memory=False,
                    on_bad_lines='skip'  # Skip problematic rows
                )
                engine_used = "CSV (fast)"
            
            elif filename.lower().endswith('.xlsx'):
                # Excel .xlsx format
                print("  Using openpyxl engine...", file=sys.stderr)
                df = pd.read_excel(
                    temp_input_path,
                    engine='openpyxl',
                    dtype=str  # Read as strings for consistency
                )
                engine_used = "Excel (openpyxl)"
            
            elif filename.lower().endswith('.xls'):
                # Old Excel .xls format
                print("  Using xlrd engine...", file=sys.stderr)
                df = pd.read_excel(
                    temp_input_path,
                    engine='xlrd',
                    dtype=str
                )
                engine_used = "Excel (xlrd)"
            
            elif filename.lower().endswith('.xlsb'):
                # Binary Excel format
                print("  Using pyxlsb engine...", file=sys.stderr)
                df = pd.read_excel(
                    temp_input_path,
                    engine='pyxlsb',
                    dtype=str
                )
                engine_used = "Excel (pyxlsb)"
            
            load_time = time.time() - load_start
            
            print(f"✓ Loaded {len(df):,} rows × {len(df.columns)} cols", file=sys.stderr)
            print(f"  Engine: {engine_used}", file=sys.stderr)
            print(f"  Load time: {load_time:.1f}s", file=sys.stderr)
            
            if load_time > 0:
                print(f"  Speed: {file_size_mb/load_time:.1f} MB/s", file=sys.stderr)
        
        except Exception as e:
            print(f"ERROR loading file: {str(e)}", file=sys.stderr)
            import traceback
            print(traceback.format_exc(), file=sys.stderr)
            raise HTTPException(
                status_code=400,
                detail=f"Failed to parse file: {str(e)}"
            )
        
        finally:
            # Always clean up temp file
            try:
                temp_input_path.unlink()
                print("✓ Cleaned up temp input file", file=sys.stderr)
            except Exception as e:
                print(f"Warning: couldn't delete temp file: {e}", file=sys.stderr)
        
        # Step 5: Extract original columns
        original_columns = df.columns.tolist()
        original_row_count = len(df)
        print(f"Original: {original_row_count:,} rows, {len(original_columns)} columns", 
              file=sys.stderr)
        
        # Step 6: Clean the data
        print("Step 5: Cleaning data...", file=sys.stderr)
        clean_start = time.time()
        
        # Remove duplicates
        print("  Removing duplicates...", file=sys.stderr)
        df_cleaned = df.drop_duplicates()
        duplicates_removed = original_row_count - len(df_cleaned)
        print(f"  ✓ Removed {duplicates_removed:,} duplicate rows", file=sys.stderr)
        
        # Free memory
        del df
        gc.collect()
        
        # Remove empty rows
        print("  Removing empty rows...", file=sys.stderr)
        before_nan = len(df_cleaned)
        df_cleaned = df_cleaned.dropna(how='all')
        empty_rows_removed = before_nan - len(df_cleaned)
        print(f"  ✓ Removed {empty_rows_removed:,} empty rows", file=sys.stderr)
        
        # Clean column names
        print("  Cleaning column names...", file=sys.stderr)
        df_cleaned.columns = df_cleaned.columns.str.strip()
        df_cleaned.columns = [f"{col}_CHANGED" for col in df_cleaned.columns]
        new_columns = df_cleaned.columns.tolist()
        print(f"  ✓ Renamed {len(new_columns)} columns", file=sys.stderr)
        
        # Strip whitespace from all cells
        print("  Cleaning cell values...", file=sys.stderr)
        for col in df_cleaned.columns:
            try:
                df_cleaned[col] = df_cleaned[col].str.strip()
            except:
                pass
        print(f"  ✓ Cleaned cell values", file=sys.stderr)
        
        final_row_count = len(df_cleaned)
        clean_time = time.time() - clean_start
        print(f"✓ Cleaning complete in {clean_time:.1f}s → {final_row_count:,} final rows", 
              file=sys.stderr)
        
        # Step 7: Save the cleaned file as Excel
        print("Step 6: Saving cleaned file as Excel...", file=sys.stderr)
        save_start = time.time()
        
        file_id = str(uuid.uuid4())
        output_filename = f"cleaned_{file_id}.xlsx"
        output_path = TEMP_DIR / output_filename
        
        print(f"  Output: {output_filename}", file=sys.stderr)
        
        # Use xlsxwriter (faster for writing)
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            df_cleaned.to_excel(writer, index=False, sheet_name='Cleaned Data')
        
        save_time = time.time() - save_start
        output_size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"✓ Saved {output_size_mb:.2f} MB in {save_time:.1f}s", file=sys.stderr)
        
        # Free memory
        del df_cleaned
        gc.collect()
        
        # Step 8: Create download URL
        download_url = f"{RAILWAY_PUBLIC_URL}/download/{file_id}"
        print(f"Download URL: {download_url}", file=sys.stderr)
        
        total_time = download_time + load_time + clean_time + save_time
        
        print("=" * 50, file=sys.stderr)
        print(f"✅ COMPLETED in {total_time:.1f}s", file=sys.stderr)
        print(f"  Input format:  {file_type}", file=sys.stderr)
        print(f"  Download:      {download_time:.1f}s", file=sys.stderr)
        print(f"  Load:          {load_time:.1f}s", file=sys.stderr)
        print(f"  Clean:         {clean_time:.1f}s", file=sys.stderr)
        print(f"  Save:          {save_time:.1f}s", file=sys.stderr)
        print("=" * 50, file=sys.stderr)
        
        # Return JSON response
        return JSONResponse(content={
            "success": True,
            "download_url": download_url,
            "file_id": file_id,
            "original_filename": filename,
            "processed_filename": output_filename,
            "input_format": file_type,
            "stats": {
                "original_rows": original_row_count,
                "final_rows": final_row_count,
                "duplicates_removed": duplicates_removed,
                "empty_rows_removed": empty_rows_removed,
                "original_columns": len(original_columns),
                "input_size_mb": round(file_size_mb, 2),
                "output_size_mb": round(output_size_mb, 2),
                "timings": {
                    "download_seconds": round(download_time, 1),
                    "load_seconds": round(load_time, 1),
                    "clean_seconds": round(clean_time, 1),
                    "save_seconds": round(save_time, 1),
                    "total_seconds": round(total_time, 1)
                },
                "engine_used": engine_used
            },
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

# Keep backward compatibility with old endpoint name
@app.post("/process-excel-from-url")
async def process_excel_from_url(data: Dict = Body(...)):
    """
    Legacy endpoint - redirects to new endpoint
    """
    return await process_file_from_url(data)

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
            filename="cleaned_data.xlsx",
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
    
    # Also cleanup temp input files
    for file_path in TEMP_DIR.glob("temp_input_*.xlsx"):
        try:
            file_path.unlink()
            deleted_count += 1
        except:
            pass
    
    for file_path in TEMP_DIR.glob("temp_input_*.csv"):
        try:
            file_path.unlink()
            deleted_count += 1
        except:
            pass
    
    if deleted_count > 0:
        print(f"Cleanup complete. Deleted {deleted_count} files.", file=sys.stderr)

scheduler = BackgroundScheduler()
scheduler.add_job(cleanup_old_files, 'interval', hours=6)
scheduler.start()

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "version": "3.0",
        "supported_formats": ["CSV", "Excel (.xlsx, .xls, .xlsb)"],
        "endpoints": {
            "/process-file-from-url": "POST - Process CSV or Excel file",
            "/process-excel-from-url": "POST - Legacy endpoint (same as above)",
            "/download/{file_id}": "GET - Download processed file"
        },
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