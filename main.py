from fastapi import FastAPI, UploadFile, File, HTTPException
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
    return {"Hello": "World"}

@app.get("/cat")
def cat():
    return FileResponse("files/dog.jpg")

@app.post("/process-excel")
async def process_excel(file: UploadFile = File(...)):
    """
    Process Excel file and return a download URL instead of the file
    """
    print(f"=== Processing Excel File ===", file=sys.stderr)
    print(f"Filename: {file.filename}", file=sys.stderr)
    print(f"Content-Type: {file.content_type}", file=sys.stderr)
    
    try:
        # Validate file type
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400,
                detail="Invalid file type. Must be .xlsx or .xls"
            )
        
        # Read the file
        print("Reading file contents...", file=sys.stderr)
        contents = await file.read()
        file_size_mb = len(contents) / (1024 * 1024)
        print(f"File size: {file_size_mb:.2f} MB", file=sys.stderr)
        
        # Load into pandas
        print("Loading Excel...", file=sys.stderr)
        df = pd.read_excel(io.BytesIO(contents))
        print(f"Loaded: {len(df)} rows, {len(df.columns)} columns", file=sys.stderr)
        
        # Extract original columns
        original_columns = df.columns.tolist()
        original_row_count = len(df)
        
        # Clean the data
        print("Cleaning data...", file=sys.stderr)
        df_cleaned = df.drop_duplicates()
        duplicates_removed = original_row_count - len(df_cleaned)
        
        df_cleaned = df_cleaned.dropna(how='all')
        empty_rows_removed = (original_row_count - duplicates_removed) - len(df_cleaned)
        
        # Strip whitespace from column names
        df_cleaned.columns = df_cleaned.columns.str.strip()
        
        # Add "_CHANGED" to column names
        df_cleaned.columns = [f"{col}_CHANGED" for col in df_cleaned.columns]
        new_columns = df_cleaned.columns.tolist()
        
        # Strip whitespace from string columns
        for col in df_cleaned.select_dtypes(include=['object']).columns:
            try:
                df_cleaned[col] = df_cleaned[col].str.strip()
            except:
                pass
        
        final_row_count = len(df_cleaned)
        print(f"Cleaning complete: {final_row_count} rows", file=sys.stderr)
        
        # Generate unique file ID
        file_id = str(uuid.uuid4())
        output_filename = f"cleaned_{file_id}.xlsx"
        output_path = TEMP_DIR / output_filename
        
        # Save the cleaned Excel file
        print(f"Saving to: {output_path}", file=sys.stderr)
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_cleaned.to_excel(writer, index=False, sheet_name='Cleaned Data')
        
        output_size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"Output file created: {output_size_mb:.2f} MB", file=sys.stderr)
        
        # Create download URL
        download_url = f"{RAILWAY_PUBLIC_URL}/download/{file_id}"
        
        print(f"Download URL: {download_url}", file=sys.stderr)
        print("=== Processing Complete ===", file=sys.stderr)
        
        # Return JSON with download URL
        return JSONResponse(content={
            "success": True,
            "download_url": download_url,
            "file_id": file_id,
            "original_filename": file.filename,
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
            "expires_in": "24 hours"
        })
        
    except Exception as e:
        print(f"ERROR: {str(e)}", file=sys.stderr)
        import traceback
        print(traceback.format_exc(), file=sys.stderr)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download/{file_id}")
async def download_file(file_id: str):
    """
    Download the processed Excel file by ID
    """
    print(f"Download request for file_id: {file_id}", file=sys.stderr)
    
    try:
        # Find the file
        file_path = TEMP_DIR / f"cleaned_{file_id}.xlsx"
        
        if not file_path.exists():
            print(f"File not found: {file_path}", file=sys.stderr)
            raise HTTPException(
                status_code=404,
                detail="File not found or expired. Files are kept for 24 hours."
            )
        
        print(f"Serving file: {file_path}", file=sys.stderr)
        
        # Return the file
        return FileResponse(
            path=file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"cleaned_{file_id}.xlsx",
            headers={
                "Content-Disposition": f"attachment; filename=cleaned_data.xlsx"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR serving file: {str(e)}", file=sys.stderr)
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/download/{file_id}")
async def delete_file(file_id: str):
    """
    Manually delete a processed file
    """
    try:
        file_path = TEMP_DIR / f"cleaned_{file_id}.xlsx"
        
        if file_path.exists():
            file_path.unlink()
            return {"success": True, "message": "File deleted"}
        else:
            raise HTTPException(status_code=404, detail="File not found")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Cleanup old files automatically
from apscheduler.schedulers.background import BackgroundScheduler
import time

def cleanup_old_files():
    """Remove files older than 24 hours"""
    print("Running cleanup task...", file=sys.stderr)
    current_time = time.time()
    deleted_count = 0
    
    for file_path in TEMP_DIR.glob("cleaned_*.xlsx"):
        file_age_hours = (current_time - file_path.stat().st_mtime) / 3600
        
        if file_age_hours > 24:  # 24 hours
            try:
                file_path.unlink()
                deleted_count += 1
                print(f"Deleted old file: {file_path.name}", file=sys.stderr)
            except Exception as e:
                print(f"Error deleting {file_path.name}: {e}", file=sys.stderr)
    
    print(f"Cleanup complete. Deleted {deleted_count} files.", file=sys.stderr)

# Schedule cleanup every 6 hours
scheduler = BackgroundScheduler()
scheduler.add_job(cleanup_old_files, 'interval', hours=6)
scheduler.start()

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "temp_dir": str(TEMP_DIR),
        "files_count": len(list(TEMP_DIR.glob("cleaned_*.xlsx")))
    }