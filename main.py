"""
FastAPI Excel/CSV Processor - Main Application

This is the main entry point for the application.
The code has been refactored into separate modules for better organization:

- validations/: File validation logic
- cleaning/: Data cleaning operations
- utils/: File loading and management utilities
- routes/: API endpoint definitions
"""

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import os
from pathlib import Path
import tempfile

from routes.file_routes import create_routes
from utils.file_manager import FileManager


# Initialize FastAPI app
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

# Initialize file manager
file_manager = FileManager(TEMP_DIR)

# Setup background scheduler for file cleanup
scheduler = BackgroundScheduler()
scheduler.add_job(file_manager.cleanup_old_files, 'interval', hours=6)
scheduler.start()

# Register routes
router = create_routes(file_manager, RAILWAY_PUBLIC_URL)
app.include_router(router)


# Basic endpoints
@app.get("/")
def index():
    """
    API information endpoint.
    """
    return {
        "message": "Excel/CSV Processor API",
        "version": "4.0",
        "supported_formats": ["CSV", "Excel (.xlsx, .xls, .xlsb)"],
        "architecture": "Modular"
    }


@app.get("/health")
def health():
    """
    Health check endpoint with API information.
    """
    return {
        "status": "healthy",
        "version": "4.0",
        "architecture": "Modular - Separated into validations, cleaning, utils, and routes",
        "supported_formats": ["CSV", "Excel (.xlsx, .xls, .xlsb)"],
        "endpoints": {
            "/process-file-from-url": "POST - Process CSV or Excel file",
            "/process-excel-from-url": "POST - Legacy endpoint (same as above)",
            "/download/{file_id}": "GET - Download processed file",
            "/storage-info": "GET - View storage usage"
        },
        "temp_dir": str(TEMP_DIR),
        "files_count": len(list(TEMP_DIR.glob("cleaned_*.xlsx")))
    }
