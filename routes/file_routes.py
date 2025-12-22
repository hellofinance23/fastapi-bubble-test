"""
File Processing Routes

This module contains all the API endpoints for file processing.
"""

from fastapi import APIRouter, HTTPException, Body
from fastapi.responses import FileResponse, JSONResponse
from typing import Dict
import sys
import time
import gc

from validations.file_validator import FileValidator
from utils.file_loader import FileLoader
from cleaning.data_cleaner import DataCleaner
from utils.file_manager import FileManager


router = APIRouter()


class FileProcessor:
    """
    Main file processing coordinator.

    This class orchestrates the entire file processing workflow by using:
    - FileValidator: To validate input
    - FileLoader: To download and load files
    - DataCleaner: To clean the data
    - FileManager: To save and manage files
    """

    def __init__(self, file_manager: FileManager, railway_public_url: str):
        """
        Initialize the FileProcessor.

        Args:
            file_manager: FileManager instance
            railway_public_url: Base URL for download links
        """
        self.validator = FileValidator()
        self.loader = FileLoader()
        self.cleaner = DataCleaner()
        self.file_manager = file_manager
        self.railway_public_url = railway_public_url

    def process_file(self, data: Dict) -> JSONResponse:
        """
        Main processing method that coordinates the entire workflow.

        Args:
            data: Request data containing file_url and filename

        Returns:
            JSONResponse: Result with download URL and statistics
        """
        print("=" * 50, file=sys.stderr)
        print("Processing File from URL (CSV/Excel)", file=sys.stderr)
        print("=" * 50, file=sys.stderr)

        temp_input_path = None

        try:
            # Step 1: Validate request
            file_url, filename = self.validator.validate_request_data(data)

            print(f"File URL: {file_url}", file=sys.stderr)
            print(f"Filename: {filename}", file=sys.stderr)

            # Step 2: Download file
            download_start = time.time()
            file_content = self.loader.download_file(file_url)
            download_time = time.time() - download_start

            file_size_mb = len(file_content) / (1024 * 1024)
            print(f"  Download time: {download_time:.1f}s", file=sys.stderr)

            # Step 3: Validate file type
            print("Step 2: Validating file type...", file=sys.stderr)
            file_type = self.validator.validate_file_type(filename)
            is_csv = self.validator.is_csv_file(filename)
            print(f"✓ File type validated: {file_type}", file=sys.stderr)

            # Step 4: Save to temp
            temp_input_path = self.file_manager.save_temp_input_file(file_content, filename)

            # Free memory
            del file_content
            gc.collect()

            # Step 5: Load file
            load_start = time.time()
            df, engine_used = self.loader.load_file(temp_input_path, filename, is_csv)
            load_time = time.time() - load_start

            print(f"  Load time: {load_time:.1f}s", file=sys.stderr)
            if load_time > 0:
                print(f"  Speed: {file_size_mb/load_time:.1f} MB/s", file=sys.stderr)

            # Step 6: Clean data
            clean_start = time.time()
            df_cleaned = self.cleaner.clean_data(df)
            clean_time = time.time() - clean_start

            print(f"  Clean time: {clean_time:.1f}s", file=sys.stderr)

            # Free memory
            del df
            gc.collect()

            # Step 7: Save cleaned file
            save_start = time.time()
            file_id, output_path, output_size_mb = self.file_manager.save_cleaned_file(df_cleaned)
            save_time = time.time() - save_start

            print(f"  Save time: {save_time:.1f}s", file=sys.stderr)

            # Free memory
            del df_cleaned
            gc.collect()

            # Step 8: Create download URL
            download_url = f"{self.railway_public_url}/download/{file_id}"
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

            # Get cleaning statistics
            stats = self.cleaner.get_stats()

            # Return success response
            return JSONResponse(content={
                "success": True,
                "download_url": download_url,
                "file_id": file_id,
                "original_filename": filename,
                "processed_filename": f"cleaned_{file_id}.xlsx",
                "input_format": file_type,
                "stats": {
                    "original_rows": stats['original_rows'],
                    "final_rows": stats['final_rows'],
                    "duplicates_removed": stats['duplicates_removed'],
                    "empty_rows_removed": stats['empty_rows_removed'],
                    "original_columns": stats['columns_count'],
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

        finally:
            # Always clean up temp file
            if temp_input_path:
                self.file_manager.delete_temp_file(temp_input_path)


def create_routes(file_manager: FileManager, railway_public_url: str) -> APIRouter:
    """
    Creates and configures the API routes.

    Args:
        file_manager: FileManager instance
        railway_public_url: Base URL for the service

    Returns:
        APIRouter: Configured router with all endpoints
    """
    processor = FileProcessor(file_manager, railway_public_url)

    @router.post("/process-file-from-url")
    async def process_file_from_url(data: Dict = Body(...)):
        """
        Process CSV or Excel file from a URL.

        Expected JSON body:
        {
            "file_url": "https://...",
            "filename": "data.xlsx" or "data.csv"
        }
        """
        return processor.process_file(data)

    @router.post("/process-excel-from-url")
    async def process_excel_from_url(data: Dict = Body(...)):
        """
        Legacy endpoint - redirects to new endpoint.
        """
        return await process_file_from_url(data)

    @router.get("/download/{file_id}")
    async def download_file(file_id: str):
        """
        Download the processed Excel file by ID.
        """
        print(f"Download request for file_id: {file_id}", file=sys.stderr)

        try:
            file_path = file_manager.get_file_path(file_id)

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

    @router.post("/preview-file-from-url")
    async def preview_file_from_url(data: Dict = Body(...)):
        """
        Preview first 20 rows of a CSV or Excel file from a URL.

        Returns the data without processing/cleaning it.

        Expected JSON body:
        {
            "file_url": "https://...",
            "filename": "data.xlsx" or "data.csv"
        }
        """
        print("=" * 50, file=sys.stderr)
        print("Previewing File from URL", file=sys.stderr)
        print("=" * 50, file=sys.stderr)

        temp_input_path = None

        try:
            # Step 1: Validate request
            file_url, filename = processor.validator.validate_request_data(data)

            print(f"File URL: {file_url}", file=sys.stderr)
            print(f"Filename: {filename}", file=sys.stderr)

            # Step 2: Download file
            file_content = processor.loader.download_file(file_url)

            # Step 3: Validate file type
            file_type = processor.validator.validate_file_type(filename)
            is_csv = processor.validator.is_csv_file(filename)
            print(f"✓ File type validated: {file_type}", file=sys.stderr)

            # Step 4: Save to temp
            temp_input_path = file_manager.save_temp_input_file(file_content, filename)

            # Free memory
            del file_content
            gc.collect()

            # Step 5: Load file
            df, engine_used = processor.loader.load_file(temp_input_path, filename, is_csv)

            total_rows = len(df)
            total_columns = len(df.columns)
            print(f"✓ Loaded {total_rows:,} rows × {total_columns} cols", file=sys.stderr)

            # Get first 5 rows and 5 columns
            max_rows = 5
            max_cols = 5
            preview_rows = min(max_rows, total_rows)
            preview_cols = min(max_cols, total_columns)

            # Limit to first 5 columns and 5 rows
            df_preview = df.iloc[:max_rows, :max_cols]

            # Helper function to format cell values
            def format_cell_value(value):
                """Format cell values: round numbers, brackets for negatives, dashes for zeros"""
                if value is None or (isinstance(value, str) and value.strip() == ''):
                    return ''

                # Try to convert to number
                try:
                    num = float(str(value).replace(',', ''))

                    # Round to nearest whole number
                    rounded = round(num)

                    # Zero as dash
                    if rounded == 0:
                        return '-'

                    # Negative numbers with brackets
                    if rounded < 0:
                        return f"({abs(rounded):,.0f})"

                    # Positive numbers with comma formatting
                    return f"{rounded:,.0f}"
                except (ValueError, TypeError):
                    # Not a number, return as-is
                    return str(value)

            # Generate HTML table
            html = "<table border='1' style='border-collapse:collapse;width:100%'>"

            # Header
            html += "<thead><tr>"
            for col in df_preview.columns:
                html += f"<th style='padding:6px;background:#f4f4f4'>{col}</th>"
            html += "</tr></thead>"

            # Body
            html += "<tbody>"
            for _, row in df_preview.iterrows():
                html += "<tr>"
                for value in row:
                    formatted_value = format_cell_value(value)
                    html += f"<td style='padding:6px'>{formatted_value}</td>"
                html += "</tr>"
            html += "</tbody></table>"

            print(f"✓ Returning HTML table preview of {preview_rows} rows × {preview_cols} cols", file=sys.stderr)
            print("=" * 50, file=sys.stderr)

            # Free memory
            del df
            del df_preview
            gc.collect()

            # Return JSON response with HTML table
            return JSONResponse(content={
                "success": True,
                "filename": filename,
                "file_type": file_type,
                "total_rows": total_rows,
                "total_columns": total_columns,
                "preview_rows": preview_rows,
                "html_table": html,
                "engine_used": engine_used
            })

        except HTTPException:
            raise
        except Exception as e:
            print(f"ERROR previewing file: {str(e)}", file=sys.stderr)
            import traceback
            print(traceback.format_exc(), file=sys.stderr)
            raise HTTPException(
                status_code=500,
                detail=f"Preview error: {str(e)}"
            )

        finally:
            # Always clean up temp file
            if temp_input_path:
                file_manager.delete_temp_file(temp_input_path)

    @router.get("/storage-info")
    def storage_info():
        """
        Check storage usage and list all files.
        """
        return file_manager.get_storage_info()

    return router
