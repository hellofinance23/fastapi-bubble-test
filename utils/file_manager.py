"""
File Manager Module

This module contains the FileManager class which handles temporary file storage,
saving cleaned files, and file cleanup operations.
"""

import pandas as pd
import uuid
import sys
import time
from pathlib import Path
from typing import Tuple


class FileManager:
    """
    Manages temporary files and file saving operations.

    This class handles:
    - Creating temporary directories
    - Saving temporary input files
    - Saving cleaned output files
    - Cleaning up old files
    """

    def __init__(self, temp_dir: Path):
        """
        Initialize the FileManager.

        Args:
            temp_dir: Path to the temporary directory
        """
        self.temp_dir = temp_dir

    def save_temp_input_file(self, file_content: bytes, filename: str) -> Path:
        """
        Saves uploaded file content to a temporary file.

        Args:
            file_content: The file content as bytes
            filename: Original filename

        Returns:
            Path: Path to the saved temporary file
        """
        print("Step 3: Saving to temporary file...", file=sys.stderr)

        temp_input_path = self.temp_dir / f"temp_input_{uuid.uuid4()}_{filename}"

        with open(temp_input_path, 'wb') as f:
            f.write(file_content)

        print(f"✓ Saved to: {temp_input_path.name}", file=sys.stderr)

        return temp_input_path

    def save_cleaned_file(self, df: pd.DataFrame) -> Tuple[str, Path, float]:
        """
        Saves the cleaned DataFrame as an Excel file.

        Args:
            df: The cleaned DataFrame to save

        Returns:
            tuple: (file_id, output_path, output_size_mb)
        """
        print("Step 6: Saving cleaned file as Excel...", file=sys.stderr)

        file_id = str(uuid.uuid4())
        output_filename = f"cleaned_{file_id}.xlsx"
        output_path = self.temp_dir / output_filename

        print(f"  Output: {output_filename}", file=sys.stderr)

        # Use xlsxwriter engine (faster for writing)
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Cleaned Data')

        output_size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"✓ Saved {output_size_mb:.2f} MB", file=sys.stderr)

        return file_id, output_path, output_size_mb

    def delete_temp_file(self, file_path: Path):
        """
        Deletes a temporary file.

        Args:
            file_path: Path to the file to delete
        """
        try:
            file_path.unlink()
            print("✓ Cleaned up temp input file", file=sys.stderr)
        except Exception as e:
            print(f"Warning: couldn't delete temp file: {e}", file=sys.stderr)

    def cleanup_old_files(self):
        """
        Removes files older than 24 hours.

        This method is called by the background scheduler.
        """
        print("Running cleanup task...", file=sys.stderr)
        current_time = time.time()
        deleted_count = 0

        # Clean up output files
        for file_path in self.temp_dir.glob("cleaned_*.xlsx"):
            file_age_hours = (current_time - file_path.stat().st_mtime) / 3600

            if file_age_hours > 24:
                try:
                    file_path.unlink()
                    deleted_count += 1
                    print(f"Deleted old file: {file_path.name}", file=sys.stderr)
                except Exception as e:
                    print(f"Error deleting {file_path.name}: {e}", file=sys.stderr)

        # Clean up temporary input files
        for pattern in ["temp_input_*.xlsx", "temp_input_*.csv"]:
            for file_path in self.temp_dir.glob(pattern):
                try:
                    file_path.unlink()
                    deleted_count += 1
                except:
                    pass

        if deleted_count > 0:
            print(f"Cleanup complete. Deleted {deleted_count} files.", file=sys.stderr)

    def get_storage_info(self) -> dict:
        """
        Gets information about current storage usage.

        Returns:
            dict: Storage information including file count and sizes
        """
        files = list(self.temp_dir.glob("cleaned_*.xlsx"))
        total_size = sum(f.stat().st_size for f in files)
        current_time = time.time()

        return {
            "files_count": len(files),
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "files": [
                {
                    "name": f.name,
                    "size_mb": round(f.stat().st_size / (1024 * 1024), 2),
                    "age_hours": round((current_time - f.stat().st_mtime) / 3600, 1)
                }
                for f in files
            ]
        }

    def get_file_path(self, file_id: str) -> Path:
        """
        Gets the path to a cleaned file by its ID.

        Args:
            file_id: The file ID

        Returns:
            Path: Path to the file
        """
        return self.temp_dir / f"cleaned_{file_id}.xlsx"
