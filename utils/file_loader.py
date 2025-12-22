"""
File Loading Module

This module contains the FileLoader class which handles downloading and loading files.
It supports CSV and Excel files with automatic encoding detection for CSV files.
"""

import pandas as pd
import requests
import chardet
import sys
from pathlib import Path
from fastapi import HTTPException
from typing import Tuple


class FileLoader:
    """
    Downloads and loads CSV and Excel files into pandas DataFrames.

    This class handles:
    - Downloading files from URLs
    - Detecting CSV file encoding automatically
    - Loading different Excel formats (.xlsx, .xls, .xlsb)
    """

    def download_file(self, file_url: str) -> bytes:
        """
        Downloads a file from a URL.

        Args:
            file_url: The URL to download from

        Returns:
            bytes: The file content

        Raises:
            HTTPException: If download fails
        """
        print("Step 1: Downloading file...", file=sys.stderr)

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
        print(f"✓ Downloaded {file_size_mb:.2f} MB", file=sys.stderr)

        return file_content

    def load_csv_file(self, file_path: Path) -> Tuple[pd.DataFrame, str]:
        """
        Loads a CSV file with automatic encoding detection.

        This method tries multiple encodings to ensure the CSV loads correctly.

        Args:
            file_path: Path to the CSV file

        Returns:
            tuple: (DataFrame, encoding_used)

        Raises:
            HTTPException: If CSV cannot be loaded with any encoding
        """
        print("  Detecting CSV encoding...", file=sys.stderr)

        # Read a sample to detect encoding
        with open(file_path, 'rb') as f:
            raw_data = f.read(100000)  # Read first 100KB
            detection = chardet.detect(raw_data)
            detected_encoding = detection['encoding']
            confidence = detection['confidence']

        print(f"  Detected: {detected_encoding} (confidence: {confidence:.0%})",
              file=sys.stderr)

        # Try multiple encodings in order
        encodings_to_try = [
            detected_encoding,
            'utf-8',
            'latin-1',
            'iso-8859-1',
            'cp1252',
            'windows-1252',
            'utf-16'
        ]

        # Remove None and duplicates
        encodings_to_try = list(dict.fromkeys([e for e in encodings_to_try if e]))

        df = None
        successful_encoding = None
        last_error = None

        # Try each encoding until one works
        for encoding in encodings_to_try:
            try:
                print(f"  Trying: {encoding}...", file=sys.stderr)
                df = pd.read_csv(
                    file_path,
                    dtype=str,
                    encoding=encoding,
                    low_memory=False,
                    on_bad_lines='skip',
                    encoding_errors='replace',
                    skip_blank_lines=True  # Skip empty rows before loading
                )
                successful_encoding = encoding
                print(f"  ✓ Success with {encoding}", file=sys.stderr)
                break
            except Exception as e:
                last_error = str(e)
                print(f"  ✗ Failed: {str(e)[:80]}", file=sys.stderr)
                continue

        if df is None:
            raise HTTPException(
                status_code=400,
                detail=f"Could not decode CSV. Detected: {detected_encoding}, Error: {last_error}"
            )

        return df, f"CSV ({successful_encoding})"

    def load_excel_file(self, file_path: Path, filename: str) -> Tuple[pd.DataFrame, str]:
        """
        Loads an Excel file using the appropriate engine.

        Supports:
        - .xlsx files (uses openpyxl)
        - .xls files (uses xlrd)
        - .xlsb files (uses pyxlsb)

        Automatically skips blank rows at the top to find correct headers.

        Args:
            file_path: Path to the Excel file
            filename: Original filename to determine format

        Returns:
            tuple: (DataFrame, engine_description)

        Raises:
            HTTPException: If Excel file cannot be loaded
        """
        filename_lower = filename.lower()

        # First, read without header to detect blank rows
        if filename_lower.endswith('.xlsx'):
            print("  Using openpyxl engine...", file=sys.stderr)
            engine = 'openpyxl'
            engine_used = "Excel (openpyxl)"
        elif filename_lower.endswith('.xls'):
            print("  Using xlrd engine...", file=sys.stderr)
            engine = 'xlrd'
            engine_used = "Excel (xlrd)"
        elif filename_lower.endswith('.xlsb'):
            print("  Using pyxlsb engine...", file=sys.stderr)
            engine = 'pyxlsb'
            engine_used = "Excel (pyxlsb)"
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported Excel format: {filename}"
            )

        # Read first few rows to find where data starts
        df_preview = pd.read_excel(
            file_path,
            engine=engine,
            dtype=str,
            nrows=100,  # Read first 100 rows to find header
            header=None  # Don't treat first row as header yet
        )

        # Find first non-empty row (where actual headers are)
        skip_rows = 0
        for idx, row in df_preview.iterrows():
            # Check if row is completely empty (all NaN or all empty strings)
            if row.isna().all() or (row.astype(str).str.strip() == '').all():
                skip_rows += 1
            else:
                # Found first non-empty row - this is the header
                break

        if skip_rows > 0:
            print(f"  Skipping {skip_rows} blank rows at top", file=sys.stderr)

        # Now read the file properly, skipping blank rows
        df = pd.read_excel(
            file_path,
            engine=engine,
            dtype=str,
            skiprows=skip_rows  # Skip blank rows before header
        )

        return df, engine_used

    def load_file(self, file_path: Path, filename: str, is_csv: bool) -> Tuple[pd.DataFrame, str]:
        """
        Loads either a CSV or Excel file into a DataFrame.

        Args:
            file_path: Path to the file
            filename: Original filename
            is_csv: True if CSV, False if Excel

        Returns:
            tuple: (DataFrame, engine_description)

        Raises:
            HTTPException: If file cannot be loaded
        """
        print(f"Step 4: Loading {'CSV' if is_csv else 'Excel'}...", file=sys.stderr)

        try:
            if is_csv:
                df, engine_used = self.load_csv_file(file_path)
            else:
                df, engine_used = self.load_excel_file(file_path, filename)

            print(f"✓ Loaded {len(df):,} rows × {len(df.columns)} cols", file=sys.stderr)
            print(f"  Engine: {engine_used}", file=sys.stderr)

            return df, engine_used

        except HTTPException:
            raise
        except Exception as e:
            print(f"ERROR loading file: {str(e)}", file=sys.stderr)
            import traceback
            print(traceback.format_exc(), file=sys.stderr)
            raise HTTPException(
                status_code=400,
                detail=f"Failed to parse file: {str(e)}"
            )
