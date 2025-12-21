"""
File Validation Module

This module contains the FileValidator class which handles all file validation tasks.
It checks if files are the correct type and format before processing.
"""

from fastapi import HTTPException
from typing import Dict


class FileValidator:
    """
    Validates files before processing.

    This class checks that:
    - The file URL is provided
    - The file has a valid extension (CSV or Excel)
    """

    # Supported file extensions
    CSV_EXTENSIONS = ('.csv',)
    EXCEL_EXTENSIONS = ('.xlsx', '.xls', '.xlsb')
    ALL_SUPPORTED_EXTENSIONS = CSV_EXTENSIONS + EXCEL_EXTENSIONS

    def validate_request_data(self, data: Dict) -> tuple:
        """
        Validates the incoming request data.

        Args:
            data: Dictionary containing 'file_url' and 'filename'

        Returns:
            tuple: (file_url, filename)

        Raises:
            HTTPException: If file_url is missing
        """
        file_url = data.get("file_url")
        filename = data.get("filename", "file.xlsx")

        if not file_url:
            raise HTTPException(
                status_code=400,
                detail="file_url is required in request body"
            )

        return file_url, filename

    def validate_file_type(self, filename: str) -> str:
        """
        Validates that the file has a supported extension.

        Args:
            filename: The name of the file to validate

        Returns:
            str: "CSV" or "Excel" indicating the file type

        Raises:
            HTTPException: If file type is not supported
        """
        filename_lower = filename.lower()

        is_csv = filename_lower.endswith(self.CSV_EXTENSIONS)
        is_excel = filename_lower.endswith(self.EXCEL_EXTENSIONS)

        if not (is_csv or is_excel):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type. Must be one of: {', '.join(self.ALL_SUPPORTED_EXTENSIONS)}"
            )

        return "CSV" if is_csv else "Excel"

    def is_csv_file(self, filename: str) -> bool:
        """
        Checks if a file is a CSV file.

        Args:
            filename: The name of the file to check

        Returns:
            bool: True if CSV, False otherwise
        """
        return filename.lower().endswith(self.CSV_EXTENSIONS)

    def is_excel_file(self, filename: str) -> bool:
        """
        Checks if a file is an Excel file.

        Args:
            filename: The name of the file to check

        Returns:
            bool: True if Excel, False otherwise
        """
        return filename.lower().endswith(self.EXCEL_EXTENSIONS)
