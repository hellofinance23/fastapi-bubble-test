"""
Data Cleaning Module

This module contains the DataCleaner class which handles all data cleaning operations.
You can easily modify the cleaning rules in this class.
"""

import pandas as pd
import sys
from typing import Dict


class DataCleaner:
    """
    Cleans and processes pandas DataFrames.

    This class performs the following cleaning operations:
    1. Removes duplicate rows
    2. Removes empty rows (rows with all blank cells)
    3. Renames column headers by adding '_CHANGED' suffix
    4. Removes extra spaces from all cell values
    """

    def __init__(self):
        """Initialize the DataCleaner."""
        self.stats = {
            'original_rows': 0,
            'duplicates_removed': 0,
            'empty_rows_removed': 0,
            'final_rows': 0,
            'columns_count': 0
        }

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Performs all cleaning operations on the dataframe.

        Args:
            df: The pandas DataFrame to clean

        Returns:
            pd.DataFrame: The cleaned dataframe
        """
        print("Step 5: Cleaning data...", file=sys.stderr)

        # Store original statistics
        self.stats['original_rows'] = len(df)
        self.stats['columns_count'] = len(df.columns)
        print(f"Original: {self.stats['original_rows']:,} rows, {self.stats['columns_count']} columns",
              file=sys.stderr)

        # Perform cleaning operations
        df_cleaned = self._remove_duplicates(df)
        df_cleaned = self._remove_empty_rows(df_cleaned)
        df_cleaned = self._clean_column_names(df_cleaned)
        df_cleaned = self._clean_cell_values(df_cleaned)

        # Store final statistics
        self.stats['final_rows'] = len(df_cleaned)

        print(f"✓ Cleaning complete → {self.stats['final_rows']:,} final rows",
              file=sys.stderr)

        return df_cleaned

    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Removes duplicate rows from the dataframe.

        Args:
            df: The dataframe to clean

        Returns:
            pd.DataFrame: Dataframe with duplicates removed
        """
        print("  Removing duplicates...", file=sys.stderr)
        original_count = len(df)
        df_cleaned = df.drop_duplicates()
        self.stats['duplicates_removed'] = original_count - len(df_cleaned)
        print(f"  ✓ Removed {self.stats['duplicates_removed']:,} duplicate rows",
              file=sys.stderr)
        return df_cleaned

    def _remove_empty_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Removes rows where all cells are empty.

        Args:
            df: The dataframe to clean

        Returns:
            pd.DataFrame: Dataframe with empty rows removed
        """
        print("  Removing empty rows...", file=sys.stderr)
        original_count = len(df)
        df_cleaned = df.dropna(how='all')
        self.stats['empty_rows_removed'] = original_count - len(df_cleaned)
        print(f"  ✓ Removed {self.stats['empty_rows_removed']:,} empty rows",
              file=sys.stderr)
        return df_cleaned

    def _clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans column names by removing extra spaces and adding '_CHANGED' suffix.

        EASY TO MODIFY: Change this method to customize how column names are renamed.
        For example:
        - Change '_CHANGED' to a different suffix
        - Remove the suffix entirely
        - Apply different naming rules

        Args:
            df: The dataframe to clean

        Returns:
            pd.DataFrame: Dataframe with cleaned column names
        """
        print("  Cleaning column names...", file=sys.stderr)

        # Remove extra spaces from column names
        df.columns = df.columns.str.strip()

        # Add '_CHANGED' suffix to all columns
        # MODIFY THIS LINE to change how columns are renamed:
        df.columns = [f"{col}_CHANGED" for col in df.columns]

        print(f"  ✓ Renamed {len(df.columns)} columns", file=sys.stderr)
        return df

    def _clean_cell_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Removes extra whitespace from all cell values.

        EASY TO MODIFY: Change this method to customize cell cleaning.
        For example:
        - Convert text to uppercase or lowercase
        - Replace specific values
        - Format numbers or dates

        Args:
            df: The dataframe to clean

        Returns:
            pd.DataFrame: Dataframe with cleaned cell values
        """
        print("  Cleaning cell values...", file=sys.stderr)

        # Remove leading/trailing spaces from all text cells
        for col in df.columns:
            try:
                # MODIFY THIS LINE to change how cell values are cleaned:
                df[col] = df[col].str.strip()
            except:
                # Skip columns that don't contain text
                pass

        print(f"  ✓ Cleaned cell values", file=sys.stderr)
        return df

    def get_stats(self) -> Dict:
        """
        Returns statistics about the cleaning operation.

        Returns:
            Dict: Dictionary containing cleaning statistics
        """
        return self.stats.copy()
