"""
Unit tests for the validate module.
"""

import tempfile
from pathlib import Path
from unittest.mock import patch
import pytest
import polars as pl

from validate import validate_df, validate, ValidationReport


class TestValidateDF:
    """Tests for the validate_df function."""

    def test_validate_df_valid_dataframe_with_print_output(self, capsys):
        """
        Test that validate_df correctly validates a valid DataFrame
        and returns a valid report when print_output is True.
        """
        # Create a valid DataFrame with proper ra and dec columns
        df = pl.DataFrame({
            "ra_j2000": [10.0, 20.0, 30.0],
            "dec_j2000": [45.0, -30.0, 0.0],
            "flux_density_mjy": [1.5, 2.3, 3.1]
        })

        report = validate_df(df, "test_table", print_output=True)

        # Check that the report indicates valid data and column names
        assert isinstance(report, ValidationReport)
        assert report.valid_data is True
        assert report.valid_column_names is True

        # Verify that output was printed
        captured = capsys.readouterr()
        assert "Table Data Validation Report" in captured.out
        assert "Column Name Validation Report" in captured.out

    def test_validate_df_invalid_column_names_with_print_output(self, capsys):
        """
        Test that validate_df correctly identifies invalid column names
        in a DataFrame and returns an invalid report when print_output is True.
        """
        # Create a DataFrame with invalid column names
        df = pl.DataFrame({
            "ra_j2000": [10.0, 20.0, 30.0],
            "dec_j2000": [45.0, -30.0, 0.0],
            "fred": [1.5, 2.3, 3.1],  # 'fred' is in NOT_ALLOWED list
            "column.with.dots": [4.0, 5.0, 6.0]  # dots are not allowed
        })

        report = validate_df(df, "test_table", print_output=True)

        # Check that the report indicates invalid column names
        assert isinstance(report, ValidationReport)
        assert report.valid_column_names is False

        # Verify that output was printed and shows column name issues
        captured = capsys.readouterr()
        assert "Table Data Validation Report" in captured.out
        assert "Column Name Validation Report" in captured.out

    def test_validate_df_without_print_output(self, capsys):
        """
        Test that validate_df returns a report without printing output
        when print_output is False.
        """
        # Create a valid DataFrame
        df = pl.DataFrame({
            "ra_j2000": [10.0, 20.0, 30.0],
            "dec_j2000": [45.0, -30.0, 0.0],
            "flux_density_mjy": [1.5, 2.3, 3.1]
        })

        report = validate_df(df, "test_table", print_output=False)

        # Check that a report is returned
        assert isinstance(report, ValidationReport)

        # Verify that no output was printed
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_validate_df_invalid_data_values(self, capsys):
        """
        Test that validate_df detects invalid data values (e.g., ra out of range).
        """
        # Create a DataFrame with invalid ra values
        df = pl.DataFrame({
            "ra_j2000": [10.0, 400.0, 30.0],  # 400.0 is out of range [0, 360)
            "dec_j2000": [45.0, -30.0, 0.0],
            "flux_density_mjy": [1.5, 2.3, 3.1]
        })

        report = validate_df(df, "test_table", print_output=True)

        # Check that the report indicates invalid data
        assert isinstance(report, ValidationReport)
        assert report.valid_data is False

        # Verify that output shows the failure
        captured = capsys.readouterr()
        assert "INVALID" in captured.out or "FAIL" in captured.out

    def test_validate_df_detects_minus_999_values(self, capsys):
        """
        Test that validate_df detects -999 values in the DataFrame.
        """
        # Create a DataFrame with -999 values
        df = pl.DataFrame({
            "ra_j2000": [10.0, 20.0, 30.0],
            "dec_j2000": [45.0, -30.0, 0.0],
            "flux_density_mjy": [1.5, -999, 3.1]  # -999 is not allowed
        })

        report = validate_df(df, "test_table", print_output=True)

        # Check that the report indicates invalid data
        assert isinstance(report, ValidationReport)
        assert report.valid_data is False

        # Verify that output shows the -999 issue
        captured = capsys.readouterr()
        assert "flux_density_mjy" in captured.out
        assert "-999" in captured.out


class TestValidate:
    """Tests for the validate function."""

    def test_validate_reads_parquet_and_validates(self, capsys, tmp_path):
        """
        Test that the validate function successfully reads a parquet file
        and delegates validation to validate_df.
        """
        # Create a temporary parquet file
        test_file = tmp_path / "test_data.parquet"
        df = pl.DataFrame({
            "ra_j2000": [10.0, 20.0, 30.0],
            "dec_j2000": [45.0, -30.0, 0.0],
            "flux_density_mjy": [1.5, 2.3, 3.1]
        })
        df.write_parquet(test_file)

        # Run validation
        report = validate(str(test_file), print_output=True)

        # Check that validation succeeded
        assert isinstance(report, ValidationReport)
        assert report.valid_data is True
        assert report.valid_column_names is True

        # Verify that output was printed
        captured = capsys.readouterr()
        assert "Table Data Validation Report" in captured.out
        assert "Column Name Validation Report" in captured.out

    def test_validate_handles_nonexistent_file_gracefully(self):
        """
        Test that the validate function handles non-existent parquet files gracefully.
        """
        # Use a path that doesn't exist
        nonexistent_file = "/path/that/does/not/exist/data.parquet"

        # Expect an exception to be raised
        with pytest.raises(Exception):
            validate(nonexistent_file, print_output=False)

    def test_validate_without_print_output(self, capsys, tmp_path):
        """
        Test that the validate function works correctly with print_output=False.
        """
        # Create a temporary parquet file
        test_file = tmp_path / "test_data.parquet"
        df = pl.DataFrame({
            "ra_j2000": [10.0, 20.0, 30.0],
            "dec_j2000": [45.0, -30.0, 0.0],
            "flux_density_mjy": [1.5, 2.3, 3.1]
        })
        df.write_parquet(test_file)

        # Run validation without printing
        report = validate(str(test_file), print_output=False)

        # Check that validation succeeded
        assert isinstance(report, ValidationReport)
        assert report.valid_data is True
        assert report.valid_column_names is True

        # Verify that no output was printed
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_validate_invalid_parquet_file(self, capsys, tmp_path):
        """
        Test that the validate function correctly identifies invalid data in a parquet file.
        """
        # Create a temporary parquet file with invalid data
        test_file = tmp_path / "invalid_data.parquet"
        df = pl.DataFrame({
            "ra_j2000": [10.0, 500.0, 30.0],  # Invalid ra value
            "dec_j2000": [45.0, -30.0, 0.0],
            "bob": [1.5, 2.3, 3.1]  # Invalid column name
        })
        df.write_parquet(test_file)

        # Run validation
        report = validate(str(test_file), print_output=True)

        # Check that validation failed
        assert isinstance(report, ValidationReport)
        assert report.valid_data is False or report.valid_column_names is False

        # Verify that output shows issues
        captured = capsys.readouterr()
        assert len(captured.out) > 0
