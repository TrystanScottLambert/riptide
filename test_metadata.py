"""
Unit tests for the metadata module.
"""

import pytest
import polars as pl
from unittest.mock import patch, Mock
import json

from metadata import (
    Columns,
    ColumnMetaData,
    MinMax,
    _scrape_cds_ucd,
    fields_from_df,
    guess_ucd,
)


class TestColumns:
    """Tests for the Columns class."""

    def test_columns_initialization_stores_metadata_in_dict(self):
        """
        Test that the Columns class correctly initializes and stores
        column metadata in a dictionary.
        """
        # Create sample ColumnMetaData objects
        col1 = ColumnMetaData(
            name="ra_j2000",
            ucd="pos.eq.ra;meta.main",
            data_type="float64",
            qc=MinMax(min=0.0, max=360.0),
            unit="deg",
        )
        col2 = ColumnMetaData(
            name="dec_j2000",
            ucd="pos.eq.dec;meta.main",
            data_type="float64",
            qc=MinMax(min=-90.0, max=90.0),
            unit="deg",
        )
        col3 = ColumnMetaData(
            name="object_id",
            ucd="meta.id;meta.main",
            data_type="string",
            qc=None,
        )

        # Initialize Columns with a list
        columns = Columns(columns=[col1, col2, col3])

        # Verify that columns are stored as a dictionary
        assert isinstance(columns.columns, dict)
        assert len(columns.columns) == 3

        # Verify that the dictionary keys are column names
        assert "ra_j2000" in columns.columns
        assert "dec_j2000" in columns.columns
        assert "object_id" in columns.columns

        # Verify that the values are ColumnMetaData objects
        assert columns.columns["ra_j2000"] == col1
        assert columns.columns["dec_j2000"] == col2
        assert columns.columns["object_id"] == col3

    def test_columns_initialization_with_empty_list(self):
        """Test that Columns can be initialized with an empty list."""
        columns = Columns(columns=[])

        assert isinstance(columns.columns, dict)
        assert len(columns.columns) == 0

    def test_set_info_updates_column_info(self):
        """
        Test that the set_info method correctly updates information
        for a given column.
        """
        col = ColumnMetaData(
            name="flux_density",
            ucd="phot.flux.density",
            data_type="float64",
            qc=MinMax(min=0.0, max=100.0),
            info=None,
        )
        columns = Columns(columns=[col])

        # Set info for the column
        columns.set_info("flux_density", "Measured flux density in mJy")

        # Verify that info was updated
        assert columns.columns["flux_density"].info == "Measured flux density in mJy"

    def test_set_info_raises_error_for_nonexistent_column(self):
        """
        Test that set_info raises a ValueError when the column doesn't exist.
        """
        col = ColumnMetaData(
            name="flux_density",
            ucd="phot.flux.density",
            data_type="float64",
            qc=MinMax(min=0.0, max=100.0),
        )
        columns = Columns(columns=[col])

        # Attempt to set info for a non-existent column
        with pytest.raises(ValueError) as exc_info:
            columns.set_info("nonexistent_column", "Some info")

        assert "No column with the name 'nonexistent_column' found" in str(
            exc_info.value
        )

    def test_get_info_retrieves_column_info(self):
        """
        Test that the get_info method correctly retrieves information
        for a given column.
        """
        col = ColumnMetaData(
            name="redshift",
            ucd="src.redshift",
            data_type="float64",
            qc=MinMax(min=0.0, max=10.0),
            info="Spectroscopic redshift",
        )
        columns = Columns(columns=[col])

        # Retrieve info
        info = columns.get_info("redshift")

        assert info == "Spectroscopic redshift"

    def test_get_info_returns_none_when_info_is_none(self):
        """
        Test that get_info returns None when the column has no info.
        """
        col = ColumnMetaData(
            name="magnitude",
            ucd="phot.mag",
            data_type="float64",
            qc=MinMax(min=10.0, max=30.0),
            info=None,
        )
        columns = Columns(columns=[col])

        # Retrieve info
        info = columns.get_info("magnitude")

        assert info is None

    def test_get_info_raises_error_for_nonexistent_column(self):
        """
        Test that get_info raises a ValueError when the column doesn't exist.
        """
        col = ColumnMetaData(
            name="magnitude",
            ucd="phot.mag",
            data_type="float64",
            qc=MinMax(min=10.0, max=30.0),
        )
        columns = Columns(columns=[col])

        # Attempt to get info for a non-existent column
        with pytest.raises(ValueError) as exc_info:
            columns.get_info("nonexistent_column")

        assert "No column with the name 'nonexistent_column' found" in str(
            exc_info.value
        )

    def test_set_minmax_updates_numeric_column(self):
        """
        Test that set_minmax correctly sets min/max for numeric columns.
        """
        col = ColumnMetaData(
            name="temperature",
            ucd="phys.temperature",
            data_type="float64",
            qc=None,
        )
        columns = Columns(columns=[col])

        # Set min/max
        columns.set_minmax("temperature", 100.0, 500.0)

        # Verify that min/max was set
        assert columns.columns["temperature"].qc == MinMax(min=100.0, max=500.0)

    def test_set_minmax_raises_error_for_string_column(self):
        """
        Test that set_minmax raises a ValueError for non-numeric (string) types.
        """
        col = ColumnMetaData(
            name="object_name",
            ucd="meta.id",
            data_type="string",
            qc=None,
        )
        columns = Columns(columns=[col])

        # Attempt to set min/max for a string column
        with pytest.raises(ValueError) as exc_info:
            columns.set_minmax("object_name", 0.0, 100.0)

        assert "Cannot set the min max of a 'string' type column" in str(exc_info.value)

    def test_set_minmax_for_integer_column(self):
        """
        Test that set_minmax works correctly for integer data types.
        """
        col = ColumnMetaData(
            name="count",
            ucd="meta.number",
            data_type="int64",
            qc=None,
        )
        columns = Columns(columns=[col])

        # Set min/max
        columns.set_minmax("count", 0.0, 1000.0)

        # Verify that min/max was set
        assert columns.columns["count"].qc == MinMax(min=0.0, max=1000.0)

    def test_set_minmax_raises_error_for_nonexistent_column(self):
        """
        Test that set_minmax raises a KeyError when the column doesn't exist.
        """
        col = ColumnMetaData(
            name="flux",
            ucd="phot.flux",
            data_type="float64",
            qc=None,
        )
        columns = Columns(columns=[col])

        # The code will raise a KeyError when trying to access a nonexistent column
        # because it checks the data_type first, which requires accessing columns dict
        with pytest.raises(KeyError):
            columns.set_minmax("nonexistent_column", 0.0, 100.0)


class TestScrapeCdsUcd:
    """Tests for the _scrape_cds_ucd function."""

    @patch("metadata.httpx.get")
    def test_scrape_cds_ucd_returns_ucd_for_valid_input(self, mock_get):
        """
        Test that _scrape_cds_ucd successfully retrieves a UCD from the
        external service for a valid input.
        """
        # Mock the HTTP response
        mock_response = Mock()
        mock_response.text = json.dumps(
            {"ucd": [{"ucd": "pos.eq.ra", "description": "Right ascension"}]}
        )
        mock_get.return_value = mock_response

        # Call the function
        result = _scrape_cds_ucd("ra_j2000")

        # Verify the result
        assert result == "pos.eq.ra"

        # Verify that httpx.get was called with the correct URL
        mock_get.assert_called_once()
        call_args = mock_get.call_args[0][0]
        assert "cdsweb.u-strasbg.fr/UCD/ucd-finder/suggest?d=" in call_args
        assert "ra j2000" in call_args  # underscores should be replaced with spaces

    @patch("metadata.httpx.get")
    def test_scrape_cds_ucd_sanitizes_column_name(self, mock_get):
        """
        Test that _scrape_cds_ucd properly sanitizes column names by
        replacing special characters with spaces.
        """
        mock_response = Mock()
        mock_response.text = json.dumps({"ucd": [{"ucd": "phot.flux.density"}]})
        mock_get.return_value = mock_response

        # Call with a column name containing special characters
        result = _scrape_cds_ucd("flux-density_mJy.total")

        # Verify that httpx.get was called with sanitized input
        call_args = mock_get.call_args[0][0]
        assert "flux density mJy total" in call_args

    @patch("metadata.httpx.get")
    def test_scrape_cds_ucd_returns_none_when_no_results(self, mock_get):
        """
        Test that _scrape_cds_ucd returns None when the external service
        returns no results.
        """
        # Mock the HTTP response with empty results
        mock_response = Mock()
        mock_response.text = json.dumps({"ucd": []})
        mock_get.return_value = mock_response

        # Call the function
        result = _scrape_cds_ucd("unknown_column_name")

        # Verify that None is returned
        assert result is None

    @patch("metadata.httpx.get")
    def test_scrape_cds_ucd_handles_missing_ucd_key(self, mock_get):
        """
        Test that _scrape_cds_ucd handles responses that don't contain a 'ucd' key.
        """
        # Mock the HTTP response without 'ucd' key
        mock_response = Mock()
        mock_response.text = json.dumps({"error": "Invalid query"})
        mock_get.return_value = mock_response

        # Call the function - should handle KeyError gracefully
        with pytest.raises(KeyError):
            _scrape_cds_ucd("invalid")


class TestFieldsFromDf:
    """Tests for the fields_from_df function."""

    @patch("metadata.guess_ucd")
    def test_fields_from_df_assigns_ucds_and_handles_minmax(self, mock_guess_ucd):
        """
        Test that fields_from_df correctly assigns UCDs using guess_ucd
        and handles MinMax for different data types.
        """
        # Mock guess_ucd to return specific UCDs
        mock_guess_ucd.side_effect = [
            "pos.eq.ra",
            "pos.eq.dec",
            "phot.flux.density",
            "meta.id",
        ]

        # Create a test DataFrame with mixed types
        df = pl.DataFrame(
            {
                "ra_j2000": [10.0, 20.0, 30.0],
                "dec_j2000": [-45.0, 0.0, 45.0],
                "flux_density": [1.5, 2.3, 3.1],
                "object_id": ["obj1", "obj2", "obj3"],
            }
        )

        # Call the function with web_search=True
        fields = fields_from_df(df, web_search=True)

        # Verify that we get the correct number of fields
        assert len(fields) == 4

        # Verify that guess_ucd was called for each column with web_search=True
        assert mock_guess_ucd.call_count == 4
        for call in mock_guess_ucd.call_args_list:
            assert call[0][1] == True  # web_search parameter

        # Verify numeric columns have MinMax set
        ra_field = next(f for f in fields if f.name == "ra_j2000")
        assert ra_field.qc is not None
        assert ra_field.qc.min == 10.0
        assert ra_field.qc.max == 30.0

        dec_field = next(f for f in fields if f.name == "dec_j2000")
        assert dec_field.qc is not None
        assert dec_field.qc.min == -45.0
        assert dec_field.qc.max == 45.0

        flux_field = next(f for f in fields if f.name == "flux_density")
        assert flux_field.qc is not None
        assert flux_field.qc.min == 1.5
        assert flux_field.qc.max == 3.1

        # Verify string column has no MinMax
        id_field = next(f for f in fields if f.name == "object_id")
        assert id_field.qc is None

        # Verify UCDs were assigned
        assert ra_field.ucd == "pos.eq.ra"
        assert dec_field.ucd == "pos.eq.dec"
        assert flux_field.ucd == "phot.flux.density"
        assert id_field.ucd == "meta.id"

    @patch("metadata.guess_ucd")
    def test_fields_from_df_respects_web_search_parameter(self, mock_guess_ucd):
        """
        Test that fields_from_df correctly passes the web_search parameter
        to guess_ucd.
        """
        mock_guess_ucd.return_value = "meta.id"

        df = pl.DataFrame({"column1": [1, 2, 3]})

        # Call with web_search=False
        fields = fields_from_df(df, web_search=False)

        # Verify that guess_ucd was called with web_search=False
        mock_guess_ucd.assert_called_once_with("column1", False)

    @patch("metadata.guess_ucd")
    def test_fields_from_df_handles_dataframe_with_one_column(self, mock_guess_ucd):
        """
        Test that fields_from_df correctly processes a DataFrame with a single column.
        """
        mock_guess_ucd.return_value = "meta.id"
        
        df = pl.DataFrame({"single_col": [1, 2, 3]})

        fields = fields_from_df(df, web_search=True)

        # Should return one field
        assert len(fields) == 1
        assert fields[0].name == "single_col"
        mock_guess_ucd.assert_called_once_with("single_col", True)

    @patch("metadata.guess_ucd")
    def test_fields_from_df_handles_integer_types(self, mock_guess_ucd):
        """
        Test that fields_from_df correctly handles integer data types
        and sets MinMax appropriately.
        """
        mock_guess_ucd.return_value = "meta.number"

        df = pl.DataFrame({"count": [5, 10, 15, 20]})

        fields = fields_from_df(df, web_search=True)

        assert len(fields) == 1
        count_field = fields[0]
        assert count_field.name == "count"
        assert count_field.data_type == "int64"
        assert count_field.qc is not None
        assert count_field.qc.min == 5
        assert count_field.qc.max == 20

    @patch("metadata.guess_ucd")
    def test_fields_from_df_handles_boolean_types(self, mock_guess_ucd):
        """
        Test that fields_from_df correctly handles boolean data types.
        """
        mock_guess_ucd.return_value = "meta.code"

        df = pl.DataFrame({"is_valid": [True, False, True]})

        fields = fields_from_df(df, web_search=True)

        assert len(fields) == 1
        bool_field = fields[0]
        assert bool_field.name == "is_valid"
        # Polars uses "boolean" as the data type string, not "bool"
        assert bool_field.data_type == "boolean"
        # Booleans will have MinMax set because False/True are not strings
        # and the code only checks isinstance(min, str) to exclude MinMax
        assert bool_field.qc is not None

    @patch("metadata.guess_ucd")
    def test_fields_from_df_assigns_correct_data_types(self, mock_guess_ucd):
        """
        Test that fields_from_df correctly assigns polars data types
        as lowercase strings.
        """
        mock_guess_ucd.side_effect = ["ucd1", "ucd2", "ucd3"]

        df = pl.DataFrame(
            {
                "float_col": [1.0, 2.0, 3.0],
                "int_col": [1, 2, 3],
                "str_col": ["a", "b", "c"],
            }
        )

        fields = fields_from_df(df, web_search=False)

        float_field = next(f for f in fields if f.name == "float_col")
        int_field = next(f for f in fields if f.name == "int_col")
        str_field = next(f for f in fields if f.name == "str_col")

        assert float_field.data_type == "float64"
        assert int_field.data_type == "int64"
        assert str_field.data_type == "string" or str_field.data_type == "str"
