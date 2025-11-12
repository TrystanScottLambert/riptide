"""
Module to handle validating the data in the parquet file.
"""

from enum import Enum
from dataclasses import dataclass
import polars as pl


class ClosedInterval(Enum):
    """
    Contains the possible intervals over which a range can be closed or open.
    'left' => [a, b)
    'right' => (a, b]
    'both' => [a, b]
    'none' => (a, b)
    """

    LEFT = "left"
    RIGHT = "right"
    BOTH = "both"
    NONE = "none"


def check_column_values(
    data_frame: pl.DataFrame,
    column_name: str,
    min: float,
    max: float,
    include: ClosedInterval,
) -> bool:
    """
    Determines if a given column name is between the min max values assuming [min, max].
    Interval closed or open can be handled with the ClosedInterval enum.
    """
    contained = data_frame.select(
        pl.col(column_name).is_between(min, max, closed=include.value).all()
    ).item()
    return contained


def _find_column(standard_root_name: str, data_frame: pl.DataFrame) -> str | None:
    """
    Attempts to find the column name that matches the root name.
    e.g. 'ra' would be the root name and a column called ra_j2000 would be
    identified as the ra column.

    Assumes columns are in snake_case.
    """
    for column_name in data_frame.columns:
        for word in column_name.split("_"):
            if word == standard_root_name:
                return column_name
    return None


def validate_ra(data_frame: pl.DataFrame, ra_column_name=None) -> bool:
    """
    Checks that the ra column is correct.
    """
    if not ra_column_name:
        ra_column_name = _find_column("ra", data_frame)
    return check_column_values(
        data_frame, ra_column_name, 0, 360, ClosedInterval.RIGHT
    ), ra_column_name


def validate_dec(data_frame: pl.DataFrame, dec_column_name=None) -> bool:
    """
    Checks that the dec column is correct.
    """
    if not dec_column_name:
        dec_column_name = _find_column("dec", data_frame)
    return check_column_values(
        data_frame, dec_column_name, -90, 90, ClosedInterval.BOTH
    ), dec_column_name


def check_no_minus_999(data_frame: pl.DataFrame) -> tuple[bool, list[str] | None]:
    """
    Checks that there are no -999 values anywhere in the table.

    True => There aren't any.
    False => There are.

    Also returns a list of all columns that do have -999 value in them.
    """
    valid = True
    bad_columns = []
    for column in data_frame:
        if -999 in column:
            valid = False
            bad_columns.append(column.name)
    if len(bad_columns) == 0:
        bad_columns = None
    return valid, bad_columns


@dataclass
class DataValueReport:
    table_name: str
    valid_ra: bool
    valid_dec: bool
    no_999: bool
    ra_column_name: str | None
    dec_column_name: str | None
    columns_with_999: list[str] | None

    def __post_init__(self) -> None:
        self.valid = all([self.valid_ra, self.valid_dec, self.no_999])

    def print_report(self) -> None:
        """
        Print a professional validation report with color-coded results.
        """
        # ANSI color codes
        GREEN = "\033[92m"
        RED = "\033[91m"
        BOLD = "\033[1m"
        RESET = "\033[0m"
        YELLOW = "\033[93m"

        # Helper function for status
        def status(passed: bool) -> str:
            if passed:
                return f"{GREEN}✓ PASS{RESET}"
            return f"{RED}✗ FAIL{RESET}"

        # Print header
        print(f"\n{BOLD}{'=' * 70}{RESET}")
        print(f"{BOLD}Table Data Validation Report{RESET}")
        print(f"{BOLD}{'=' * 70}{RESET}")

        # Overall status
        overall_color = GREEN if self.valid else RED
        overall_status = "VALID" if self.valid else "INVALID"
        print(f"\n{BOLD}Table Name:{RESET} {self.table_name}")
        print(f"{BOLD}Overall Status:{RESET} {overall_color}{overall_status}{RESET}")

        print(f"\n{BOLD}Validation Checks:{RESET}")
        print(f"{'-' * 70}")

        print(
            f"  Valid RA column ('{self.ra_column_name}' in range [0, 360)): {status(self.valid_ra)}"
        )

        print(
            f"  Valid Dec column ('{self.dec_column_name}' in range [-90, 90]): {status(self.valid_dec)}"
        )

        no_999_status = status(self.no_999)
        no_999_info = ""
        if not self.no_999 and self.columns_with_999:
            for column_name in self.columns_with_999:
                no_999_info += f"\n    {YELLOW}→ Column '{column_name}' has -999 values. Using -999 as a None value is not permited."
        print(f"  No -999 in columns: {no_999_status}{no_999_info}")


def validate_table(table_name: str) -> DataValueReport:
    """
    Performs all the data validation checks on the given table.
    """
    df = pl.read_parquet(table_name)
    ra_valid, ra_column_name = validate_ra(df)
    dec_valid, dec_column_name = validate_dec(df)
    no_999, columns_999 = check_no_minus_999(df)
    return DataValueReport(
        table_name,
        ra_valid,
        dec_valid,
        no_999,
        ra_column_name,
        dec_column_name,
        columns_999,
    )
