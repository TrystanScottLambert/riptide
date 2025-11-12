"""
Module to handle validating the data in the parquet file.
"""

from enum import Enum
from dataclasses import dataclass
import polars as pl

from status import Status, State


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


def validate_ra(data_frame: pl.DataFrame, ra_column_name=None) -> Status:
    """
    Checks that the ra column is correct.
    """
    if not ra_column_name:
        ra_column_name = _find_column("ra", data_frame)

    valid = check_column_values(
        data_frame, ra_column_name, 0, 360, ClosedInterval.RIGHT
    )
    if valid:
        return Status(State.PASS, ra_column_name)
    return Status(State.FAIL, ra_column_name)


def validate_dec(data_frame: pl.DataFrame, dec_column_name=None) -> Status:
    """
    Checks that the dec column is correct.
    """
    if not dec_column_name:
        dec_column_name = _find_column("dec", data_frame)
    valid = check_column_values(
        data_frame, dec_column_name, -90, 90, ClosedInterval.BOTH
    )
    if valid:
        return Status(State.PASS, dec_column_name)
    return Status(State.FAIL, dec_column_name)


def check_no_minus_999(data_frame: pl.DataFrame) -> Status:
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
    if valid:
        return Status(State.PASS)
    return Status(State.FAIL, ";;;".join(bad_columns))


@dataclass
class DataValueReport:
    table_name: str
    valid_ra: Status
    valid_dec: Status
    no_999: Status

    def __post_init__(self) -> None:
        self.valid = all(
            [
                self.valid_ra.state == State.PASS,
                self.valid_dec.state == State.PASS,
                self.no_999.state == State.PASS,
            ]
        )

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
        def status(given_status: Status) -> str:
            match given_status.state:
                case State.PASS:
                    return f"{GREEN}✓ PASS{RESET}"
                case State.FAIL:
                    return f"{RED}✗ FAIL{RESET}"
                case State.WARNING:
                    return f"{YELLOW}⚠ WARNING"

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
            f"  Valid RA column ('{self.valid_ra.message}' in range [0, 360)): {status(self.valid_ra)}"
        )

        print(
            f"  Valid Dec column ('{self.valid_dec.message}' in range [-90, 90]): {status(self.valid_dec)}"
        )

        no_999_status = status(self.no_999)
        no_999_info = ""
        if self.no_999.state == State.FAIL:
            bad_columns = self.no_999.message.split(";;;")
            for column_name in bad_columns:
                no_999_info += f"\n    {RED}→ Column '{column_name}' has -999 values. Using -999 as a None value is not permited.{RESET}"
        print(f"  No -999 in columns: {no_999_status}{no_999_info}")
        print(f"{BOLD}{'=' * 70}{RESET}\n")


def validate_table(df: pl.DataFrame, table_name: str) -> DataValueReport:
    """
    Performs all the data validation checks on the given table.
    """
    ra_valid = validate_ra(df)
    dec_valid = validate_dec(df)
    no_999 = check_no_minus_999(df)
    return DataValueReport(
        table_name,
        ra_valid,
        dec_valid,
        no_999,
    )
