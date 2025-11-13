"""
Module which combines the reports from the various validations
"""

from dataclasses import dataclass

import polars as pl

from data_validator import validate_table
from column_name_validator import validate_column_name


@dataclass
class ValidationReport:
    valid_data: bool
    valid_column_names: bool


def validate_df(
    df: pl.DataFrame, name_of_table: str, print_output=True
) -> ValidationReport:
    """
    Validates a data frame both the data and the column names.
    """
    table_report = validate_table(df, name_of_table)

    column_names = df.columns
    column_reports = [validate_column_name(column_name) for column_name in column_names]
    if print_output:
        table_report.print_report()
        names_valid = True
        BOLD = "\033[1m"
        RESET = "\033[0m"

        # Print header
        print(f"\n{BOLD}{'=' * 70}{RESET}")
        print(f"{BOLD}Column Name Validation Report{RESET}")
        print(f"{BOLD}{'=' * 70}{RESET}")

        for report in column_reports:
            if not report.valid:
                report.print_report()
                names_valid = False

        print(f"{'=' * 70}")

    return ValidationReport(table_report.valid, names_valid)


def validate(table_name: str, print_output=True) -> ValidationReport:
    """
    Does the overall validation for the parquet file.
    """
    df = pl.read_parquet(table_name)
    return validate_df(df, table_name, print_output)
