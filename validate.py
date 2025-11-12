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


def validate(table_name: str) -> ValidationReport:
    """
    Does the overall validation for the parquet file.
    """
    df = pl.read_parquet(table_name)
    table_report = validate_table(df, table_name)
    
    column_names = df.columns
    column_reports = [validate_column_name(column_name) for column_name in column_names]
    
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

    print(f"{BOLD}{'=' * 70}{RESET}")

 
    return ValidationReport(table_report.valid, names_valid)
