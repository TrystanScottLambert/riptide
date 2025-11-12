"""
Module to handle validating the data in the parquet file.
"""
from enum import Enum
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

def check_column_values(data_frame: pl.DataFrame, column_name: str, min: float, max:float, include: ClosedInterval) -> bool:
    """
    Determines if a given column name is between the min max values assuming [min, max].
    Interval closed or open can be handled with the ClosedInterval enum.
    """
    contained = data_frame.select(pl.col(column_name).is_between(min, max, closed = include.value).all()).item()
    return contained

def _find_column(standard_root_name: str, data_frame: pl.DataFrame) -> str | None:
    """
    Attempts to find the column name that matches the root name.
    e.g. 'ra' would be the root name and a column called ra_j2000 would be
    identified as the ra column.
    
    Assumes columns are in snake_case.
    """
    for column_name in data_frame.columns:
        for word in column_name.split('_'):
            if word == standard_root_name:
                return column_name
    return None

def validate_ra(data_frame: pl.DataFrame, ra_column_name = None) -> bool:
    """
    Checks that the ra column is correct.
    """
    if not ra_column_name:
        ra_column_name = _find_column("ra", data_frame)
    return check_column_values(data_frame, ra_column_name, 0, 360, ClosedInterval.RIGHT)

def validate_dec(data_frame: pl.DataFrame, dec_column_name = None) -> bool:
    """
    Checks that the dec column is correct.
    """
    if not dec_column_name:
        dec_column_name = _find_column("dec", data_frame)
    return check_column_values(data_frame, dec_column_name, -90, 90, ClosedInterval.BOTH)


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
        bad_columns  = None
    return valid, bad_columns
    
