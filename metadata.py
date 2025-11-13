"""
Module for handling the WAVES-specific flavor of MAML
Helper functions for building the metadata for the datasets.
"""

from dataclasses import dataclass
from enum import Enum
import re
import datetime

import polars as pl


def _validate_email(email: str) -> bool:
    """
    Checking that an email is correct.
    """
    return re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", email)


class SurveyName(Enum):
    WAVES = "WAVES"
    FOURC3R2 = "WAVES-4C3R2"
    STEPS = "WAVES-STePS"
    ORCHIDS = "WAVES-ORCHIDS"


@dataclass
class Author:
    name: str
    surname: str
    email: str

    def __post_init__(self) -> None:
        """
        Validating email.
        """
        if not _validate_email(self.email):
            raise ValueError("Email is not valid")

    def __str__(self) -> None:
        return f"{self.name.capitalize()} {self.surname.capitalize()} <{self.email}>"


@dataclass
class Dependency:
    """
    Other tables or datasets that someone might be dependendt on.
    """

    survey: str
    dataset: str
    table: str
    version: str


@dataclass
class MinMax:
    min: float
    max: float


@dataclass
class ColumnMetaData:
    name: str
    unit: str = None
    info: str = None
    ucd: str
    data_type: str
    qc: MinMax


class License(Enum):
    PUBLIC = "Copyright WAVES [Private]"
    PRIVATE = "MIT"


@dataclass
class MetaData:
    survey: SurveyName
    dataset: str
    table: str
    version: str
    date: str = str(datetime.today()).split(" ")[0]
    author: Author
    coauthors: list[Author]
    dois: list[str]
    depends: list[Dependency]
    description: str
    comments: str | list[str] = None
    license: License = None
    keywords: list[str] = None
    maml_version: str = "v1.1"
    fields = list[ColumnMetaData]


def fields_from_df(data_frame: pl.DataFrame) -> list[ColumnMetaData]:
    """
    Automatically generating as much field metadata as possible.

    This function will attempt to guess the ucd strings using the
    official WAVES lookup table. If the search cds is True
    then any other column names will make requests to the cds website.
    """
    column_names = data_frame.columns()
    # We are lucky here that datacentral adopts the polars datatypes lower cased.
    data_types = [str(dtype).lower() for dtype in list(data_frame.dtype)]
    mins = data_frame.min()
    maxs = data_frame.max()
    qcs = [MinMax(min, max) for min, max in zip(mins, maxs)]

