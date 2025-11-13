"""
Module for handling the WAVES-specific flavor of MAML
Helper functions for building the metadata for the datasets.
"""

from dataclasses import dataclass
from enum import Enum
import re
from datetime import datetime
import polars as pl


from config import protected_words, filter_words, exceptions


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
    ucd: str
    data_type: str
    qc: MinMax
    unit: str = None
    info: str = None


class License(Enum):
    PUBLIC = "Copyright WAVES [Private]"
    PRIVATE = "MIT"


@dataclass
class MetaData:
    survey: SurveyName
    dataset: str
    table: str
    version: str
    author: Author
    coauthors: list[Author]
    dois: list[str]
    depends: list[Dependency]
    description: str
    fields = list[ColumnMetaData]
    date: str = str(datetime.today()).split(" ")[0]
    comments: str | list[str] = None
    license: License = None
    keywords: list[str] = None
    maml_version: str = "v1.1"


def _scrape_ucd(column_name: str) -> str:
    """
    Helper function will try to guess the ucd from the protected_words and filter configs.
    """
    current_ucds = []
    for exception in exceptions:
        if exception.name in column_name:
            current_ucds += [exception.ucd]
    for protected_word in protected_words:
        if "_" in protected_word.name:
            if protected_word.name in column_name:
                current_ucds += protected_word.ucd
        else:
            for word in column_name.split("_"):
                if word == protected_word.name:
                    current_ucds += protected_word.ucd
    for filter_word in filter_words:
        if filter_word.name in column_name:
            print(filter_word.secondary_ucd)
            current_ucds += [filter_word.secondary_ucd]
    full_ucds = list(dict.fromkeys(";".join(current_ucds).split(";")))
    return ";".join(full_ucds)


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
