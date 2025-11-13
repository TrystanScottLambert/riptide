"""
Module for handling the WAVES-specific flavor of MAML
Helper functions for building the metadata for the datasets.
"""

from dataclasses import dataclass
from enum import Enum
import re
import datetime


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
    unit: str
    info: str
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
