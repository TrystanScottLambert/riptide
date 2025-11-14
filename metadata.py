"""
Module for handling the WAVES-specific flavor of MAML
Helper functions for building the metadata for the datasets.
"""

from dataclasses import dataclass
from enum import Enum
import re
from datetime import datetime
import polars as pl
import httpx
import json
from config import protected_words, filter_words, exceptions


def _is_valid_email(email: str) -> bool:
    """
    Checking that an email is correct. Very basic validation checking . and @
    """
    if re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", email):
        return True
    return False


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
        if not _is_valid_email(self.email):
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

    def _is_missing(self) -> list[str]:
        """
        Returns a list of all the fields that are None.
        """
        return [field for field, value in self.__dict__.items() if not value]


class License(Enum):
    PUBLIC = "Copyright WAVES [Private]"
    PRIVATE = "MIT"


@dataclass
class Columns:
    columns: dict[str, ColumnMetaData]

    def set_info(self, column_name: str, info: str) -> None:
        """
        Sets the info field for the given column.
        """
        try:
            self.columns[column_name].info = info
        except KeyError:
            raise ValueError(f"No column with the name '{column_name}' found.")

    def get_info(self, column_name: str) -> str | None:
        """
        Returns the info for the given column.
        """
        try:
            value = self.columns[column_name].info
        except KeyError:
            raise ValueError(f"No column with the name '{column_name}' found.")
        return value

    @property
    def info(self) -> list[str]:
        """
        returns a list of all the info strings for all the columns.
        """
        return [column.info for column in self.columns.values()]

    def set_unit(self, column_name: str, unit: str) -> None:
        """
        Sets the unit field for the given column.
        """
        try:
            self.columns[column_name].unit = unit
        except KeyError:
            raise ValueError(f"No column with the name '{column_name}' found.")

    def get_unit(self, column_name: str) -> None:
        """
        Returns the unit for the given column
        """
        try:
            value = self.columns[column_name].unit
        except KeyError:
            raise ValueError(f"No column with the name '{column_name}' found.")
        return value

    @property
    def units(self) -> list[str]:
        """
        Returns a list of all the unit strings for all the columns.
        """
        return [column.unit for column in self.columns.values()]

    def set_ucd(self, column_name: str, ucd: str) -> None:
        """
        Sets the ucd field for the given column.
        """
        try:
            self.columns[column_name].ucd = ucd
        except KeyError:
            raise ValueError(f"No column with the name '{column_name}' found.")

    def get_ucd(self, column_name: str) -> str:
        """
        Returns the unit for the given column.
        """
        try:
            value = self.columns[column_name].ucd
        except KeyError:
            raise ValueError(f"No column with the name '{column_name}' found.")
        return value

    @property
    def ucds(self) -> list[str]:
        """
        Returns a list of all the ucds for all the columns.
        """
        return [column.ucd for column in self.columns.values()]

    def set_minmax(self, column_name: str, min: float, max: float) -> None:
        """
        Sets the minimum and maximum values for the given column.
        If the column is not numerica this will raise an error."
        """
        if self.columns[column_name].data_type == "string":
            raise ValueError(
                f"Cannot set the min max of a 'string' type column: '{column_name}'"
            )
        try:
            self.columns[column_name].qc = MinMax(min=min, max=max)
        except KeyError:
            raise ValueError(f"No column with the name '{column_name}' found.")

    def get_minmax(self, column_name: str) -> MinMax:
        """
        Returns the qc (min max) for the given column.
        """
        try:
            value = self.columns[column_name].qc
        except KeyError:
            raise ValueError(f"No column with the name '{column_name}' found.")
        return value

    @property
    def qcs(self) -> list[MinMax]:
        """
        Returns a list of all the qcs for all the columns.
        """
        [column.qc for column in self.columns.values()]

    @property
    def names(self) -> list[str]:
        """
        Returns a list of all the column names
        """
        return self.colunns.keys()

    @property
    def data_types(self) -> list[str]:
        """
        Returns a list of all the datatypes
        """
        return [column.data_type for column in self.columns.values()]

    def is_complete(self) -> bool:
        """
        Returns True if the columns have all the metadata and false if there are fields missing.
        """
        for column in self.columns.values():
            if len(column._is_missing()) != 0:
                return False
        return True

    def missing_values(self) -> dict[str, list[str]]:
        """
        Returns a dictonary of all the columns that have missing fields and what
        those fields are.
        """
        missing_dict = {}
        for column_name, column in self.columns.items():
            missing_fields = column._is_missing()
            if len(missing_fields) != 0:
                missing_dict[column_name] = missing_fields
        return missing_dict


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
            current_ucds += [filter_word.secondary_ucd]
    full_ucds = list(dict.fromkeys(";".join(current_ucds).split(";")))
    return ";".join(full_ucds)


def _scrape_cds_ucd(column_name: str) -> str | None:
    """
    Makes a request to https://cdsweb.u-strasbg.fr/UCD/ucd-finder/ and returns best guess at ucd.
    """
    sanitized_string = column_name.translate(str.maketrans("-_.", "   "))
    re = httpx.get(
        f"https://cdsweb.u-strasbg.fr/UCD/ucd-finder/suggest?d={sanitized_string}"
    )
    re_dict = json.loads(re.text)
    try:
        return re_dict["ucd"][0]["ucd"]
    except IndexError:
        return None


def guess_ucd(column_name: str, web_search: bool = True) -> str | None:
    """
    Looks for a WAVES ucd if it exists or else scrapes the CDS website.
    """
    ucd = _scrape_ucd(column_name)
    if ucd == "" and web_search:
        ucd = _scrape_cds_ucd(column_name)
    return ucd


def fields_from_df(data_frame: pl.DataFrame, web_search: bool = True) -> Columns:
    """
    Automatically generating as much field metadata as possible.

    This function will attempt to guess the ucd strings using the
    official WAVES lookup table. If the search cds is True
    then any other column names will make requests to the cds website.
    """
    column_names = data_frame.columns
    # We are lucky here that datacentral adopts the polars datatypes lower cased.
    data_types = [str(dtype).lower() for dtype in list(data_frame.dtypes)]

    mins = data_frame.min().row(0)
    maxs = data_frame.max().row(0)
    qcs = [
        MinMax(min, max) if not isinstance(min, str) else None
        for min, max in zip(mins, maxs)
    ]

    ucds = [guess_ucd(column_name, web_search) for column_name in column_names]

    field_data = []
    for name, data_type, ucd, qc in zip(column_names, data_types, ucds, qcs):
        field_data.append(ColumnMetaData(name, ucd, data_type, qc))

    return Columns({column.name: column for column in field_data})
