"""
Module to handle reading in and parsing configuration settings
"""

from dataclasses import dataclass
import yaml

MAX_COLUMN_LENGTH = 50
WARN_COLUMN_LENGTH = 25
_PROTECTED_WORD_FILE = "extdata/protected_words.yaml"
_FILTER_NAME_FILE = "extdata/filters.yaml"
_EXCEPTION_FILE = "extdata/exceptions.yaml"


@dataclass
class ExceptionWord:
    name: str
    ucd: str
    unit: str


@dataclass
class ProtectedWord:
    name: str
    common_representations: list[str]
    ucd: str
    unit: str


@dataclass
class FilterName:
    name: str
    secondary_ucd: str

    def __post_init__(self) -> None:
        self.inverse_name = "_".join(self.name.split("_")[::-1])


with open(_EXCEPTION_FILE, encoding="utf8") as file:
    exceptions_dict = yaml.safe_load(file)

with open(_PROTECTED_WORD_FILE, encoding="utf8") as file:
    protected_word_dict = yaml.safe_load(file)

with open(_FILTER_NAME_FILE, encoding="utf8") as file:
    filter_word_dict = yaml.safe_load(file)

protected_words = [
    ProtectedWord(name, entry["common_mistakes"], entry["ucd"], entry["unit"])
    for name, entry in protected_word_dict.items()
]

filter_words = [
    FilterName(name, entry["secondary_ucd"]) for name, entry in filter_word_dict.items()
]

exceptions = [
    ExceptionWord(name, entry["ucd"], entry["unit"])
    for name, entry in exceptions_dict.items()
]
