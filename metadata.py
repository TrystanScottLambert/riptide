"""
Python script to generate a yaml meta data file from a parquet file.
"""

import sys
import os
import warnings
import json
from typing import Union

from pyarrow.parquet import ParquetFile
import yaml
from yaml import SafeDumper
import httpx
from rich import progress


WAVES_UCDS = {
    "UberID": "meta.id;meta.main",
    "CATAID": "meta.id",
}

DATA_TYPES = {
    "int32": "int",
    "int64": "long int",
    "int": "int",
    "object": "char",
    "float32": "float",
    "float64": "double",
    "float": "float",
    "bool": "boolean",
}

STATIC_METADATA = {
    "RIP": "RIP Name",
    "table_name": "TableName",
    "version": "0.0.0",
    "date": "YYYY-MM-DD",
    "author": "Lead Author <email>",
    "coauthors": ["Coauthor 1 <email1>", "Coauthor 2 <email2>"],
    "depend": ["RIP this depends on [optional]", "RIP this depends on [optional]"],
    "comment1": "something interesting about the data [optional]",
    "comment2": "something else [optional]",
    "fields": [],
}


def _scrape_cds_ucd(column_name: str) -> Union[str, None]:
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


def guess_ucd(column_name: str) -> Union[str, None]:
    """
    Looks for a WAVES ucd if it exists or else scrapes the CDS website.
    """
    for waves_col_name, waves_ucd in WAVES_UCDS.items():
        if column_name.lower() == waves_col_name.lower():
            return waves_ucd
    return _scrape_cds_ucd(column_name)


def get_waves_datatype(dtype: str) -> str:
    """
    Returns the appropriate data stype string used by waves.
    """
    try:
        ivoa_dataype = DATA_TYPES[dtype]
        return ivoa_dataype
    except KeyError as err:
        warnings.warn(
            f"Problem with converting data type. {err} type is not handled yet."
        )
        return dtype


def generate_yaml_from_parquet(parquet_path: str, yaml_path: str) -> None:
    """
    Main function which builds the .yaml file meta data from the parquet.
    """
    pf = ParquetFile(parquet_path)
    column_types = pf.schema_arrow.types
    column_names = pf.schema_arrow.names

    metadata = STATIC_METADATA.copy()

    column_data = list(zip(column_types, column_names))
    for _type, name in progress.track(column_data, "Building yaml: "):
        metadata["fields"].append(
            {
                "name": name,
                "unit": None,
                "description": None,
                "ucd": guess_ucd(name),
                "data_type": get_waves_datatype(str(_type)),
            }
        )

    SafeDumper.add_representer(
        type(None),
        lambda dumper, value: dumper.represent_scalar("tag:yaml.org,2002:null", ""),
    )

    with open(yaml_path, "w", encoding="utf8") as file:
        yaml.safe_dump(metadata, file, sort_keys=False, default_flow_style=False)


def main() -> None:
    """
    Main function for scoping reasons.
    """
    file_name = sys.argv[1]
    base_name, _ = os.path.splitext(file_name)
    generate_yaml_from_parquet(file_name, f"{base_name}.yaml")


if __name__ == "__main__":
    main()
