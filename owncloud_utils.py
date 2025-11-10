"""
Collection of useful utilities to handle the owncloud storage space.
"""
from dataclasses import dataclass
import owncloud


USERNAME = "TrystanLambert"
PASSWORD = "MKeh@34FBDgCrVq"
DIP_PATH = "./WAVES/DIPs/"

oc = owncloud.Client("https://cloud.datacentral.org.au")
oc.login(USERNAME, PASSWORD)


def list_all_dips() -> list[str]:
    """
    List all current folders in the DIP directory.
    """
    return [dip.name for dip in oc.list(DIP_PATH)]

@dataclass
class DIP:
    """
    Represents a DIP object
    """
    path: str
    parquet_files: list[str]
    maml_files: list[str]


if __name__ == '__main__':
    things = list_all_dips()
    print(things)
