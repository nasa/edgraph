import os
import uuid
from typing import Generator

def generate_uuid_from_doi(doi: str) -> str:
    """
    Generate a UUID based on a DOI using uuid5.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, doi))

def generate_uuid_from_name(name: str) -> str:
    """
    Generate a UUID based on a name using uuid5.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, name))

def find_json_files(directory: str) -> Generator[str, None, None]:
    """
    Recursively find and yield all JSON files in the specified directory.
    """
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".json"):
                yield os.path.join(root, file)
