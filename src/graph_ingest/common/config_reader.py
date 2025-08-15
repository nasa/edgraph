import json
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class DatabaseConfig:
    uri: str
    user: str
    password: str


@dataclass
class PathsConfig:
    source_dois_directory: str
    dataset_metadata_directory: str
    gcmd_sciencekeyword_directory: str
    publications_metadata_directory: str
    pubs_of_pubs: str
    log_directory: str


@dataclass
class AppConfig:
    database: DatabaseConfig
    paths: PathsConfig


def load_config(config_path: Optional[str] = None) -> AppConfig:
    """
    Load the configuration from a JSON file, with optional overrides from environment variables.
    If no config_path is provided, it defaults to the config file in the config directory.
    """
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')

    with open(config_path, 'r') as file:
        raw_config = json.load(file)

    # Override database settings with environment variables, if available
    db = raw_config['database']
    db['uri'] = os.getenv('NEO4J_URI', db['uri'])
    db['user'] = os.getenv('NEO4J_USER', db['user'])
    db['password'] = os.getenv('NEO4J_PASSWORD', db['password'])

    # Construct the dataclass structure
    return AppConfig(
        database=DatabaseConfig(**raw_config['database']),
        paths=PathsConfig(**raw_config['paths'])
    )
