"""Unit tests for config_reader module."""

import json
import os
import pytest
from unittest.mock import patch, mock_open
from graph_ingest.common.config_reader import load_config, AppConfig

def test_config_structure():
    """Test that the configuration has the expected structure."""
    # Sample config data
    sample_config = {
        "database": {
            "uri": "neo4j://localhost:7687",
            "user": "neo4j",
            "password": "password"
        },
        "paths": {
            "source_dois_directory": "/path/to/dois",
            "dataset_metadata_directory": "/path/to/datasets",
            "gcmd_sciencekeyword_directory": "/path/to/keywords",
            "publications_metadata_directory": "/path/to/publications",
            "pubs_of_pubs": "/path/to/pubs_of_pubs",
            "log_directory": "/path/to/logs"
        }
    }
    
    # Save original environment variables
    original_env = {}
    env_vars = ["NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD"]
    for var in env_vars:
        original_env[var] = os.environ.get(var)
        if var in os.environ:
            del os.environ[var]
    
    try:
        # Mock the open function to return our sample config
        with patch("builtins.open", mock_open(read_data=json.dumps(sample_config))):
            # Load the configuration using the provided config_reader
            config = load_config()

            # Verify that we got an AppConfig instance
            assert isinstance(config, AppConfig), "load_config should return an AppConfig instance"

            # Check that database attributes exist and have correct values
            assert config.database.uri == "neo4j://localhost:7687"
            assert config.database.user == "neo4j"
            assert config.database.password == "password"

            # Check that paths attributes exist and have correct values
            assert config.paths.source_dois_directory == "/path/to/dois"
            assert config.paths.dataset_metadata_directory == "/path/to/datasets"
            assert config.paths.gcmd_sciencekeyword_directory == "/path/to/keywords"
            assert config.paths.publications_metadata_directory == "/path/to/publications"
            assert config.paths.pubs_of_pubs == "/path/to/pubs_of_pubs"
            assert config.paths.log_directory == "/path/to/logs"
    finally:
        # Restore original environment variables
        for var, value in original_env.items():
            if value is not None:
                os.environ[var] = value

if __name__ == "__main__":
    pytest.main() 