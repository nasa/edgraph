"""Unit tests for datacenter ingestion."""

import pytest
import os
import json
import logging
from unittest.mock import patch, MagicMock, mock_open, ANY, PropertyMock, call

from graph_ingest.common.config_reader import AppConfig, DatabaseConfig, PathsConfig
from graph_ingest.ingest_scripts.ingest_node_datacenter import DataCenterIngestor, main

# Sample test data
MOCK_DATACENTER = {
    "DataCenters": [
        {
            "ShortName": "PODAAC",
            "LongName": "Physical Oceanography Distributed Active Archive Center",
            "ContactInformation": {
                "RelatedUrls": [
                    {
                        "URL": "https://podaac.jpl.nasa.gov"
                    }
                ]
            }
        }
    ]
}

MOCK_DATACENTER_MULTIPLE = {
    "DataCenters": [
        {
            "ShortName": "PODAAC",
            "LongName": "Physical Oceanography Distributed Active Archive Center",
            "ContactInformation": {
                "RelatedUrls": [
                    {
                        "URL": "https://podaac.jpl.nasa.gov"
                    }
                ]
            }
        },
        {
            "ShortName": "NSIDC",
            "LongName": "National Snow and Ice Data Center",
            "ContactInformation": {
                "RelatedUrls": [
                    {
                        "URL": "https://nsidc.org"
                    }
                ]
            }
        }
    ]
}

MOCK_DATACENTER_MISSING_FIELDS = {
    "DataCenters": [
        {
            "ShortName": "TESTDAAC"
            # Missing LongName and ContactInformation
        }
    ]
}

MOCK_DATACENTER_EMPTY_FIELDS = {
    "DataCenters": [
        {
            "ShortName": "TESTDAAC",
            "LongName": "",
            "ContactInformation": {
                "RelatedUrls": []
            }
        }
    ]
}

MOCK_DATACENTER_MISSING_URL = {
    "DataCenters": [
        {
            "ShortName": "TESTDAAC",
            "LongName": "Test DAAC",
            "ContactInformation": {
                "RelatedUrls": [
                    {
                        # Missing URL
                    }
                ]
            }
        }
    ]
}

MOCK_NO_DATACENTERS = {
    # No DataCenters key
    "SomeOtherKey": []
}


class TestDataCenterIngestor:
    """Tests for the DataCenterIngestor class."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://neo4j:7687",
                user="neo4j",
                password="password"
            ),
            paths=PathsConfig(
                source_dois_directory="/path/to/dois",
                dataset_metadata_directory="/mock/data/dir",
                gcmd_sciencekeyword_directory="/path/to/keywords",
                publications_metadata_directory="/path/to/publications",
                pubs_of_pubs="/path/to/pubs_of_pubs",
                log_directory="/mock/log/dir"
            )
        )

    def test_initialization(self, mock_env_get):
        """Test initialization of DataCenterIngestor."""
        # Act
        with patch('graph_ingest.ingest_scripts.ingest_node_datacenter.load_config', return_value=self.mock_config), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.os.makedirs') as mock_makedirs, \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.setup_logger') as mock_setup_logger, \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.get_driver') as mock_get_driver:
            
            ingestor = DataCenterIngestor()
            
            # Assert
            assert ingestor.config == self.mock_config
            assert ingestor.log_directory == "/mock/log/dir"
            mock_makedirs.assert_called_once_with("/mock/log/dir", exist_ok=True)
            mock_setup_logger.assert_called_once()
            mock_get_driver.assert_called_once()

    def test_set_uniqueness_constraint_success(self, mock_env_get):
        """Test setting uniqueness constraint successfully."""
        # Arrange
        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session

        # Act
        with patch('graph_ingest.ingest_scripts.ingest_node_datacenter.load_config', return_value=self.mock_config), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.os.makedirs'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.setup_logger'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.get_driver', return_value=mock_driver):
            
            ingestor = DataCenterIngestor()
            ingestor.set_uniqueness_constraint()
            
            # Assert
            mock_session.run.assert_called_once_with(
                "CREATE CONSTRAINT FOR (dc:DataCenter) REQUIRE dc.globalId IS UNIQUE"
            )

    def test_set_uniqueness_constraint_failure(self, mock_env_get):
        """Test handling error when setting uniqueness constraint."""
        # Arrange
        mock_session = MagicMock()
        mock_session.run.side_effect = Exception("Constraint already exists")
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session
        mock_logger = MagicMock()

        # Act
        with patch('graph_ingest.ingest_scripts.ingest_node_datacenter.load_config', return_value=self.mock_config), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.os.makedirs'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.setup_logger', return_value=mock_logger), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.get_driver', return_value=mock_driver):
            
            ingestor = DataCenterIngestor()
            ingestor.logger = mock_logger
            ingestor.set_uniqueness_constraint()
            
            # Assert
            mock_session.run.assert_called_once()
            mock_logger.error.assert_called_once_with(
                "Failed to create uniqueness constraint: Constraint already exists"
            )

    def test_add_data_centers_batch(self, mock_env_get):
        """Test adding a batch of data centers."""
        # Arrange
        mock_tx = MagicMock()
        data_centers_batch = [
            {
                "shortName": "PODAAC",
                "longName": "Physical Oceanography Distributed Active Archive Center",
                "url": "https://podaac.jpl.nasa.gov"
            }
        ]

        # Act
        with patch('graph_ingest.ingest_scripts.ingest_node_datacenter.load_config', return_value=self.mock_config), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.os.makedirs'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.setup_logger'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.get_driver'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.generate_uuid_from_name', return_value="test-uuid"):
            
            ingestor = DataCenterIngestor()
            ingestor.add_data_centers_batch(mock_tx, data_centers_batch)
            
            # Assert
            mock_tx.run.assert_called_once_with(
                "MERGE (dc:DataCenter {globalId: $globalId}) "
                "ON CREATE SET dc.shortName = $shortName, dc.longName = $longName, dc.url = $url",
                globalId="test-uuid",
                shortName="PODAAC",
                longName="Physical Oceanography Distributed Active Archive Center",
                url="https://podaac.jpl.nasa.gov"
            )

    def test_process_files_single_datacenter(self, mock_env_get):
        """Test processing a single datacenter."""
        # Arrange
        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session

        # Act
        with patch('graph_ingest.ingest_scripts.ingest_node_datacenter.load_config', return_value=self.mock_config), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.os.makedirs'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.setup_logger'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.get_driver', return_value=mock_driver), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.find_json_files', return_value=["test.json"]), \
             patch('builtins.open', mock_open(read_data=json.dumps(MOCK_DATACENTER))):
            
            ingestor = DataCenterIngestor()
            ingestor.process_files()
            
            # Assert
            mock_session.execute_write.assert_called_once()

    def test_process_files_multiple_datacenters(self, mock_env_get):
        """Test processing multiple datacenters."""
        # Arrange
        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session

        # Act
        with patch('graph_ingest.ingest_scripts.ingest_node_datacenter.load_config', return_value=self.mock_config), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.os.makedirs'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.setup_logger'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.get_driver', return_value=mock_driver), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.find_json_files', return_value=["test.json"]), \
             patch('builtins.open', mock_open(read_data=json.dumps(MOCK_DATACENTER_MULTIPLE))):
            
            ingestor = DataCenterIngestor()
            ingestor.process_files()
            
            # Assert
            mock_session.execute_write.assert_called_once()

    def test_process_files_no_datacenters(self, mock_env_get):
        """Test processing a file with no datacenters."""
        # Arrange
        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session

        # Act
        with patch('graph_ingest.ingest_scripts.ingest_node_datacenter.load_config', return_value=self.mock_config), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.os.makedirs'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.setup_logger'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.get_driver', return_value=mock_driver), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.find_json_files', return_value=["test.json"]), \
             patch('builtins.open', mock_open(read_data=json.dumps(MOCK_NO_DATACENTERS))):
            
            ingestor = DataCenterIngestor()
            ingestor.process_files()
            
            # Assert
            mock_session.execute_write.assert_not_called()

    def test_process_files_missing_fields(self, mock_env_get):
        """Test processing a file with missing fields."""
        # Arrange
        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session

        # Act
        with patch('graph_ingest.ingest_scripts.ingest_node_datacenter.load_config', return_value=self.mock_config), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.os.makedirs'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.setup_logger'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.get_driver', return_value=mock_driver), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.find_json_files', return_value=["test.json"]), \
             patch('builtins.open', mock_open(read_data=json.dumps(MOCK_DATACENTER_MISSING_FIELDS))):
            
            ingestor = DataCenterIngestor()
            ingestor.process_files()
            
            # Assert
            mock_session.execute_write.assert_called_once()

    def test_process_files_missing_url(self, mock_env_get):
        """Test processing a file with missing URL."""
        # Arrange
        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session

        # Act
        with patch('graph_ingest.ingest_scripts.ingest_node_datacenter.load_config', return_value=self.mock_config), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.os.makedirs'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.setup_logger'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.get_driver', return_value=mock_driver), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.find_json_files', return_value=["test.json"]), \
             patch('builtins.open', mock_open(read_data=json.dumps(MOCK_DATACENTER_MISSING_URL))):
            
            ingestor = DataCenterIngestor()
            ingestor.process_files()
            
            # Assert
            mock_session.execute_write.assert_called_once()

    def test_process_files_batch_processing(self, mock_env_get):
        """Test batch processing of files."""
        # Arrange
        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session

        # Create a list of 150 datacenters (more than default batch size)
        large_datacenter_list = {
            "DataCenters": [
                {
                    "ShortName": f"DAAC{i}",
                    "LongName": f"Test DAAC {i}",
                    "ContactInformation": {
                        "RelatedUrls": [{"URL": f"https://daac{i}.test.gov"}]
                    }
                } for i in range(150)
            ]
        }

        # Act
        with patch('graph_ingest.ingest_scripts.ingest_node_datacenter.load_config', return_value=self.mock_config), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.os.makedirs'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.setup_logger'), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.get_driver', return_value=mock_driver), \
             patch('graph_ingest.ingest_scripts.ingest_node_datacenter.find_json_files', return_value=["test.json"]), \
             patch('builtins.open', mock_open(read_data=json.dumps(large_datacenter_list))):
            
            ingestor = DataCenterIngestor()
            ingestor.process_files(batch_size=50)
            
            # Assert
            # With 150 datacenters and batch size 50, we expect 3 batch executions
            assert mock_session.execute_write.call_count == 3

    def test_main_function(self, mock_env_get):
        """Test the main function."""
        # Arrange
        mock_ingestor = MagicMock()

        # Act
        with patch('graph_ingest.ingest_scripts.ingest_node_datacenter.DataCenterIngestor', return_value=mock_ingestor):
            main()
            
            # Assert
            mock_ingestor.set_uniqueness_constraint.assert_called_once()
            mock_ingestor.process_files.assert_called_once()


@pytest.fixture
def mock_env_get():
    """Mock environment variable getter."""
    with patch('os.environ.get') as mock:
        yield mock 