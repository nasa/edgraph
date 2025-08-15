"""Unit tests for platform ingestion."""

import os
import json
import logging
from unittest.mock import patch, MagicMock, mock_open, ANY, PropertyMock, call

import pytest

from graph_ingest.common.config_reader import AppConfig, DatabaseConfig, PathsConfig
from graph_ingest.ingest_scripts.ingest_node_platform import PlatformIngestor, main
from graph_ingest.tests.fixtures.test_data import MOCK_PLATFORM
# Import the realistic platform fixture
from graph_ingest.tests.fixtures.generated_test_data import MOCK_PLATFORM as REALISTIC_PLATFORM
from graph_ingest.tests.unit.base_test import BaseIngestorTest

class TestPlatformIngestor(BaseIngestorTest):
    """Tests for the PlatformIngestor class."""

    def test_initialization(self):
        """Test initialization of PlatformIngestor."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_logger = MagicMock(spec=logging.Logger)
        mock_driver = MagicMock()
        
        # Apply patches
        with patch('graph_ingest.ingest_scripts.ingest_node_platform.setup_logger', return_value=mock_logger) as mock_setup_logger:
            with patch('graph_ingest.ingest_scripts.ingest_node_platform.load_config', return_value=mock_config):
                with patch('os.makedirs') as mock_makedirs:
                    with patch('graph_ingest.ingest_scripts.ingest_node_platform.get_driver', return_value=mock_driver) as mock_get_driver:
                        # Act
                        ingestor = PlatformIngestor()
                        
                        # Assert
                        mock_makedirs.assert_called_once_with('/mock/log/dir', exist_ok=True)
                        mock_setup_logger.assert_called_once()
                        mock_get_driver.assert_called_once()
                        assert ingestor.driver == mock_driver
                        assert ingestor.logger == mock_logger
                        assert ingestor.config == mock_config

    def test_set_platform_uniqueness_constraint(self):
        """Test setting uniqueness constraint."""
        # Arrange
        with patch('graph_ingest.ingest_scripts.ingest_node_platform.load_config') as mock_load_config:
            mock_config = self.setup_mock_config()
            mock_load_config.return_value = mock_config
            
            with patch('os.makedirs'):
                with patch('graph_ingest.ingest_scripts.ingest_node_platform.setup_logger') as mock_setup_logger:
                    with patch('graph_ingest.ingest_scripts.ingest_node_platform.get_driver') as mock_get_driver:
                        # Create mocks for the Neo4j driver chain
                        mock_driver = MagicMock()
                        mock_session = MagicMock()
                        mock_logger = MagicMock()
                        mock_setup_logger.return_value = mock_logger
                        
                        # Configure the mocks
                        mock_get_driver.return_value = mock_driver
                        mock_driver.session.return_value = mock_session
                        mock_session.__enter__ = MagicMock(return_value=mock_session)
                        mock_session.__exit__ = MagicMock(return_value=None)
                        
                        # Act
                        ingestor = PlatformIngestor()
                        ingestor.set_platform_uniqueness_constraint()
                        
                        # Assert
                        mock_driver.session.assert_called()
                        mock_session.run.assert_called_once_with(
                            "CREATE CONSTRAINT FOR (p:Platform) REQUIRE p.globalId IS UNIQUE"
                        )
                        mock_logger.info.assert_called_with("Uniqueness constraint on Platform.globalId set successfully.")

    def test_process_json_files(self):
        """Test processing JSON files."""
        # Arrange
        with patch('graph_ingest.ingest_scripts.ingest_node_platform.load_config') as mock_load_config:
            mock_config = self.setup_mock_config()
            mock_load_config.return_value = mock_config
            
            with patch('os.makedirs'):
                with patch('graph_ingest.ingest_scripts.ingest_node_platform.setup_logger') as mock_setup_logger:
                    with patch('graph_ingest.ingest_scripts.ingest_node_platform.get_driver') as mock_get_driver:
                        with patch('graph_ingest.ingest_scripts.ingest_node_platform.find_json_files') as mock_find_json_files:
                            with patch('builtins.open', new_callable=mock_open):
                                with patch('json.load') as mock_json_load:
                                    with patch('graph_ingest.ingest_scripts.ingest_node_platform.tqdm') as mock_tqdm:
                                        # Set up mocks
                                        mock_driver = MagicMock()
                                        mock_session = MagicMock()
                                        mock_get_driver.return_value = mock_driver
                                        mock_driver.session.return_value = mock_session
                                        mock_session.__enter__ = MagicMock(return_value=mock_session)
                                        mock_session.__exit__ = MagicMock(return_value=None)
                                        mock_logger = MagicMock()
                                        mock_setup_logger.return_value = mock_logger
                                        
                                        # Set up mocks for file processing
                                        json_file_path = "/mock/data/dir/file1.json"
                                        mock_find_json_files.return_value = [json_file_path]
                                        
                                        # Mock the JSON data structure
                                        mock_json_data = {
                                            "Platforms": [
                                                {
                                                    "ShortName": "Platform1",
                                                    "LongName": "Test Platform 1"
                                                },
                                                {
                                                    "ShortName": "Platform2",
                                                    "LongName": "Test Platform 2"
                                                }
                                            ]
                                        }
                                        mock_json_load.return_value = mock_json_data
                                        
                                        # Mock tqdm to simply return the input
                                        mock_tqdm.side_effect = lambda x, **kwargs: x
                                        
                                        # Act
                                        ingestor = PlatformIngestor()
                                        ingestor.process_json_files("/mock/data/dir")
                                        
                                        # Assert
                                        mock_find_json_files.assert_called_once_with("/mock/data/dir")
                                        mock_json_load.assert_called_once()
                                        mock_session.execute_write.assert_called_once()
                                        
                                        # Verify logging messages
                                        mock_logger.info.assert_has_calls([
                                            call("Found 1 JSON files in directory: /mock/data/dir"),
                                            call("Total platforms to process: 2"),
                                            call("Processed batch of 2 platforms. Total created so far: 2"),
                                            call("Total platforms created: 2")
                                        ], any_order=False)

    @patch('graph_ingest.ingest_scripts.ingest_node_platform.PlatformIngestor.process_json_files')
    @patch('graph_ingest.ingest_scripts.ingest_node_platform.PlatformIngestor.set_platform_uniqueness_constraint')
    @patch('graph_ingest.ingest_scripts.ingest_node_platform.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_node_platform.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_node_platform.load_config')
    @patch('os.makedirs')
    def test_run(self, mock_makedirs, mock_load_config, mock_setup_logger, mock_get_driver,
                mock_set_constraint, mock_process_files):
        """Test the run method."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Act
        ingestor = PlatformIngestor()
        ingestor.run()
        
        # Assert
        mock_set_constraint.assert_called_once()
        mock_process_files.assert_called_once_with("/mock/data/dir")
        
        # Verify final logging message
        mock_logger.info.assert_called_with("Platform node creation process completed.")

    @patch('graph_ingest.ingest_scripts.ingest_node_platform.PlatformIngestor')
    @patch('builtins.print')
    def test_main_function(self, mock_print, mock_ingestor_class):
        """Test the main function."""
        # Arrange
        mock_ingestor = MagicMock()
        mock_ingestor_class.return_value = mock_ingestor
        
        # Act
        main()
        
        # Assert
        mock_ingestor_class.assert_called_once()
        mock_ingestor.run.assert_called_once()
        mock_print.assert_called_with("Platform node creation process completed.") 