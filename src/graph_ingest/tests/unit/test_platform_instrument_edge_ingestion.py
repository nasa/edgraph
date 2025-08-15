"""Unit tests for Platform-Instrument relationship ingestion."""

import os
import json
import logging
from unittest.mock import patch, MagicMock, mock_open, call, ANY

import pytest
from tqdm import tqdm

from graph_ingest.ingest_scripts.ingest_edge_platform_instrument import (
    PlatformInstrumentRelationshipIngestor,
    main
)
from graph_ingest.tests.fixtures.test_data import MOCK_PLATFORM, MOCK_INSTRUMENT
from graph_ingest.common.config_reader import AppConfig, DatabaseConfig, PathsConfig
from graph_ingest.tests.unit.base_test import BaseIngestorTest

class TestPlatformInstrumentRelationshipIngestor(BaseIngestorTest):
    """Tests for the PlatformInstrumentRelationshipIngestor class."""

    def setup_neo4j_mock(self):
        """Set up Neo4j mock for testing.
        
        Returns:
            tuple: Mock driver, session, and transaction objects
        """
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_tx = MagicMock()
        
        mock_driver.session.return_value.__enter__.return_value = mock_session
        mock_session.execute_write.side_effect = lambda func, *args, **kwargs: func(mock_tx, *args, **kwargs)
        
        return mock_driver, mock_session, mock_tx

    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.get_driver')
    def test_initialization(self, mock_get_driver, mock_setup_logger, mock_load_config, mock_makedirs):
        """Test that the ingestor initializes correctly with mock configuration."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Act
        ingestor = PlatformInstrumentRelationshipIngestor()
        
        # Assert
        mock_load_config.assert_called_once()
        mock_get_driver.assert_called_once()
        mock_setup_logger.assert_called_once()
        mock_makedirs.assert_called_once_with(mock_config.paths.log_directory, exist_ok=True)
        assert ingestor.config == mock_config
        assert ingestor.driver == mock_driver
        assert ingestor.logger == mock_logger

    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.load_config')
    @patch('os.makedirs')
    def test_generate_uuid_from_shortname(self, mock_makedirs, mock_load_config, mock_setup_logger, mock_get_driver):
        """Test UUID generation from shortname."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        
        # Act
        ingestor = PlatformInstrumentRelationshipIngestor()
        uuid1 = ingestor.generate_uuid_from_shortname("PLATFORM1")
        uuid2 = ingestor.generate_uuid_from_shortname("PLATFORM1")  # Should be the same
        uuid3 = ingestor.generate_uuid_from_shortname("PLATFORM2")  # Should be different
        
        # Assert
        assert uuid1 is not None
        assert uuid1 == uuid2  # Same input should produce same UUID
        assert uuid1 != uuid3  # Different input should produce different UUID

    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.load_config')
    @patch('os.makedirs')
    def test_create_relationship(self, mock_makedirs, mock_load_config, mock_setup_logger, mock_get_driver):
        """Test creating a relationship between platform and instrument."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        mock_tx = MagicMock()
        
        # Create test IDs directly instead of relying on the fixtures
        platform_globalId = "platform-test-uuid"
        instrument_globalId = "instrument-test-uuid"
        
        # Act
        ingestor = PlatformInstrumentRelationshipIngestor()
        ingestor.create_relationship(mock_tx, platform_globalId, instrument_globalId)
        
        # Assert
        mock_tx.run.assert_called_once()
        args, kwargs = mock_tx.run.call_args
        assert "MATCH (p:Platform {globalId: $platform_globalId}), (i:Instrument {globalId: $instrument_globalId})" in args[0]
        assert "MERGE (p)-[:HAS_INSTRUMENT]->(i)" in args[0]
        assert kwargs["platform_globalId"] == platform_globalId
        assert kwargs["instrument_globalId"] == instrument_globalId
        assert mock_logger.info.call_count == 1  # Info log for relationship creation

    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.find_json_files')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.load_config')
    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.tqdm')
    def test_process_json_files(self, mock_tqdm, mock_makedirs, mock_load_config, mock_setup_logger, mock_get_driver, mock_find_json_files):
        """Test processing JSON files to create relationships."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Set up mock driver and session with proper context manager behavior
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session
        mock_get_driver.return_value = mock_driver
        
        # Mock tqdm to return input unchanged
        mock_tqdm.side_effect = lambda x, **kwargs: x
        
        # Mock find_json_files to return list of JSON files
        json_files = ["/mock/data/dir/file1.json", "/mock/data/dir/file2.json"]
        mock_find_json_files.return_value = json_files
        
        # Create sample JSON data
        json_data1 = {
            "Platforms": [
                {
                    "ShortName": "PLATFORM1",
                    "Instruments": [
                        {"ShortName": "INSTRUMENT1"},
                        {"ShortName": "INSTRUMENT2"}
                    ]
                }
            ]
        }
        json_data2 = {
            "Platforms": [
                {
                    "ShortName": "PLATFORM2",
                    "Instruments": [
                        {"ShortName": "INSTRUMENT3"}
                    ]
                }
            ]
        }
        
        # Setup open to return file handles for the JSON files
        with patch('builtins.open', mock_open()) as m:
            # Setup json.load to return the test data
            with patch('json.load') as mock_json_load:
                mock_json_load.side_effect = [json_data1, json_data2]
                
                # Act
                ingestor = PlatformInstrumentRelationshipIngestor()
                ingestor.process_json_files("/mock/data/dir", batch_size=2)  # Small batch size for testing
                
                # Assert
                mock_find_json_files.assert_called_once()
                assert mock_json_load.call_count == 2
                # Verify execute_write was called to create relationships
                assert mock_session.execute_write.call_count > 0
                # Verify the correct number of relationships were created
                assert mock_logger.info.call_args_list[-1][0][0].startswith("Total relationships created:")

    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.find_json_files')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.load_config')
    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.tqdm')
    def test_process_json_files_with_invalid_data(self, mock_tqdm, mock_makedirs, mock_load_config, mock_setup_logger, mock_get_driver, mock_find_json_files):
        """Test processing JSON files with invalid data."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Set up mock driver and session
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session
        mock_get_driver.return_value = mock_driver
        
        # Mock tqdm to return input unchanged
        mock_tqdm.side_effect = lambda x, **kwargs: x
        
        # Mock find_json_files to return list of JSON files
        json_files = ["/mock/data/dir/file1.json", "/mock/data/dir/file2.json", "/mock/data/dir/file3.json"]
        mock_find_json_files.return_value = json_files
        
        # Create sample JSON data with various issues
        json_data1 = {
            # Missing Platforms array
        }
        json_data2 = {
            "Platforms": [
                {
                    # Missing ShortName
                    "Instruments": [
                        {"ShortName": "INSTRUMENT1"}
                    ]
                }
            ]
        }
        json_data3 = {
            "Platforms": [
                {
                    "ShortName": "PLATFORM3",
                    "Instruments": [
                        # Missing ShortName
                        {"LongName": "Instrument Three"}
                    ]
                }
            ]
        }
        
        # Setup open to return file handles for the JSON files
        with patch('builtins.open', mock_open()) as m:
            # Setup json.load to return the test data
            with patch('json.load') as mock_json_load:
                mock_json_load.side_effect = [json_data1, json_data2, json_data3]
                
                # Act
                ingestor = PlatformInstrumentRelationshipIngestor()
                ingestor.process_json_files("/mock/data/dir")
                
                # Assert
                mock_find_json_files.assert_called_once()
                assert mock_json_load.call_count == 3
                # Verify execute_write was not called for invalid data
                assert mock_session.execute_write.call_count == 0
                # Warning should be logged for invalid data
                assert mock_logger.warning.call_count >= 1

    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.find_json_files')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.load_config')
    @patch('os.makedirs')
    def test_process_json_files_with_exceptions(self, mock_makedirs, mock_load_config, mock_setup_logger, mock_get_driver, mock_find_json_files):
        """Test processing JSON files with exceptions."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Set up mock driver and session
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session
        mock_get_driver.return_value = mock_driver
        
        # Mock find_json_files to return list of JSON files
        json_files = ["/mock/data/dir/file1.json", "/mock/data/dir/file2.json"]
        mock_find_json_files.return_value = json_files
        
        # Mock open to raise an exception for one file
        def mock_open_side_effect(*args, **kwargs):
            if args[0] == "/mock/data/dir/file1.json":
                return mock_open().return_value
            else:
                raise FileNotFoundError(f"File not found: {args[0]}")
        
        with patch('builtins.open', side_effect=mock_open_side_effect):
            with patch('json.load') as mock_json_load:
                # Setup json.load to return valid data for the first file
                mock_json_load.return_value = {
                    "Platforms": [
                        {
                            "ShortName": "PLATFORM1",
                            "Instruments": [
                                {"ShortName": "INSTRUMENT1"}
                            ]
                        }
                    ]
                }
                
                # Act
                ingestor = PlatformInstrumentRelationshipIngestor()
                ingestor.process_json_files("/mock/data/dir")
                
                # Assert
                mock_find_json_files.assert_called_once()
                # Verify execute_write was called for valid file
                assert mock_session.execute_write.call_count > 0
                # Verify error was logged for the file with exception
                assert mock_logger.error.call_count >= 1

    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.PlatformInstrumentRelationshipIngestor.process_json_files')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.load_config')
    @patch('os.makedirs')
    def test_run(self, mock_makedirs, mock_load_config, mock_setup_logger, mock_get_driver, mock_process_json_files):
        """Test the run method."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        
        # Act
        ingestor = PlatformInstrumentRelationshipIngestor()
        ingestor.run()
        
        # Assert
        mock_process_json_files.assert_called_once_with(mock_config.paths.dataset_metadata_directory)

    @patch('builtins.print')
    def test_main_function(self, mock_print):
        """Test the main function."""
        with patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.PlatformInstrumentRelationshipIngestor') as MockIngestor:
            mock_ingestor = MagicMock()
            MockIngestor.return_value = mock_ingestor
            
            # Act
            main()
            
            # Assert
            MockIngestor.assert_called_once()
            mock_ingestor.run.assert_called_once()
            mock_ingestor.logger.info.assert_called_with("Relationship creation between Platforms and Instruments completed.")
            mock_print.assert_called_with("Relationship creation process completed.") 