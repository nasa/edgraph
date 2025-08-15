"""Unit tests for instrument ingestion."""

import pytest
import os
import json
import logging
from unittest.mock import patch, MagicMock, mock_open, ANY, PropertyMock, call

from graph_ingest.ingest_scripts.ingest_node_instrument import InstrumentIngestor, main
from graph_ingest.tests.fixtures.test_data import MOCK_INSTRUMENT
from graph_ingest.tests.unit.base_test import BaseIngestorTest
# Import the realistic instrument fixture
from graph_ingest.tests.fixtures.generated_test_data import MOCK_INSTRUMENT as REALISTIC_INSTRUMENT

class TestInstrumentIngestor(BaseIngestorTest):
    """Tests for the InstrumentIngestor class."""

    def test_initialization(self):
        """Test initialization of InstrumentIngestor."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_logger = MagicMock(spec=logging.Logger)
        mock_driver = MagicMock()
        
        # Apply patches
        with patch('graph_ingest.ingest_scripts.ingest_node_instrument.setup_logger', return_value=mock_logger) as mock_setup_logger:
            with patch('graph_ingest.ingest_scripts.ingest_node_instrument.load_config', return_value=mock_config):
                with patch('os.makedirs') as mock_makedirs:
                    with patch('graph_ingest.ingest_scripts.ingest_node_instrument.get_driver', return_value=mock_driver) as mock_get_driver:
                        # Act
                        ingestor = InstrumentIngestor()
                        
                        # Assert
                        mock_makedirs.assert_called_once_with('/mock/log/dir', exist_ok=True)
                        mock_setup_logger.assert_called_once()
                        mock_get_driver.assert_called_once()
                        assert ingestor.driver == mock_driver
                        assert ingestor.logger == mock_logger
                        assert ingestor.config == mock_config

    def test_set_instrument_uniqueness_constraint(self):
        """Test setting uniqueness constraint."""
        # Arrange
        with patch('graph_ingest.ingest_scripts.ingest_node_instrument.load_config') as mock_load_config:
            mock_config = self.setup_mock_config()
            mock_load_config.return_value = mock_config
            
            with patch('os.makedirs'):
                with patch('graph_ingest.ingest_scripts.ingest_node_instrument.setup_logger') as mock_setup_logger:
                    with patch('graph_ingest.ingest_scripts.ingest_node_instrument.get_driver') as mock_get_driver:
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
                        ingestor = InstrumentIngestor()
                        ingestor.set_instrument_uniqueness_constraint()
                        
                        # Assert
                        mock_driver.session.assert_called()
                        mock_session.run.assert_called_once_with(
                            "CREATE CONSTRAINT FOR (i:Instrument) REQUIRE i.globalId IS UNIQUE"
                        )
                        mock_logger.info.assert_called_with("Uniqueness constraint on Instrument.globalId set successfully.")

    @patch('graph_ingest.ingest_scripts.ingest_node_instrument.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_node_instrument.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_node_instrument.load_config')
    @patch('os.makedirs')
    def test_generate_uuid_from_shortname(self, mock_makedirs, mock_load_config, mock_setup_logger, mock_get_driver):
        """Test UUID generation from short name."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        
        # Act
        ingestor = InstrumentIngestor()
        uuid1 = ingestor.generate_uuid_from_shortname("TEST_INSTRUMENT")
        uuid2 = ingestor.generate_uuid_from_shortname("TEST_INSTRUMENT")  # Should be the same
        uuid3 = ingestor.generate_uuid_from_shortname("ANOTHER_INSTRUMENT")  # Should be different
        invalid_uuid = ingestor.generate_uuid_from_shortname("")  # Should be None
        
        # Assert
        assert uuid1 is not None
        assert uuid1 == uuid2  # Same input should produce same UUID
        assert uuid1 != uuid3  # Different input should produce different UUID
        assert invalid_uuid is None  # Empty string should return None

    @patch('graph_ingest.ingest_scripts.ingest_node_instrument.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_node_instrument.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_node_instrument.load_config')
    @patch('os.makedirs')
    def test_create_instrument_node(self, mock_makedirs, mock_load_config, mock_setup_logger, mock_get_driver):
        """Test creating an instrument node using realistic data."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        mock_transaction = MagicMock()
        
        # Use the realistic instrument data from the sample
        instrument_data = {
            "globalId": REALISTIC_INSTRUMENT['properties']['globalId'],
            "shortName": REALISTIC_INSTRUMENT['properties']['shortName'],
            "longName": REALISTIC_INSTRUMENT['properties']['longName']
        }
        
        # Act
        ingestor = InstrumentIngestor()
        ingestor.create_instrument_node(mock_transaction, instrument_data)
        
        # Assert
        mock_transaction.run.assert_called_once()
        args, kwargs = mock_transaction.run.call_args
        assert "MERGE (i:Instrument {globalId: $globalId})" in args[0]
        assert kwargs["globalId"] == REALISTIC_INSTRUMENT['properties']['globalId']
        assert kwargs["shortName"] == REALISTIC_INSTRUMENT['properties']['shortName']
        assert kwargs["longName"] == REALISTIC_INSTRUMENT['properties']['longName']

    def test_process_json_files(self):
        """Test processing JSON files."""
        # Arrange
        with patch('graph_ingest.ingest_scripts.ingest_node_instrument.load_config') as mock_load_config:
            mock_config = self.setup_mock_config()
            mock_load_config.return_value = mock_config
            
            with patch('os.makedirs'):
                with patch('graph_ingest.ingest_scripts.ingest_node_instrument.setup_logger') as mock_setup_logger:
                    with patch('graph_ingest.ingest_scripts.ingest_node_instrument.get_driver') as mock_get_driver:
                        with patch('graph_ingest.ingest_scripts.ingest_node_instrument.find_json_files') as mock_find_json_files:
                            with patch('builtins.open', new_callable=mock_open):
                                with patch('json.load') as mock_json_load:
                                    with patch('graph_ingest.ingest_scripts.ingest_node_instrument.tqdm') as mock_tqdm:
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
                                                    "Instruments": [
                                                        {
                                                            "ShortName": "Instrument1",
                                                            "LongName": "Test Instrument 1"
                                                        },
                                                        {
                                                            "ShortName": "Instrument2",
                                                            "LongName": "Test Instrument 2"
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                        mock_json_load.return_value = mock_json_data
                                        
                                        # Mock tqdm to simply return the input
                                        mock_tqdm.side_effect = lambda x, **kwargs: x
                                        
                                        # Act
                                        ingestor = InstrumentIngestor()
                                        ingestor.process_json_files("/mock/data/dir")
                                        
                                        # Assert
                                        mock_find_json_files.assert_called_once_with("/mock/data/dir")
                                        mock_json_load.assert_called_once()
                                        mock_session.execute_write.assert_called_once()
                                        
                                        # Verify logging messages
                                        mock_logger.info.assert_has_calls([
                                            call("Found 1 JSON files in directory: /mock/data/dir"),
                                            call("Total instruments to process: 2"),
                                            call("Processed batch of 2 instruments. Total created so far: 2"),
                                            call("Total instruments created: 2")
                                        ], any_order=False)

    @patch('graph_ingest.ingest_scripts.ingest_node_instrument.InstrumentIngestor.process_json_files')
    @patch('graph_ingest.ingest_scripts.ingest_node_instrument.InstrumentIngestor.set_instrument_uniqueness_constraint')
    @patch('graph_ingest.ingest_scripts.ingest_node_instrument.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_node_instrument.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_node_instrument.load_config')
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
        ingestor = InstrumentIngestor()
        ingestor.run()
        
        # Assert
        mock_set_constraint.assert_called_once()
        mock_process_files.assert_called_once_with("/mock/data/dir")
        
        # Verify final logging message
        mock_logger.info.assert_called_with("Instrument node creation process completed.")

    @patch('graph_ingest.ingest_scripts.ingest_node_instrument.InstrumentIngestor')
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
        mock_print.assert_called_with("Instrument node creation process completed.") 