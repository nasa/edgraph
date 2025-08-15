"""Unit tests for Dataset-Platform relationship ingestion."""

import logging
import unittest
from unittest.mock import patch, MagicMock, ANY, call, PropertyMock, mock_open
import uuid
import json

import pytest
from graph_ingest.ingest_scripts.ingest_edge_dataset_platform import DatasetPlatformRelationshipIngestor, main
from graph_ingest.common.core import find_json_files

class TestDatasetPlatformRelationshipIngestor(unittest.TestCase):
    """Test cases for Dataset-Platform relationship ingestion."""

    def setup_mock_config(self):
        """Set up mock configuration."""
        mock_config = MagicMock()
        mock_config.paths.log_directory = "/mock/log/dir"
        mock_config.paths.dataset_metadata_directory = "/mock/data/dir"
        return mock_config

    def test_initialization(self):
        """Test that the ingestor initializes correctly with mock configuration."""
        mock_config = self.setup_mock_config()
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.load_config', return_value=mock_config) as mock_load_config, \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.get_driver') as mock_get_driver, \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.setup_logger') as mock_setup_logger, \
             patch('os.makedirs') as mock_makedirs:
            
            mock_driver = MagicMock()
            mock_get_driver.return_value = mock_driver
            mock_logger = MagicMock()
            mock_setup_logger.return_value = mock_logger
            
            ingestor = DatasetPlatformRelationshipIngestor()
            
            assert ingestor.config == mock_config
            assert ingestor.driver == mock_driver
            assert ingestor.logger == mock_logger
            mock_makedirs.assert_called_once_with(mock_config.paths.log_directory, exist_ok=True)

    def test_generate_uuid_from_doi(self):
        """Test generation of UUID from DOI."""
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.load_config'), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.setup_logger'), \
             patch('os.makedirs'):

            ingestor = DatasetPlatformRelationshipIngestor()
            
            # Test with a valid DOI
            test_doi = "10.5067/CALIOP/CALIPSO/LID_L2_BLOWINGSNOW-ANTARCTICA-STANDARD-V1-00"
            expected_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, test_doi))
            
            actual_uuid = ingestor.generate_uuid_from_doi(test_doi)
            self.assertEqual(expected_uuid, actual_uuid)
            
            # Test with None DOI
            self.assertIsNone(ingestor.generate_uuid_from_doi(None))
            
            # Test with empty DOI
            self.assertIsNone(ingestor.generate_uuid_from_doi(""))

    def test_generate_uuid_from_shortname(self):
        """Test generation of UUID from ShortName."""
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.load_config'), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.setup_logger'), \
             patch('os.makedirs'):

            ingestor = DatasetPlatformRelationshipIngestor()
            
            # Test with a valid ShortName
            test_shortname = "CALIPSO"
            expected_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, test_shortname))
            
            actual_uuid = ingestor.generate_uuid_from_shortname(test_shortname)
            self.assertEqual(expected_uuid, actual_uuid)
            
            # Test with None ShortName
            self.assertIsNone(ingestor.generate_uuid_from_shortname(None))
            
            # Test with empty ShortName
            self.assertIsNone(ingestor.generate_uuid_from_shortname(""))

    def test_create_platform_dataset_relationship(self):
        """Test creating a relationship between dataset and platform."""
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.load_config', return_value=self.setup_mock_config()), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.get_driver') as mock_get_driver, \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.setup_logger') as mock_setup_logger, \
             patch('os.makedirs'):
            
            mock_driver = MagicMock()
            mock_get_driver.return_value = mock_driver
            mock_logger = MagicMock()
            mock_setup_logger.return_value = mock_logger
            
            mock_tx = MagicMock()
            
            # Create simplified mock data for testing
            mock_dataset_id = 'dataset-uuid-123'
            mock_platform_id = 'platform-uuid-456'
            
            # Act
            ingestor = DatasetPlatformRelationshipIngestor()
            ingestor.logger = mock_logger  # Ensure we're using the mock logger
            ingestor.create_platform_dataset_relationship(mock_tx, mock_dataset_id, mock_platform_id)
            
            # Assert
            mock_tx.run.assert_called_once()
            args, kwargs = mock_tx.run.call_args
            assert "MATCH (d:Dataset {globalId: $dataset_uuid}), (p:Platform {globalId: $platform_uuid})" in args[0]
            assert "MERGE (d)-[:HAS_PLATFORM]->(p)" in args[0]
            assert kwargs["dataset_uuid"] == mock_dataset_id
            assert kwargs["platform_uuid"] == mock_platform_id

    def test_process_json_files(self):
        """Test processing a JSON file to create dataset-platform relationships."""
        # Setup
        mock_config = self.setup_mock_config()
        data_dir = mock_config.paths.dataset_metadata_directory
        
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.get_driver') as mock_get_driver, \
            patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.load_config', return_value=mock_config), \
            patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.setup_logger'), \
            patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.find_json_files') as mock_find_json_files, \
            patch('os.makedirs'):
            
            # Mock the driver
            mock_session = MagicMock()
            mock_driver = MagicMock()
            mock_driver.session.return_value.__enter__.return_value = mock_session
            mock_get_driver.return_value = mock_driver
            
            # Mock find_json_files to return a sample file path
            mock_find_json_files.return_value = [f"{data_dir}/file1.json"]
            
            # Create a sample dataset metadata with platform references
            dataset_metadata = {
                "DOI": {"DOI": "10.1234/test"},
                "Platforms": [
                    {"ShortName": "PLATFORM1"},
                    {"ShortName": "PLATFORM2"}
                ]
            }
            
            # Mock open and json.load to return our test data
            with patch('builtins.open', mock_open(read_data=json.dumps(dataset_metadata))), \
                 patch('json.load', return_value=dataset_metadata), \
                 patch('tqdm.tqdm', lambda x, **kwargs: x):
                
                # Create ingestor and process JSON files
                ingestor = DatasetPlatformRelationshipIngestor()
                ingestor.process_json_files(data_dir)
                
                # Verify find_json_files was called with the correct directory
                mock_find_json_files.assert_called_once_with(data_dir)
                
                # Verify the session execute_write was called
                assert mock_session.execute_write.call_count == 1

    def test_process_json_files_with_invalid_data(self):
        """Test processing JSON files with invalid data."""
        # Setup
        mock_config = self.setup_mock_config()
        data_dir = mock_config.paths.dataset_metadata_directory
        
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.get_driver') as mock_get_driver, \
            patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.load_config', return_value=mock_config), \
            patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.setup_logger') as mock_setup_logger, \
            patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.find_json_files') as mock_find_json_files, \
            patch('os.makedirs'):
            
            # Mock the driver
            mock_session = MagicMock()
            mock_driver = MagicMock()
            mock_driver.session.return_value.__enter__.return_value = mock_session
            mock_get_driver.return_value = mock_driver
            
            # Configure mock logger
            mock_logger = MagicMock()
            mock_setup_logger.return_value = mock_logger
            
            # Mock find_json_files to return a sample file path
            mock_find_json_files.return_value = [f"{data_dir}/invalid_file.json"]
            
            # Create a sample dataset metadata with missing platform information
            invalid_dataset_metadata = {
                "DOI": {"DOI": "10.1234/test"}
                # Missing Platforms array
            }
            
            # Mock open and json.load to return our invalid test data
            with patch('builtins.open', mock_open(read_data=json.dumps(invalid_dataset_metadata))), \
                 patch('json.load', return_value=invalid_dataset_metadata), \
                 patch('tqdm.tqdm', lambda x, **kwargs: x):
                
                # Create ingestor and process JSON files
                ingestor = DatasetPlatformRelationshipIngestor()
                ingestor.process_json_files(data_dir)
                
                # Verify find_json_files was called with the correct directory
                mock_find_json_files.assert_called_once_with(data_dir)
                
                # Verify that no relationship was created due to invalid data
                mock_session.execute_write.assert_not_called()
                
                # Verify logging of warnings
                assert mock_logger.warning.call_count >= 1, "Should log at least one warning about invalid data"

    def test_run(self):
        """Test the run method."""
        mock_config = self.setup_mock_config()
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.load_config', return_value=mock_config), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.get_driver') as mock_get_driver, \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.setup_logger') as mock_setup_logger, \
             patch('os.makedirs'):
            
            mock_driver = MagicMock()
            mock_get_driver.return_value = mock_driver
            mock_logger = MagicMock()
            mock_setup_logger.return_value = mock_logger

            with patch.object(DatasetPlatformRelationshipIngestor, 'process_json_files') as mock_process_files:
                # Act
                ingestor = DatasetPlatformRelationshipIngestor()
                ingestor.run()

                # Assert
                mock_process_files.assert_called_once_with(mock_config.paths.dataset_metadata_directory)
                assert ingestor.driver == mock_driver
                assert ingestor.logger == mock_logger

    def test_main(self):
        """Test the main function."""
        mock_config = self.setup_mock_config()
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.load_config', return_value=mock_config), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.get_driver') as mock_get_driver, \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_platform.setup_logger') as mock_setup_logger, \
             patch('os.makedirs'), \
             patch('builtins.print') as mock_print:

            # Configure mocks
            mock_driver = MagicMock()
            mock_get_driver.return_value = mock_driver
            mock_logger = MagicMock()
            mock_setup_logger.return_value = mock_logger

            with patch.object(DatasetPlatformRelationshipIngestor, 'run') as mock_run:
                # Act
                main()

                # Assert
                mock_run.assert_called_once()
                mock_print.assert_called_once_with("Dataset-Platform relationship processing completed.") 