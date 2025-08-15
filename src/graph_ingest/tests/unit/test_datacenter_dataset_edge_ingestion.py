"""Unit tests for DataCenter-Dataset relationship ingestion."""

import os
import json
import logging
from unittest.mock import patch, MagicMock, mock_open, ANY, call

import pytest

from graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset import RelationshipIngestor, main
from graph_ingest.tests.fixtures.generated_test_data import MOCK_EDGE_HAS_DATASET, MOCK_DATASET, MOCK_DATACENTER
from graph_ingest.common.config_reader import AppConfig, DatabaseConfig, PathsConfig
from graph_ingest.tests.unit.base_test import BaseIngestorTest

class TestDatacenterDatasetRelationshipIngestor(BaseIngestorTest):
    """Tests for the RelationshipIngestor class for DataCenter-Dataset relationships."""

    def setup_mock_config(self):
        """Return a properly formatted mock config using dataclasses."""
        return AppConfig(
            database=DatabaseConfig(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="password"
            ),
            paths=PathsConfig(
                source_dois_directory="/mock/source/dir",
                dataset_metadata_directory="/mock/data/dir",
                gcmd_sciencekeyword_directory="/mock/science/dir",
                publications_metadata_directory="/mock/pubs/dir",
                pubs_of_pubs="/mock/pubs_of_pubs/dir",
                log_directory="/mock/log/dir"
            )
        )

    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.get_driver')
    def test_initialization(self, mock_get_driver, mock_setup_logger, mock_load_config, mock_makedirs):
        """Test initialization of RelationshipIngestor."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock(spec=logging.Logger)
        mock_setup_logger.return_value = mock_logger
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        
        # Act
        ingestor = RelationshipIngestor()
        
        # Assert
        mock_makedirs.assert_called_once_with('/mock/log/dir', exist_ok=True)
        mock_setup_logger.assert_called_once()
        mock_get_driver.assert_called_once()
        assert ingestor.driver == mock_driver
        assert ingestor.logger == mock_logger
        assert ingestor.config == mock_config

    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.load_config')
    @patch('os.makedirs')
    def test_batch_create_relationships(self, mock_makedirs, mock_load_config, mock_setup_logger, mock_get_driver):
        """Test batch creating relationships between datacenter and dataset."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        mock_tx = MagicMock()
        
        # Use realistic data from fixtures
        datacenter_id = MOCK_DATACENTER['properties']['globalId']
        dataset_id = MOCK_DATASET['properties']['globalId']
        relationships = [
            {"dataCenterId": datacenter_id, "datasetId": dataset_id},
            {"dataCenterId": datacenter_id, "datasetId": "another-dataset-id"}
        ]
        
        # Act
        ingestor = RelationshipIngestor()
        ingestor.batch_create_relationships(mock_tx, relationships)
        
        # Assert
        mock_tx.run.assert_called_once()
        args, kwargs = mock_tx.run.call_args
        assert "UNWIND $rels as rel" in args[0]
        assert "MATCH (dc:DataCenter {globalId: rel.dataCenterId}), (ds:Dataset {globalId: rel.datasetId})" in args[0]
        assert "MERGE (dc)-[:HAS_DATASET]->(ds)" in args[0]
        assert kwargs["rels"] == relationships

    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.find_json_files')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.load_config')
    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.tqdm')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.generate_uuid_from_doi')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.generate_uuid_from_name')
    def test_process_files(self, mock_generate_uuid_from_name, mock_generate_uuid_from_doi, 
                          mock_tqdm, mock_makedirs, mock_load_config, mock_setup_logger, 
                          mock_get_driver, mock_find_json_files):
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
        
        # Mock UUID generation
        mock_generate_uuid_from_doi.return_value = MOCK_DATASET['properties']['globalId']
        mock_generate_uuid_from_name.return_value = MOCK_DATACENTER['properties']['globalId']
        
        # Create sample JSON data
        json_data1 = {
            "DOI": {"DOI": "10.1234/test1"},
            "DataCenters": [
                {"ShortName": "NASA/DATACENTER1"},
                {"ShortName": "NASA/DATACENTER2"}
            ]
        }
        json_data2 = {
            "DOI": {"DOI": "10.5678/test2"},
            "DataCenters": [
                {"ShortName": "NASA/DATACENTER3"}
            ]
        }
        
        # Setup open to return file handles for the JSON files
        with patch('builtins.open', mock_open()) as m:
            # Setup json.load to return the test data
            with patch('json.load') as mock_json_load:
                mock_json_load.side_effect = [json_data1, json_data2]
                
                # Act
                ingestor = RelationshipIngestor()
                ingestor.process_files("/mock/data/dir", batch_size=5)  # Small batch size for testing
                
                # Assert
                mock_find_json_files.assert_called_once()
                assert mock_json_load.call_count == 2
                # Verify execute_write was called to create relationships
                assert mock_session.execute_write.call_count > 0
                # Verify the correct batch sizes were processed
                assert mock_logger.info.call_count >= 4  # Multiple info logs including batch processing
                # Check for successful completion logs
                assert any("All files processed successfully" in str(call) for call in mock_logger.info.call_args_list)

    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.find_json_files')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.load_config')
    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.tqdm')
    def test_process_files_with_invalid_data(self, mock_tqdm, mock_makedirs, mock_load_config, 
                                          mock_setup_logger, mock_get_driver, mock_find_json_files):
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
            # Missing DOI
            "DataCenters": [
                {"ShortName": "NASA/DATACENTER1"}
            ]
        }
        json_data2 = {
            "DOI": {"DOI": "10.5678/test2"},
            # Missing DataCenters array
        }
        json_data3 = {
            "DOI": {"DOI": "10.9012/test3"},
            "DataCenters": [
                # Missing ShortName
                {"LongName": "Data Center Three"}
            ]
        }
        
        # Setup open to return file handles for the JSON files
        with patch('builtins.open', mock_open()) as m:
            # Setup json.load to return the test data
            with patch('json.load') as mock_json_load:
                mock_json_load.side_effect = [json_data1, json_data2, json_data3]
                
                # Act
                ingestor = RelationshipIngestor()
                ingestor.process_files("/mock/data/dir")
                
                # Assert
                mock_find_json_files.assert_called_once()
                assert mock_json_load.call_count == 3
                # No batch should be processed, as all files have invalid data
                assert mock_session.execute_write.call_count == 0
                # Check for warning about failed files
                assert any("All files processed successfully" not in str(call) for call in mock_logger.info.call_args_list)

    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.find_json_files')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.load_config')
    @patch('os.makedirs')
    def test_process_files_with_exceptions(self, mock_makedirs, mock_load_config, 
                                        mock_setup_logger, mock_get_driver, mock_find_json_files):
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
                    "DOI": {"DOI": "10.1234/test1"},
                    "DataCenters": [
                        {"ShortName": "NASA/DATACENTER1"}
                    ]
                }
                
                # Act
                ingestor = RelationshipIngestor()
                ingestor.process_files("/mock/data/dir")
                
                # Assert
                mock_find_json_files.assert_called_once()
                assert mock_json_load.call_count == 1  # Only one file successfully loaded
                # Error should be logged for the failed file
                assert mock_logger.error.call_count >= 1
                # Check for warning about failed files
                assert any("Failed to process" in str(call) for call in mock_logger.error.call_args_list)

    @patch('builtins.print')
    def test_main_function(self, mock_print):
        """Test the main function."""
        # Arrange
        with patch('graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset.RelationshipIngestor') as mock_ingestor_class:
            mock_ingestor = MagicMock()
            mock_ingestor_class.return_value = mock_ingestor
            mock_ingestor.config = self.setup_mock_config()
            
            # Act
            main()
            
            # Assert
            mock_ingestor_class.assert_called_once()
            mock_ingestor.process_files.assert_called_once_with("/mock/data/dir")
            mock_ingestor.logger.info.assert_called_with("Relationship creation process completed.")
            mock_print.assert_called_with("Relationship creation process completed.") 