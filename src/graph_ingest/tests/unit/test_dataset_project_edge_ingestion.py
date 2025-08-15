"""Unit tests for Dataset-Project relationship ingestion."""

import os
import json
import logging
from unittest.mock import patch, MagicMock, ANY, call, mock_open

import pytest

from graph_ingest.ingest_scripts.ingest_edge_dataset_project import DatasetProjectRelationshipIngestor, main
from graph_ingest.tests.fixtures.test_data import MOCK_DATASET, MOCK_PROJECT
from graph_ingest.common.config_reader import AppConfig, DatabaseConfig, PathsConfig
from graph_ingest.tests.unit.base_test import BaseIngestorTest
from graph_ingest.tests.utils.mock_helpers import setup_test_file_system
from graph_ingest.common.core import find_json_files

class TestDatasetProjectRelationshipIngestor(BaseIngestorTest):
    """Tests for the DatasetProjectRelationshipIngestor class."""

    def test_initialization(self):
        """Test initialization of DatasetProjectRelationshipIngestor."""
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.load_config', return_value=self.setup_mock_config()), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.get_driver') as mock_get_driver, \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.setup_logger') as mock_setup_logger, \
             patch('os.makedirs'):
            
            mock_driver = MagicMock()
            mock_get_driver.return_value = mock_driver
            mock_logger = MagicMock()
            mock_setup_logger.return_value = mock_logger
            
            ingestor = DatasetProjectRelationshipIngestor()
            
            assert ingestor.config == self.setup_mock_config()
            assert ingestor.driver == mock_driver
            assert ingestor.logger == mock_logger

    def test_generate_uuid_from_shortname(self):
        """Test UUID generation from shortname."""
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.get_driver') as mock_get_driver, \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.load_config', return_value=self.setup_mock_config()), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.setup_logger'), \
             patch('os.makedirs'):
            
            mock_driver = MagicMock()
            mock_get_driver.return_value = mock_driver
            
            ingestor = DatasetProjectRelationshipIngestor()
            uuid1 = ingestor.generate_uuid_from_shortname("PROJECT1")
            uuid2 = ingestor.generate_uuid_from_shortname("PROJECT1")  # Should be the same
            uuid3 = ingestor.generate_uuid_from_shortname("PROJECT2")  # Should be different
            
            assert uuid1 is not None
            assert uuid1 == uuid2  # Same input should produce same UUID
            assert uuid1 != uuid3  # Different input should produce different UUID

    def test_create_relationship(self):
        """Test creating a relationship between dataset and project."""
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.load_config', return_value=self.setup_mock_config()), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.get_driver') as mock_get_driver, \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.setup_logger') as mock_setup_logger, \
             patch('os.makedirs'):
            
            mock_driver = MagicMock()
            mock_get_driver.return_value = mock_driver
            mock_logger = MagicMock()
            mock_setup_logger.return_value = mock_logger
            mock_tx = MagicMock()
            
            # Create simplified mock data for testing
            dataset_globalId = 'dataset-uuid-123'
            project_globalId = 'project-uuid-456'
            
            ingestor = DatasetProjectRelationshipIngestor()
            ingestor.logger = mock_logger  # Ensure we're using our mocked logger
            ingestor.create_relationship(mock_tx, dataset_globalId, project_globalId)
            
            mock_tx.run.assert_called_once()
            args, kwargs = mock_tx.run.call_args
            assert "MATCH (d:Dataset {globalId: $dataset_globalId}), (p:Project {globalId: $project_globalId})" in args[0]
            assert kwargs["dataset_globalId"] == dataset_globalId
            assert kwargs["project_globalId"] == project_globalId

    def test_process_json_files(self):
        """Test processing JSON files to create relationships."""
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.load_config', return_value=self.setup_mock_config()), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.get_driver') as mock_get_driver, \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.setup_logger') as mock_setup_logger, \
             patch('os.makedirs'), \
             patch('builtins.open', mock_open()), \
             patch('json.load'), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.find_json_files') as mock_find_json_files, \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.DatasetProjectRelationshipIngestor.generate_uuid_from_shortname') as mock_generate_uuid, \
             patch('tqdm.tqdm', lambda x, **kwargs: x):
            
            # Configure mock driver
            mock_driver = MagicMock()
            mock_get_driver.return_value = mock_driver
            mock_session = MagicMock()
            mock_driver.session.return_value.__enter__.return_value = mock_session
            mock_tx = MagicMock()
            mock_session.execute_write.side_effect = lambda x: x(mock_tx)
            
            # Configure mock logger
            mock_logger = MagicMock()
            mock_setup_logger.return_value = mock_logger
            
            # Configure mock UUID generation
            mock_generate_uuid.return_value = "test-uuid-123"
            
            # Configure mock file finding
            mock_files = ['/mock/data/dir/file1.json']
            mock_find_json_files.return_value = mock_files
            
            # Configure mock JSON loading
            mock_json_data = {
                "DOI": {"DOI": "10.1234/test1"},
                "Projects": [{"ShortName": "PROJECT1"}]
            }
            json.load.return_value = mock_json_data
            
            # Act
            ingestor = DatasetProjectRelationshipIngestor()
            ingestor.config = self.setup_mock_config()
            ingestor.logger = mock_logger
            ingestor.process_json_files(self.setup_mock_config().paths.dataset_metadata_directory)
            
            # Assert
            mock_find_json_files.assert_called_once_with(self.setup_mock_config().paths.dataset_metadata_directory)
            mock_session.execute_write.assert_called_once()
            assert mock_logger.info.call_count > 0
            
    def test_process_json_files_with_invalid_data(self):
        """Test processing JSON files with invalid data."""
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.load_config', return_value=self.setup_mock_config()), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.get_driver') as mock_get_driver, \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.setup_logger') as mock_setup_logger, \
             patch('os.makedirs'), \
             patch('builtins.open', mock_open()), \
             patch('json.load'), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.find_json_files') as mock_find_json_files, \
             patch('tqdm.tqdm', lambda x, **kwargs: x):
            
            # Configure mock driver
            mock_driver = MagicMock()
            mock_get_driver.return_value = mock_driver
            mock_session = MagicMock()
            mock_driver.session.return_value.__enter__.return_value = mock_session
            mock_tx = MagicMock()
            mock_session.execute_write.side_effect = lambda x: x(mock_tx)
            
            # Configure mock logger
            mock_logger = MagicMock()
            mock_setup_logger.return_value = mock_logger
            
            # Configure mock file finding
            mock_files = ['/mock/data/dir/file1.json']
            mock_find_json_files.return_value = mock_files
            
            # Configure mock JSON loading - missing DOI
            mock_json_data = {
                "Projects": [{"ShortName": "PROJECT1"}]
            }
            json.load.return_value = mock_json_data
            
            # Act
            ingestor = DatasetProjectRelationshipIngestor()
            ingestor.config = self.setup_mock_config()
            ingestor.logger = mock_logger
            ingestor.process_json_files(self.setup_mock_config().paths.dataset_metadata_directory)
            
            # Assert
            # No relationships should be processed
            mock_find_json_files.assert_called_once_with(self.setup_mock_config().paths.dataset_metadata_directory)
            mock_session.execute_write.assert_not_called()
            # Warning should be logged about missing DOI/Projects
            assert mock_logger.warning.call_count > 0

    def test_run(self):
        """Test the run method."""
        mock_config = self.setup_mock_config()
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.load_config', return_value=mock_config), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.get_driver') as mock_get_driver, \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.setup_logger') as mock_setup_logger, \
             patch('os.makedirs'):
            
            mock_driver = MagicMock()
            mock_get_driver.return_value = mock_driver
            mock_logger = MagicMock()
            mock_setup_logger.return_value = mock_logger

            with patch.object(DatasetProjectRelationshipIngestor, 'process_json_files') as mock_process_files:
                # Act
                ingestor = DatasetProjectRelationshipIngestor()
                ingestor.run()

                # Assert
                mock_process_files.assert_called_once_with(mock_config.paths.dataset_metadata_directory)

    def test_main_function(self):
        """Test the main function."""
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.get_driver'), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.setup_logger') as mock_setup_logger, \
             patch('os.makedirs'), \
             patch('graph_ingest.ingest_scripts.ingest_edge_dataset_project.DatasetProjectRelationshipIngestor') as mock_ingestor_class:
            
            mock_logger = MagicMock()
            mock_setup_logger.return_value = mock_logger
            mock_ingestor = MagicMock()
            mock_ingestor_class.return_value = mock_ingestor
            
            main()
            
            mock_ingestor.run.assert_called_once()
            mock_ingestor.logger.info.assert_called_with("Relationship creation between Datasets and Projects completed.") 