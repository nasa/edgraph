"""Unit tests for Dataset-ScienceKeyword relationship ingestion."""

import os
import json
import logging
from unittest.mock import patch, MagicMock, mock_open, ANY, call

import pytest

from graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword import DatasetScienceKeywordIngestor, main
from graph_ingest.tests.fixtures.generated_test_data import MOCK_EDGE_HAS_SCIENCEKEYWORD, MOCK_DATASET, MOCK_SCIENCEKEYWORD
from graph_ingest.common.config_reader import AppConfig, DatabaseConfig, PathsConfig

class TestDatasetScienceKeywordIngestor:
    """Tests for the DatasetScienceKeywordIngestor class."""

    def setup_neo4j_mock(self):
        """Set up a standardized Neo4j mock chain.
        
        Returns:
            Tuple of (mock_driver, mock_session, mock_transaction)
        """
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_transaction = MagicMock()
        
        # Set up the chain
        mock_driver.session.return_value = mock_session
        mock_session.execute_write = MagicMock()
        
        return mock_driver, mock_session, mock_transaction

    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.setup_logger')
    def test_initialization(self, mock_setup_logger, mock_load_config, mock_makedirs):
        """Test initialization of DatasetScienceKeywordIngestor."""
        # Arrange
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="password"
            ),
            paths=PathsConfig(
                source_dois_directory="/mock/dois/dir",
                dataset_metadata_directory="/mock/data/dir",
                gcmd_sciencekeyword_directory="/mock/sciencekeyword/dir",
                publications_metadata_directory="/mock/publication/dir",
                pubs_of_pubs="/mock/pubs/dir",
                log_directory="/mock/log/dir"
            )
        )
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock(spec=logging.Logger)
        mock_setup_logger.return_value = mock_logger
        
        # Act
        with patch.dict('os.environ', {
            'NEO4J_URI': 'bolt://mock-neo4j:7687',
            'NEO4J_USER': 'test_user',
            'NEO4J_PASSWORD': 'test_password'
        }):
            ingestor = DatasetScienceKeywordIngestor()
        
        # Assert
        mock_makedirs.assert_called_once_with('/mock/log/dir', exist_ok=True)
        mock_setup_logger.assert_called_once()
        assert ingestor.config == mock_config
        assert ingestor.logger == mock_logger

    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.setup_logger')
    @patch('os.makedirs')
    def test_generate_uuid_from_doi(self, mock_makedirs, mock_setup_logger, mock_load_config):
        """Test UUID generation from DOI."""
        # Arrange
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="password"
            ),
            paths=PathsConfig(
                source_dois_directory="/mock/dois/dir",
                dataset_metadata_directory="/mock/data/dir",
                gcmd_sciencekeyword_directory="/mock/sciencekeyword/dir",
                publications_metadata_directory="/mock/publication/dir",
                pubs_of_pubs="/mock/pubs/dir",
                log_directory="/mock/log/dir"
            )
        )
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Act
        ingestor = DatasetScienceKeywordIngestor()
        uuid1 = ingestor.generate_uuid_from_doi("10.1234/test1")
        uuid2 = ingestor.generate_uuid_from_doi("10.1234/test1")  # Should be the same
        uuid3 = ingestor.generate_uuid_from_doi("10.1234/test2")  # Should be different
        invalid_uuid1 = ingestor.generate_uuid_from_doi("")  # Should be None
        invalid_uuid2 = ingestor.generate_uuid_from_doi(None)  # Should be None
        
        # Assert
        assert uuid1 is not None
        assert uuid1 == uuid2  # Same input should produce same UUID
        assert uuid1 != uuid3  # Different input should produce different UUID
        assert invalid_uuid1 is None  # Empty string should return None
        assert invalid_uuid2 is None  # None input should return None
        assert mock_logger.error.call_count == 2  # Two error logs for invalid inputs

    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.setup_logger')
    @patch('os.makedirs')
    def test_generate_uuid_from_string(self, mock_makedirs, mock_setup_logger, mock_load_config):
        """Test UUID generation from string."""
        # Arrange
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="password"
            ),
            paths=PathsConfig(
                source_dois_directory="/mock/dois/dir",
                dataset_metadata_directory="/mock/data/dir",
                gcmd_sciencekeyword_directory="/mock/sciencekeyword/dir",
                publications_metadata_directory="/mock/publication/dir",
                pubs_of_pubs="/mock/pubs/dir",
                log_directory="/mock/log/dir"
            )
        )
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Act
        ingestor = DatasetScienceKeywordIngestor()
        uuid1 = ingestor.generate_uuid_from_string("EARTH SCIENCE")
        uuid2 = ingestor.generate_uuid_from_string("EARTH SCIENCE")  # Should be the same
        uuid3 = ingestor.generate_uuid_from_string("ATMOSPHERE")  # Should be different
        invalid_uuid1 = ingestor.generate_uuid_from_string("")  # Should be None
        invalid_uuid2 = ingestor.generate_uuid_from_string(None)  # Should be None
        
        # Assert
        assert uuid1 is not None
        assert uuid1 == uuid2  # Same input should produce same UUID
        assert uuid1 != uuid3  # Different input should produce different UUID
        assert invalid_uuid1 is None  # Empty string should return None
        assert invalid_uuid2 is None  # None input should return None
        assert mock_logger.error.call_count == 2  # Two error logs for invalid inputs

    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.setup_logger')
    @patch('os.makedirs')
    def test_create_relationship(self, mock_makedirs, mock_setup_logger, mock_load_config):
        """Test creating a relationship between dataset and science keyword."""
        # Arrange
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="password"
            ),
            paths=PathsConfig(
                source_dois_directory="/mock/dois/dir",
                dataset_metadata_directory="/mock/data/dir",
                gcmd_sciencekeyword_directory="/mock/sciencekeyword/dir",
                publications_metadata_directory="/mock/publication/dir",
                pubs_of_pubs="/mock/pubs/dir",
                log_directory="/mock/log/dir"
            )
        )
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        mock_tx = MagicMock()
        
        # Use realistic data from fixtures
        dataset_uuid = MOCK_DATASET['properties']['globalId']
        keyword_uuid = MOCK_SCIENCEKEYWORD['properties']['globalId']
        
        # Act
        ingestor = DatasetScienceKeywordIngestor()
        ingestor.create_relationship(mock_tx, dataset_uuid, keyword_uuid)
        
        # Assert
        mock_tx.run.assert_called_once()
        args, kwargs = mock_tx.run.call_args
        assert "MATCH (d:Dataset {globalId: $dataset_uuid}), (k:ScienceKeyword {globalId: $keyword_uuid})" in args[0]
        assert "MERGE (d)-[:HAS_SCIENCEKEYWORD]->(k)" in args[0]
        assert kwargs["dataset_uuid"] == dataset_uuid
        assert kwargs["keyword_uuid"] == keyword_uuid

    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.setup_logger')
    @patch('os.makedirs')
    def test_find_json_files(self, mock_makedirs, mock_setup_logger, mock_load_config):
        """Test finding JSON files in a directory."""
        # Arrange
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="password"
            ),
            paths=PathsConfig(
                source_dois_directory="/mock/dois/dir",
                dataset_metadata_directory="/mock/data/dir",
                gcmd_sciencekeyword_directory="/mock/sciencekeyword/dir",
                publications_metadata_directory="/mock/publication/dir",
                pubs_of_pubs="/mock/pubs/dir",
                log_directory="/mock/log/dir"
            )
        )
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        with patch('os.walk') as mock_walk:
            mock_walk.return_value = [
                ('/mock/data', ['dir1', 'dir2'], ['file1.json', 'file2.txt']),
                ('/mock/data/dir1', [], ['file3.json', 'file4.json']),
                ('/mock/data/dir2', [], ['file5.txt', 'file6.json'])
            ]
            
            # Act
            ingestor = DatasetScienceKeywordIngestor()
            json_files = ingestor.find_json_files('/mock/data')
            
            # Assert
            assert len(json_files) == 4
            assert '/mock/data/file1.json' in json_files
            assert '/mock/data/dir1/file3.json' in json_files
            assert '/mock/data/dir1/file4.json' in json_files
            assert '/mock/data/dir2/file6.json' in json_files
            assert '/mock/data/file2.txt' not in json_files
            assert '/mock/data/dir2/file5.txt' not in json_files

    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.setup_logger')
    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.tqdm')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.GraphDatabase')
    def test_process_json_files(self, mock_graph_db, mock_tqdm, mock_makedirs, mock_setup_logger, mock_load_config):
        """Test processing JSON files to create relationships."""
        # Arrange
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="password"
            ),
            paths=PathsConfig(
                source_dois_directory="/mock/dois/dir",
                dataset_metadata_directory="/mock/data/dir",
                gcmd_sciencekeyword_directory="/mock/sciencekeyword/dir",
                publications_metadata_directory="/mock/publication/dir",
                pubs_of_pubs="/mock/pubs/dir",
                log_directory="/mock/log/dir"
            )
        )
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Setup mock driver
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session
        mock_graph_db.driver.return_value.__enter__.return_value = mock_driver
        
        # Mock tqdm to return input unchanged
        mock_tqdm.side_effect = lambda x, **kwargs: x
        
        # Act
        ingestor = DatasetScienceKeywordIngestor()
        
        # Mock find_json_files
        with patch.object(ingestor, 'find_json_files') as mock_find_json_files:
            mock_find_json_files.return_value = ["/mock/data/file1.json", "/mock/data/file2.json"]
            
            # Mock generate_uuid functions
            with patch.object(ingestor, 'generate_uuid_from_doi') as mock_generate_uuid_from_doi, \
                 patch.object(ingestor, 'generate_uuid_from_string') as mock_generate_uuid_from_string:
                
                # Setup test data
                mock_generate_uuid_from_doi.return_value = MOCK_DATASET['properties']['globalId']
                mock_generate_uuid_from_string.return_value = MOCK_SCIENCEKEYWORD['properties']['globalId']
                
                # Mock open and json.load
                json_data1 = {
                    "DOI": {"DOI": "10.1234/test1"},
                    "ScienceKeywords": [
                        {
                            "Topic": "EARTH SCIENCE",
                            "Term": "ATMOSPHERE",
                            "Variable_Level_1": "ATMOSPHERIC TEMPERATURE"
                        }
                    ]
                }
                json_data2 = {
                    "DOI": {"DOI": "10.5678/test2"},
                    "ScienceKeywords": [
                        {
                            "Topic": "EARTH SCIENCE",
                            "Term": "OCEANS"
                        }
                    ]
                }
                
                with patch('builtins.open', mock_open()) as m, \
                     patch('json.load') as mock_json_load:
                    mock_json_load.side_effect = [json_data1, json_data2]
                    
                    # Act
                    ingestor.process_json_files("/mock/data/dir", batch_size=2)
                    
                    # Assert
                    mock_find_json_files.assert_called_once()
                    assert mock_json_load.call_count == 2
                    # Verify execute_write was called for each relationship
                    assert mock_session.execute_write.call_count > 0
                    # Verify logger info calls for processing
                    assert mock_logger.info.call_count >= 4
                    # Check for final info logging
                    assert any("Total relationships processed" in str(call) for call in mock_logger.info.call_args_list)
                    assert any("Relationships created" in str(call) for call in mock_logger.info.call_args_list)

    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.setup_logger')
    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.tqdm')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.GraphDatabase')
    def test_process_json_files_with_invalid_data(self, mock_graph_db, mock_tqdm, mock_makedirs, mock_setup_logger, mock_load_config):
        """Test processing JSON files with invalid data."""
        # Arrange
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="password"
            ),
            paths=PathsConfig(
                source_dois_directory="/mock/dois/dir",
                dataset_metadata_directory="/mock/data/dir",
                gcmd_sciencekeyword_directory="/mock/sciencekeyword/dir",
                publications_metadata_directory="/mock/publication/dir",
                pubs_of_pubs="/mock/pubs/dir",
                log_directory="/mock/log/dir"
            )
        )
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Setup mock driver
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session
        mock_graph_db.driver.return_value.__enter__.return_value = mock_driver
        
        # Mock tqdm to return input unchanged
        mock_tqdm.side_effect = lambda x, **kwargs: x
        
        # Act
        ingestor = DatasetScienceKeywordIngestor()
        
        # Mock find_json_files
        with patch.object(ingestor, 'find_json_files') as mock_find_json_files:
            mock_find_json_files.return_value = ["/mock/data/file1.json", "/mock/data/file2.json", "/mock/data/file3.json"]
            
            # Mock generate_uuid functions
            with patch.object(ingestor, 'generate_uuid_from_doi') as mock_generate_uuid_from_doi, \
                 patch.object(ingestor, 'generate_uuid_from_string') as mock_generate_uuid_from_string:
                
                # Setup test data with some invalid results
                mock_generate_uuid_from_doi.side_effect = [MOCK_DATASET['properties']['globalId'], None, MOCK_DATASET['properties']['globalId']]
                mock_generate_uuid_from_string.side_effect = [MOCK_SCIENCEKEYWORD['properties']['globalId'], None]
                
                # Mock open and json.load
                json_data1 = {
                    "DOI": {"DOI": "10.1234/test1"},
                    "ScienceKeywords": [
                        {
                            "Topic": "EARTH SCIENCE",
                            "Term": "ATMOSPHERE",
                        }
                    ]
                }
                json_data2 = {
                    # Missing DOI
                    "ScienceKeywords": [
                        {
                            "Topic": "EARTH SCIENCE"
                        }
                    ]
                }
                json_data3 = {
                    "DOI": {"DOI": "10.9012/test3"},
                    # Missing ScienceKeywords array
                }
                
                with patch('builtins.open', mock_open()) as m, \
                     patch('json.load') as mock_json_load:
                    mock_json_load.side_effect = [json_data1, json_data2, json_data3]
                    
                    # Act
                    ingestor.process_json_files("/mock/data/dir")
                    
                    # Assert
                    mock_find_json_files.assert_called_once()
                    assert mock_json_load.call_count == 3
                    # Verify execute_write was called less times due to invalid data
                    assert mock_session.execute_write.call_count < 3
                    # Check for warnings
                    assert mock_logger.warning.call_count >= 1

    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.setup_logger')
    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.GraphDatabase')
    def test_process_json_files_with_exceptions(self, mock_graph_db, mock_makedirs, mock_setup_logger, mock_load_config):
        """Test processing JSON files with exceptions."""
        # Arrange
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="password"
            ),
            paths=PathsConfig(
                source_dois_directory="/mock/dois/dir",
                dataset_metadata_directory="/mock/data/dir",
                gcmd_sciencekeyword_directory="/mock/sciencekeyword/dir",
                publications_metadata_directory="/mock/publication/dir",
                pubs_of_pubs="/mock/pubs/dir",
                log_directory="/mock/log/dir"
            )
        )
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Setup mock driver
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session
        mock_graph_db.driver.return_value.__enter__.return_value = mock_driver
        
        # Act
        ingestor = DatasetScienceKeywordIngestor()
        
        # Mock find_json_files
        with patch.object(ingestor, 'find_json_files') as mock_find_json_files:
            mock_find_json_files.return_value = ["/mock/data/file1.json", "/mock/data/file2.json"]
            
            # Mock open to raise exception for second file
            def mock_open_side_effect(*args, **kwargs):
                if args[0] == "/mock/data/file1.json":
                    return mock_open().return_value
                else:
                    raise FileNotFoundError(f"File not found: {args[0]}")
            
            with patch('builtins.open', side_effect=mock_open_side_effect), \
                 patch('json.load') as mock_json_load, \
                 patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.tqdm', lambda x, **kwargs: x):
                
                # Setup json.load to return valid data for the first file
                mock_json_load.return_value = {
                    "DOI": {"DOI": "10.1234/test1"},
                    "ScienceKeywords": [
                        {
                            "Topic": "EARTH SCIENCE"
                        }
                    ]
                }
                
                # Setup UUID generation to succeed
                with patch.object(ingestor, 'generate_uuid_from_doi') as mock_generate_uuid_from_doi, \
                     patch.object(ingestor, 'generate_uuid_from_string') as mock_generate_uuid_from_string:
                    
                    mock_generate_uuid_from_doi.return_value = MOCK_DATASET['properties']['globalId']
                    mock_generate_uuid_from_string.return_value = MOCK_SCIENCEKEYWORD['properties']['globalId']
                    
                    # Act
                    ingestor.process_json_files("/mock/data/dir")
                    
                    # Assert
                    mock_find_json_files.assert_called_once()
                    assert mock_json_load.call_count == 1  # Only one file successfully loaded
                    # Error should be logged for the failed file
                    assert mock_logger.error.call_count >= 1
                    # Check for error about failed files
                    assert any("Failed to process file" in str(call) for call in mock_logger.error.call_args_list)

    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.DatasetScienceKeywordIngestor.process_json_files')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.setup_logger')
    @patch('os.makedirs')
    def test_run(self, mock_makedirs, mock_setup_logger, mock_load_config, mock_process_json_files):
        """Test the run method."""
        # Arrange
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="password"
            ),
            paths=PathsConfig(
                source_dois_directory="/mock/dois/dir",
                dataset_metadata_directory="/mock/data/dir",
                gcmd_sciencekeyword_directory="/mock/sciencekeyword/dir",
                publications_metadata_directory="/mock/publication/dir",
                pubs_of_pubs="/mock/pubs/dir",
                log_directory="/mock/log/dir"
            )
        )
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Act
        ingestor = DatasetScienceKeywordIngestor()
        ingestor.run()
        
        # Assert
        mock_process_json_files.assert_called_once_with(mock_config.paths.dataset_metadata_directory)

    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.setup_logger')
    def test_main_function(self, mock_setup_logger, mock_load_config):
        """Test the main function."""
        # Arrange
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="password"
            ),
            paths=PathsConfig(
                source_dois_directory="/mock/dois/dir",
                dataset_metadata_directory="/mock/data/dir",
                gcmd_sciencekeyword_directory="/mock/sciencekeyword/dir",
                publications_metadata_directory="/mock/publication/dir",
                pubs_of_pubs="/mock/pubs/dir",
                log_directory="/mock/log/dir"
            )
        )
        mock_load_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        with patch('graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword.DatasetScienceKeywordIngestor') as mock_ingestor_class:
            mock_ingestor = MagicMock()
            mock_ingestor_class.return_value = mock_ingestor
            mock_ingestor.logger = mock_logger
            
            # Act
            main()
            
            # Assert
            mock_ingestor_class.assert_called_once()
            mock_ingestor.run.assert_called_once()

    def setup_mock_config(self):
        """Create a mock configuration for testing."""
        return AppConfig(
            database=DatabaseConfig(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="password"
            ),
            paths=PathsConfig(
                source_dois_directory="/mock/dois/dir",
                dataset_metadata_directory="/mock/data/dir",
                gcmd_sciencekeyword_directory="/mock/sciencekeyword/dir",
                publications_metadata_directory="/mock/publication/dir",
                pubs_of_pubs="/mock/pubs/dir",
                log_directory="/mock/log/dir"
            )
        ) 