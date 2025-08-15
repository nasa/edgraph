"""Unit tests for Publication-Publication relationship ingestion.

This test suite covers the PublicationsOfPublicationsIngestor class, which is responsible for
creating CITES relationships between Publication nodes in Neo4j. The tests focus on:

1. Initialization of the ingestor
2. UUID generation from DOI strings
3. Checking if publication nodes exist
4. Creation of publication nodes
5. Creation of CITES relationships between publications
6. Data chunking for parallel processing
7. Processing of publication nodes
8. Processing of CITES relationships
9. Parallel processing of publications
10. The main execution flow

These tests ensure that the publication-publication relationship ingestion process correctly
creates nodes and relationships based on citation data.
"""

import os
import json
import logging
import uuid
from unittest.mock import patch, MagicMock, mock_open, ANY, call
from concurrent.futures import Future
import sys

import pytest
from neo4j import exceptions

from graph_ingest.ingest_scripts.ingest_node_edge_publications_publications import PublicationsOfPublicationsIngestor, main
from graph_ingest.common.config_reader import AppConfig, DatabaseConfig, PathsConfig
from graph_ingest.tests.fixtures.generated_test_data import MOCK_PUBLICATION

class TestPublicationsOfPublicationsIngestor:
    """Tests for the PublicationsOfPublicationsIngestor class."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.sample_publication_data = {
            "doi": "10.1234/test-doi",
            "title": "Test Publication",
            "year": 2023,
            "abstract": "This is a test abstract",
            "authors": ["Test Author"]
        }
        
        self.sample_citing_doi = "10.1234/citing-doi"
        self.sample_cited_doi = "10.1234/cited-doi"
        
        # Sample data for the JSON file
        self.sample_pubs_of_pubs_data = {
            self.sample_citing_doi: [
                {
                    "doi": self.sample_cited_doi,
                    "title": "Cited Publication",
                    "year": 2022,
                    "abstract": "This is a cited publication",
                    "authors": ["Cited Author"]
                }
            ]
        }

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
        mock_session.execute_read = MagicMock()
        mock_session.execute_write = MagicMock()
        
        return mock_driver, mock_session, mock_transaction

    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.setup_logger')
    @patch('logging.FileHandler')
    def test_initialization(self, mock_file_handler, mock_setup_logger, mock_load_config, mock_makedirs):
        """Test the initialization of the PublicationsOfPublicationsIngestor."""
        # Configure the mock
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://neo4j:7687",
                user="neo4j",
                password="test"
            ),
            paths=PathsConfig(
                source_dois_directory="/test/data/dois",
                dataset_metadata_directory="/test/data/datasets",
                gcmd_sciencekeyword_directory="/test/data/keywords",
                publications_metadata_directory="/test/data/publications",
                pubs_of_pubs="/test/data/pubs_of_pubs.json",
                log_directory="/test/log/directory"
            )
        )
        mock_load_config.return_value = mock_config
        mock_setup_logger.return_value = MagicMock()
        
        # Create the ingestor
        ingestor = PublicationsOfPublicationsIngestor()
        
        # Verify the initialization
        mock_load_config.assert_called_once()
        mock_makedirs.assert_called_once_with("/test/log/directory", exist_ok=True)
        mock_setup_logger.assert_called_once()
        
        # Check that the configuration was properly set
        assert ingestor.config == mock_load_config.return_value
        assert ingestor.log_directory == "/test/log/directory"
        assert ingestor.logger == mock_setup_logger.return_value
        assert ingestor.neo4j_uri == "bolt://neo4j:7687"
        assert ingestor.neo4j_user == "neo4j"
        assert ingestor.neo4j_password == "password"  # Default value from env

    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.setup_logger')
    @patch('logging.FileHandler')
    def test_generate_uuid_from_doi(self, mock_file_handler, mock_setup_logger, mock_load_config, mock_makedirs):
        """Test the generation of UUID from DOI."""
        # Configure the mock
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://neo4j:7687",
                user="neo4j",
                password="test"
            ),
            paths=PathsConfig(
                source_dois_directory="/test/data/dois",
                dataset_metadata_directory="/test/data/datasets",
                gcmd_sciencekeyword_directory="/test/data/keywords",
                publications_metadata_directory="/test/data/publications",
                pubs_of_pubs="/test/data/pubs_of_pubs.json",
                log_directory="/test/log/directory"
            )
        )
        mock_load_config.return_value = mock_config
        mock_setup_logger.return_value = MagicMock()
        
        # Create the ingestor
        ingestor = PublicationsOfPublicationsIngestor()
        
        # Test valid DOI
        doi = "10.1234/test-doi"
        expected_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, doi))
        actual_uuid = ingestor.generate_uuid_from_doi(doi)
        assert actual_uuid == expected_uuid
        
        # Test empty DOI
        assert ingestor.generate_uuid_from_doi("") is None
        
        # Test non-string DOI
        assert ingestor.generate_uuid_from_doi(123) is None
        
        # Test None DOI
        assert ingestor.generate_uuid_from_doi(None) is None

    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.setup_logger')
    @patch('logging.FileHandler')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.GraphDatabase')
    def test_publication_exists(self, mock_graph_database, mock_file_handler, mock_setup_logger, mock_load_config, mock_makedirs):
        """Test checking if a publication exists."""
        # Configure the mocks
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://neo4j:7687",
                user="neo4j",
                password="test"
            ),
            paths=PathsConfig(
                source_dois_directory="/test/data/dois",
                dataset_metadata_directory="/test/data/datasets",
                gcmd_sciencekeyword_directory="/test/data/keywords",
                publications_metadata_directory="/test/data/publications",
                pubs_of_pubs="/test/data/pubs_of_pubs.json",
                log_directory="/test/log/directory"
            )
        )
        mock_load_config.return_value = mock_config
        mock_setup_logger.return_value = MagicMock()
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_result = MagicMock()
        mock_tx.run.return_value = mock_result
        
        # Create the ingestor
        ingestor = PublicationsOfPublicationsIngestor()
        
        # Test when publication exists
        mock_result.single.return_value = {"pub": "data"}
        assert PublicationsOfPublicationsIngestor.publication_exists(mock_tx, "test-global-id") is True
        mock_tx.run.assert_called_with("MATCH (pub:Publication {globalId: $globalId}) RETURN pub", globalId="test-global-id")
        
        # Test when publication does not exist
        mock_result.single.return_value = None
        assert PublicationsOfPublicationsIngestor.publication_exists(mock_tx, "test-global-id") is False

    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.setup_logger')
    @patch('logging.FileHandler')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.GraphDatabase')
    def test_create_publication_node(self, mock_graph_db, mock_file_handler, mock_setup_logger, mock_load_config, mock_makedirs):
        """Test creating a publication node."""
        # Configure the mocks
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://neo4j:7687",
                user="neo4j",
                password="test"
            ),
            paths=PathsConfig(
                source_dois_directory="/test/data/dois",
                dataset_metadata_directory="/test/data/datasets",
                gcmd_sciencekeyword_directory="/test/data/keywords",
                publications_metadata_directory="/test/data/publications",
                pubs_of_pubs="/test/data/pubs_of_pubs.json",
                log_directory="/test/log/directory"
            )
        )
        mock_load_config.return_value = mock_config
        mock_setup_logger.return_value = MagicMock()
    
        # Create the ingestor
        ingestor = PublicationsOfPublicationsIngestor()
    
        # Create the mock transaction
        mock_tx = MagicMock()
        publication = self.sample_publication_data.copy()
        
        # Add the globalId to the publication data
        publication["globalId"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, publication["doi"]))
    
        # Call the method directly to test normal creation
        PublicationsOfPublicationsIngestor.create_publication_node(mock_tx, publication)
    
        # Verify the transaction
        mock_tx.run.assert_called_once()
        # Check that the query parameter contained all the required publication data
        args, kwargs = mock_tx.run.call_args
        assert kwargs["doi"] == publication["doi"]
        assert kwargs["title"] == publication["title"]
        assert kwargs["year"] == publication["year"]
        assert kwargs["abstract"] == publication["abstract"]
        assert kwargs["globalId"] == publication["globalId"]

    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.setup_logger')
    @patch('logging.FileHandler')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.GraphDatabase')
    def test_process_publication_nodes(self, mock_graph_db, mock_file_handler, mock_setup_logger, mock_load_config, mock_makedirs):
        """Test processing publication nodes."""
        # Create a mock driver and session
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session
        
        # Create mock data with one citing publication and one cited publication
        batch = [
            ("10.1000/test1", [{"doi": "10.1000/test2", "title": "Test Paper 2"}])
        ]
        
        # Set up session.execute_read to appropriately handle publication_exists calls
        # First call should be for citing publication, return False (not exists)
        # Second call should be for cited publication, return False (not exists)
        mock_session.execute_read.side_effect = [False, False]
        
        # Track execute_write calls to verify they're correctly made
        write_calls = []
        def track_write_calls(func, *args, **kwargs):
            write_calls.append((func, args, kwargs))
            return None
        
        mock_session.execute_write.side_effect = track_write_calls
        
        # Create instance and mock GraphDatabase.driver
        ingestor = PublicationsOfPublicationsIngestor()
        
        # Run the method with our mocked components
        with patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.GraphDatabase') as mock_graph_db:
            mock_graph_db.driver.return_value = mock_driver
            result = ingestor.process_nodes_batch(batch, "bolt://test", "test", "test")
        
        # Verify execute_read was called twice, once for each publication
        assert mock_session.execute_read.call_count == 2
        
        # Verify execute_write was called twice, once for each publication
        assert mock_session.execute_write.call_count == 2
        
        # Verify the result contains the correct counts
        assert result == {"publication_nodes_created": 2, "existing_publication_nodes": 0}

    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.setup_logger')
    @patch('logging.FileHandler')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.GraphDatabase')
    def test_process_cites_relationships(self, mock_graph_db, mock_file_handler, mock_setup_logger, mock_load_config, mock_makedirs):
        """Test processing CITES relationships."""
        # Create a mock driver and session
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session
        
        # Create mock data with one citing publication and one cited publication
        batch = [
            ("10.1000/test1", [{"doi": "10.1000/test2", "title": "Test Paper 2"}])
        ]
        
        # Set up session.execute_read to appropriately handle publication_exists calls
        # Both publications exist
        mock_session.execute_read.side_effect = [True, True]
        
        # Track execute_write calls to verify they're correctly made
        write_calls = []
        def track_write_calls(func, *args, **kwargs):
            write_calls.append((func, args, kwargs))
            return None
        
        mock_session.execute_write.side_effect = track_write_calls
        
        # Create instance and mock GraphDatabase.driver
        ingestor = PublicationsOfPublicationsIngestor()
        
        # Run the method with our mocked components
        with patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.GraphDatabase') as mock_graph_db:
            mock_graph_db.driver.return_value = mock_driver
            result = ingestor.process_relationships_batch(batch, "bolt://test", "test", "test")
        
        # Verify execute_read was called twice, once for each publication
        assert mock_session.execute_read.call_count == 2
        
        # Verify execute_write was called once to create the relationship
        assert mock_session.execute_write.call_count == 1
        
        # Verify the result contains the correct counts
        assert result == {"cites_relationships_created": 1, "cited_publications_not_found": 0}

    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.setup_logger')
    @patch('logging.FileHandler')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.Pool')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.tqdm')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.open', new_callable=mock_open)
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.json.load')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.cpu_count')
    def test_process_publications_parallel(self, mock_cpu_count, mock_json_load, mock_open, mock_tqdm, mock_pool_class, mock_file_handler, mock_setup_logger, mock_load_config, mock_makedirs):
        """Test the process_publications_parallel method."""
        # Configure the mocks
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://neo4j:7687",
                user="neo4j",
                password="test"
            ),
            paths=PathsConfig(
                source_dois_directory="/test/data/dois",
                dataset_metadata_directory="/test/data/datasets",
                gcmd_sciencekeyword_directory="/test/data/keywords",
                publications_metadata_directory="/test/data/publications",
                pubs_of_pubs="/test/data/pubs_of_pubs.json",
                log_directory="/test/log/directory"
            )
        )
        mock_load_config.return_value = mock_config
        mock_setup_logger.return_value = MagicMock()
        
        # JSON data
        mock_json_load.return_value = self.sample_pubs_of_pubs_data
        
        # Create the ingestor
        ingestor = PublicationsOfPublicationsIngestor()
        
        # Mock Pool
        mock_pool = MagicMock()
        mock_pool_class.return_value.__enter__.return_value = mock_pool
        
        # Mock results from workers
        mock_result1 = {"publication_nodes_created": 5, "existing_publication_nodes": 2}
        mock_result2 = {"cites_relationships_created": 4, "cited_publications_not_found": 1}
        
        # Configure pool's imap_unordered
        mock_pool.imap_unordered.return_value = [mock_result1]
        
        # Set CPU count to something deterministic
        mock_cpu_count.return_value = 4
        
        # Configure tqdm for progress reporting
        mock_tqdm.return_value = [mock_result1]
        
        # Test method
        ingestor.process_publications_parallel()
        
        # Verify that the file was opened and JSON was loaded
        mock_open.assert_called_once_with("/test/data/pubs_of_pubs.json", "r")
        mock_json_load.assert_called_once()
        
        # Verify Pool was created with correct number of workers
        mock_pool_class.assert_called_with(processes=2)  # Should be cpu_count() // 2
        
        # Verify processes were started
        assert mock_pool.imap_unordered.call_count == 2  # Once for nodes, once for relationships
        
        # Verify tqdm was used for progress
        assert mock_tqdm.call_count >= 1
        
        # Verify logs occurred
        ingestor.logger.info.assert_called()

    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.PublicationsOfPublicationsIngestor')
    def test_main_function(self, mock_ingestor_class):
        """Test the main function."""
        # Configure the mock
        mock_ingestor = MagicMock()
        mock_ingestor_class.return_value = mock_ingestor
        
        # Configure the logger to capture messages
        mock_ingestor.logger = MagicMock()
        
        # Call main
        main()
        
        # Verify that the ingestor was created and run was called
        mock_ingestor_class.assert_called_once()
        mock_ingestor.run.assert_called_once()
        
        # The message is printed directly via print(), not through logger
        # So we don't need to assert on logger.info

    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.setup_logger')
    @patch('logging.FileHandler')
    def test_create_cites_relationship(self, mock_file_handler, mock_setup_logger, mock_load_config, mock_makedirs):
        """Test creating a cites relationship."""
        # Configure the mocks
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://neo4j:7687",
                user="neo4j",
                password="test"
            ),
            paths=PathsConfig(
                source_dois_directory="/test/data/dois",
                dataset_metadata_directory="/test/data/datasets",
                gcmd_sciencekeyword_directory="/test/data/keywords",
                publications_metadata_directory="/test/data/publications",
                pubs_of_pubs="/test/data/pubs_of_pubs.json",
                log_directory="/test/log/directory"
            )
        )
        mock_load_config.return_value = mock_config
        mock_setup_logger.return_value = MagicMock()
        
        # Create a mock transaction
        mock_tx = MagicMock()
        
        # Create the ingestor
        ingestor = PublicationsOfPublicationsIngestor()
        
        # Test creating a relationship
        citing_globalId = str(uuid.uuid5(uuid.NAMESPACE_DNS, self.sample_citing_doi))
        cited_globalId = str(uuid.uuid5(uuid.NAMESPACE_DNS, self.sample_cited_doi))
        
        PublicationsOfPublicationsIngestor.create_cites_relationship(mock_tx, citing_globalId, cited_globalId)
        
        # Verify the transaction
        mock_tx.run.assert_called_once()
        args, kwargs = mock_tx.run.call_args
        assert kwargs["citing_globalId"] == citing_globalId
        assert kwargs["cited_globalId"] == cited_globalId

    @patch('os.makedirs')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_node_edge_publications_publications.setup_logger')
    @patch('logging.FileHandler')
    def test_chunk_data(self, mock_file_handler, mock_setup_logger, mock_load_config, mock_makedirs):
        """Test chunking data for parallel processing."""
        # Configure the mocks
        mock_config = AppConfig(
            database=DatabaseConfig(
                uri="bolt://neo4j:7687",
                user="neo4j",
                password="test"
            ),
            paths=PathsConfig(
                source_dois_directory="/test/data/dois",
                dataset_metadata_directory="/test/data/datasets",
                gcmd_sciencekeyword_directory="/test/data/keywords",
                publications_metadata_directory="/test/data/publications",
                pubs_of_pubs="/test/data/pubs_of_pubs.json",
                log_directory="/test/log/directory"
            )
        )
        mock_load_config.return_value = mock_config
        mock_setup_logger.return_value = MagicMock()
        
        # Create the ingestor
        ingestor = PublicationsOfPublicationsIngestor()
        
        # Create test data
        data = {
            "doi1": "data1",
            "doi2": "data2",
            "doi3": "data3",
            "doi4": "data4"
        }
        
        # Test chunking with chunk size 2
        chunks = list(ingestor.chunk_data(data, 2))
        assert len(chunks) == 2
        assert len(chunks[0]) == 2
        assert len(chunks[1]) == 2
        
        # Test chunking with chunk size 3
        chunks = list(ingestor.chunk_data(data, 3))
        assert len(chunks) == 2
        assert len(chunks[0]) == 3
        assert len(chunks[1]) == 1
        
        # Test chunking with chunk size larger than data
        chunks = list(ingestor.chunk_data(data, 10))
        assert len(chunks) == 1
        assert len(chunks[0]) == 4 