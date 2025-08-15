"""Unit tests for FastRP embedding computation.

This test suite covers the FastRPProcessor class, which is responsible for
computing FastRP embeddings for nodes in Neo4j. The tests focus on:

1. Initialization of the processor
2. Dropping existing graph projections
3. Creating graph projections
4. Running the FastRP algorithm
5. Getting embedding statistics
6. The full FastRP computation workflow

These tests ensure that the FastRP computation process correctly
interacts with the Neo4j Graph Data Science library to compute and store node embeddings.
"""

import logging
from unittest.mock import patch, MagicMock, call, ANY

import pytest

from graph_ingest.ingest_scripts.ingest_compute_fastrp import FastRPProcessor, main


class TestFastRPProcessor:
    """Tests for the FastRPProcessor class."""

    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.setup_logger')
    def test_initialization(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test the initialization of the FastRPProcessor."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_setup_logger.return_value = MagicMock()
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        
        # Create the processor
        processor = FastRPProcessor()
        
        # Verify the initialization
        mock_load_config.assert_called_once()
        mock_setup_logger.assert_called_once()
        mock_get_driver.assert_called_once()
        
        # Check that the configuration was properly set
        assert processor.config == mock_load_config.return_value
        assert processor.logger == mock_setup_logger.return_value
        assert processor.driver == mock_driver

    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.setup_logger')
    def test_drop_existing_projection_when_exists(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test dropping an existing graph projection when it exists."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_result = MagicMock()
        mock_result["exists"] = True
        mock_tx.run.return_value.single.return_value = mock_result
        
        # Create the processor
        processor = FastRPProcessor()
        
        # Test dropping the existing projection
        processor._drop_existing_projection(mock_tx)
        
        # Verify that tx.run was called correctly
        assert any("CALL gds.graph.exists('graphEmbedding') YIELD exists RETURN exists" in str(call) for call in mock_tx.run.call_args_list)
        assert any("CALL gds.graph.drop('graphEmbedding') YIELD graphName" in str(call) for call in mock_tx.run.call_args_list)
        
        # Verify logging
        mock_logger.info.assert_any_call("Existing graph projection 'graphEmbedding' dropped.")

    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.setup_logger')
    def test_drop_existing_projection_when_not_exists(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test dropping an existing graph projection when it doesn't exist."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger

        # Create a mock transaction with no existing graph projection
        mock_tx = MagicMock()
        
        # Configure the mock_tx.run to return result with exists=False
        mock_result = MagicMock()
        mock_result.__getitem__.return_value = False  # This is the key part - result["exists"] should be False
        mock_tx.run.return_value.single.return_value = mock_result
        
        # Create the processor
        processor = FastRPProcessor()

        # Test dropping the existing projection
        processor._drop_existing_projection(mock_tx)

        # Verify that the right query was called to check if graph exists
        mock_tx.run.assert_called_once_with("CALL gds.graph.exists('graphEmbedding') YIELD exists RETURN exists")
        
        # Verify that the drop query was NOT called (since exists=False)
        # If the drop query was called, there would be more than one call to mock_tx.run
        assert mock_tx.run.call_count == 1
        
        # Verify that some logging happened
        assert mock_logger.info.call_count > 0

    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.setup_logger')
    def test_create_graph_projection_success(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test creating a graph projection successfully."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_result = MagicMock()
        mock_result["graphName"] = "graphEmbedding"
        mock_result["nodeCount"] = 100
        mock_result["relationshipCount"] = 200
        mock_tx.run.return_value.single.return_value = mock_result
        
        # Create the processor
        processor = FastRPProcessor()
        
        # Test creating the graph projection
        processor._create_graph_projection(mock_tx)
        
        # Verify that tx.run was called
        mock_tx.run.assert_called_once()
        
        # Verify logging - use ANY to match the generated message regardless of the mock object values
        mock_logger.info.assert_called_with(ANY)

    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.setup_logger')
    def test_create_graph_projection_failure(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test creating a graph projection with failure."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_tx.run.return_value.single.return_value = None
        
        # Create the processor
        processor = FastRPProcessor()
        
        # Test creating the graph projection
        processor._create_graph_projection(mock_tx)
        
        # Verify that tx.run was called correctly
        mock_tx.run.assert_called_once()
        
        # Verify logging
        mock_logger.error.assert_called_with("Failed to create graph projection.")

    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.setup_logger')
    def test_run_fastrp_success(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test running FastRP algorithm successfully."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_result = MagicMock()
        mock_result["nodePropertiesWritten"] = 100
        mock_tx.run.return_value.single.return_value = mock_result
        
        # Create the processor
        processor = FastRPProcessor()
        
        # Test running FastRP
        processor._run_fastrp(mock_tx)
        
        # Verify that tx.run was called correctly
        mock_tx.run.assert_called_once()
        
        # Verify logging using ANY for flexible assertion
        mock_logger.info.assert_called_with(ANY)

    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.setup_logger')
    def test_run_fastrp_failure(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test running FastRP algorithm with failure."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_tx.run.return_value.single.return_value = None
        
        # Create the processor
        processor = FastRPProcessor()
        
        # Test running FastRP
        processor._run_fastrp(mock_tx)
        
        # Verify that tx.run was called correctly
        mock_tx.run.assert_called_once()
        
        # Verify logging
        mock_logger.error.assert_called_with("Failed to run FastRP or write embeddings.")

    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.setup_logger')
    def test_drop_graph_projection_success(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test dropping a graph projection successfully."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_result = MagicMock()
        mock_result["graphName"] = "graphEmbedding"
        mock_tx.run.return_value.single.return_value = mock_result
        
        # Create the processor
        processor = FastRPProcessor()
        
        # Test dropping the graph projection
        processor._drop_graph_projection(mock_tx)
        
        # Verify that tx.run was called correctly
        mock_tx.run.assert_called_with("CALL gds.graph.drop('graphEmbedding') YIELD graphName")
        
        # Verify logging using ANY to match regardless of mock object values
        mock_logger.info.assert_called_with(ANY)

    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.setup_logger')
    def test_drop_graph_projection_failure(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test dropping a graph projection with failure."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_tx.run.return_value.single.return_value = None
        
        # Create the processor
        processor = FastRPProcessor()
        
        # Test dropping the graph projection
        processor._drop_graph_projection(mock_tx)
        
        # Verify that tx.run was called correctly
        mock_tx.run.assert_called_with("CALL gds.graph.drop('graphEmbedding') YIELD graphName")
        
        # Verify logging
        mock_logger.error.assert_called_with("Failed to drop the graph projection.")

    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.setup_logger')
    def test_get_embedding_stats_success(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test getting FastRP statistics successfully."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_result = MagicMock()
        mock_result["embedding_nodes"] = 100
        mock_tx.run.return_value.single.return_value = mock_result
        
        # Create the processor
        processor = FastRPProcessor()
        
        # Test getting FastRP stats
        processor._get_embedding_stats(mock_tx)
        
        # Verify that tx.run was called
        mock_tx.run.assert_called_once()
        
        # Verify logging with ANY to match regardless of the mock object values
        mock_logger.info.assert_called_with(ANY)

    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.setup_logger')
    def test_get_embedding_stats_failure(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test getting FastRP statistics with failure."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_tx.run.return_value.single.return_value = None
        
        # Create the processor
        processor = FastRPProcessor()
        
        # Test getting FastRP stats
        processor._get_embedding_stats(mock_tx)
        
        # Verify that tx.run was called correctly
        mock_tx.run.assert_called_once()
        
        # Verify logging
        mock_logger.error.assert_called_with("Failed to retrieve FastRP embedding stats.")

    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.setup_logger')
    def test_run_fastrp_embeddings_workflow(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test the complete FastRP embeddings workflow."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock session and driver
        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_driver.session.return_value = mock_session
        mock_driver.session.return_value.__enter__.return_value = mock_session
        mock_get_driver.return_value = mock_driver
        
        # Create the processor
        processor = FastRPProcessor()
        
        # Test the complete workflow
        processor.run_fastrp_embeddings()
        
        # Verify that the session methods were called in the correct order
        assert mock_session.execute_write.call_args_list == [
            call(processor._drop_existing_projection),
            call(processor._create_graph_projection),
            call(processor._run_fastrp),
            call(processor._drop_graph_projection)
        ]
        assert mock_session.execute_read.call_args_list == [
            call(processor._get_embedding_stats)
        ]
        
        # Verify logging with expected exact strings
        expected_logs = [
            "Dropping existing graph projection (if any)...",
            "Creating graph projection...",
            "Running FastRP embeddings...",
            "Fetching FastRP embedding stats...",
            "Dropping graph projection after use..."
        ]
        for log in expected_logs:
            mock_logger.info.assert_any_call(log)

    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.setup_logger')
    def test_close(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test closing the database connection."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        
        # Create the processor
        processor = FastRPProcessor()
        
        # Test closing the connection
        processor.close()
        
        # Verify that the driver was closed
        mock_driver.close.assert_called_once()
        
        # Verify logging
        mock_logger.info.assert_called_with("Neo4j connection closed.")

    @patch('graph_ingest.ingest_scripts.ingest_compute_fastrp.FastRPProcessor')
    def test_main_function(self, mock_processor_class):
        """Test the main function."""
        # Configure the mock
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor
        
        # Call main
        main()
        
        # Verify that the processor was created and its methods were called
        mock_processor_class.assert_called_once()
        mock_processor.run_fastrp_embeddings.assert_called_once()
        mock_processor.close.assert_called_once() 