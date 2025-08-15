"""Unit tests for PageRank computation.

This test suite covers the PageRankProcessor class, which is responsible for
computing PageRank scores for Publication and Dataset nodes in Neo4j. The tests focus on:

1. Initialization of the processor
2. Dropping existing graph projections
3. Creating graph projections
4. Running PageRank algorithm
5. Getting PageRank statistics
6. The full PageRank computation workflow

These tests ensure that the PageRank computation process correctly
interacts with the Neo4j Graph Data Science library to compute and store PageRank scores.
"""

import logging
from unittest.mock import patch, MagicMock, call, ANY

import pytest

from graph_ingest.ingest_scripts.ingest_compute_pagerank import PageRankProcessor, main


class TestPageRankProcessor:
    """Tests for the PageRankProcessor class."""

    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.setup_logger')
    def test_initialization(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test the initialization of the PageRankProcessor."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_setup_logger.return_value = MagicMock()
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        
        # Create the processor
        processor = PageRankProcessor()
        
        # Verify the initialization
        mock_load_config.assert_called_once()
        mock_setup_logger.assert_called_once()
        mock_get_driver.assert_called_once()
        
        # Check that the configuration was properly set
        assert processor.config == mock_load_config.return_value
        assert processor.logger == mock_setup_logger.return_value
        assert processor.driver == mock_driver

    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.setup_logger')
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
        processor = PageRankProcessor()
        
        # Test dropping the existing projection
        processor._drop_existing_projection(mock_tx)
        
        # Verify that tx.run was called with the expected queries
        assert any("CALL gds.graph.exists('publicationDatasetGraph') YIELD exists RETURN exists" in str(call) for call in mock_tx.run.call_args_list)
        assert any("CALL gds.graph.drop('publicationDatasetGraph') YIELD graphName" in str(call) for call in mock_tx.run.call_args_list)
        
        # Verify logging
        mock_logger.info.assert_any_call("Existing graph projection 'publicationDatasetGraph' dropped.")

    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.setup_logger')
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
        processor = PageRankProcessor()

        # Test dropping the existing projection
        processor._drop_existing_projection(mock_tx)

        # Verify that the right query was called to check if graph exists
        mock_tx.run.assert_called_once_with("CALL gds.graph.exists('publicationDatasetGraph') YIELD exists RETURN exists")
        
        # Verify that the drop query was NOT called (since exists=False)
        # If the drop query was called, there would be more than one call to mock_tx.run
        assert mock_tx.run.call_count == 1
        
        # Verify that some logging happened
        assert mock_logger.info.call_count > 0

    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.setup_logger')
    def test_create_graph_projection_success(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test creating a graph projection successfully."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_result = MagicMock()
        mock_result["graphName"] = "publicationDatasetGraph"
        mock_result["nodeCount"] = 100
        mock_result["relationshipCount"] = 200
        mock_tx.run.return_value.single.return_value = mock_result
        
        # Create the processor
        processor = PageRankProcessor()
        
        # Test creating the graph projection
        processor._create_graph_projection(mock_tx)
        
        # Verify that tx.run was called
        mock_tx.run.assert_called_once()
        
        # Verify logging with ANY to match regardless of mock object values
        mock_logger.info.assert_called_with(ANY)

    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.setup_logger')
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
        processor = PageRankProcessor()
        
        # Test creating the graph projection
        processor._create_graph_projection(mock_tx)
        
        # Verify that tx.run was called correctly
        mock_tx.run.assert_called_once()
        
        # Verify logging
        mock_logger.error.assert_called_with("Failed to create graph projection.")

    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.setup_logger')
    def test_run_pagerank_success(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test running PageRank algorithm successfully."""
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
        processor = PageRankProcessor()
        
        # Test running PageRank
        processor._run_pagerank(mock_tx)
        
        # Verify that tx.run was called correctly
        mock_tx.run.assert_called_once()
        
        # Verify logging with ANY
        mock_logger.info.assert_called_with(ANY)

    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.setup_logger')
    def test_run_pagerank_failure(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test running PageRank algorithm with failure."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_tx.run.return_value.single.return_value = None
        
        # Create the processor
        processor = PageRankProcessor()
        
        # Test running PageRank
        processor._run_pagerank(mock_tx)
        
        # Verify that tx.run was called correctly
        mock_tx.run.assert_called_once()
        
        # Verify logging
        mock_logger.error.assert_called_with("Failed to run PageRank or write properties.")

    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.setup_logger')
    def test_drop_graph_projection_success(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test dropping a graph projection successfully."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_result = MagicMock()
        mock_result["graphName"] = "publicationDatasetGraph"
        mock_tx.run.return_value.single.return_value = mock_result
        
        # Create the processor
        processor = PageRankProcessor()
        
        # Test dropping the graph projection
        processor._drop_graph_projection(mock_tx)
        
        # Verify that tx.run was called correctly
        mock_tx.run.assert_called_with("CALL gds.graph.drop('publicationDatasetGraph') YIELD graphName")
        
        # Verify logging with ANY
        mock_logger.info.assert_called_with(ANY)

    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.setup_logger')
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
        processor = PageRankProcessor()
        
        # Test dropping the graph projection
        processor._drop_graph_projection(mock_tx)
        
        # Verify that tx.run was called correctly
        mock_tx.run.assert_called_with("CALL gds.graph.drop('publicationDatasetGraph') YIELD graphName")
        
        # Verify logging
        mock_logger.error.assert_called_with("Failed to drop the graph projection.")

    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.setup_logger')
    def test_get_pagerank_stats_success(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test getting PageRank statistics successfully."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_result = MagicMock()
        mock_result["pagerank_nodes"] = 100
        mock_tx.run.return_value.single.return_value = mock_result
        
        # Create the processor
        processor = PageRankProcessor()
        
        # Test getting PageRank stats
        processor._get_pagerank_stats(mock_tx)
        
        # Verify that tx.run was called
        mock_tx.run.assert_called_once()
        
        # Verify logging with ANY
        mock_logger.info.assert_called_with(ANY)

    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.setup_logger')
    def test_get_pagerank_stats_failure(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test getting PageRank statistics with failure."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        # Create a mock transaction
        mock_tx = MagicMock()
        mock_tx.run.return_value.single.return_value = None
        
        # Create the processor
        processor = PageRankProcessor()
        
        # Test getting PageRank stats
        processor._get_pagerank_stats(mock_tx)
        
        # Verify that tx.run was called correctly
        mock_tx.run.assert_called_once()
        
        # Verify logging
        mock_logger.error.assert_called_with("Failed to retrieve PageRank stats.")

    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.setup_logger')
    def test_run_pagerank_workflow(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test the complete PageRank workflow."""
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
        processor = PageRankProcessor()
        
        # Test the complete workflow
        processor.run_pagerank()
        
        # Verify that the session methods were called in the correct order
        assert mock_session.execute_write.call_args_list == [
            call(processor._drop_existing_projection),
            call(processor._create_graph_projection),
            call(processor._run_pagerank),
            call(processor._drop_graph_projection)
        ]
        assert mock_session.execute_read.call_args_list == [
            call(processor._get_pagerank_stats)
        ]
        
        # Verify logging with expected exact strings
        expected_logs = [
            "Dropping existing graph projection (if any)...",
            "Creating graph projection for Publications and Datasets...",
            "Running PageRank on Publications and Datasets...",
            "Fetching PageRank stats...",
            "Dropping graph projection after PageRank computation..."
        ]
        for log in expected_logs:
            mock_logger.info.assert_any_call(log)

    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.setup_logger')
    def test_close(self, mock_setup_logger, mock_load_config, mock_get_driver):
        """Test closing the database connection."""
        # Configure the mocks
        mock_load_config.return_value = {"test_config": "value"}
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        
        # Create the processor
        processor = PageRankProcessor()
        
        # Test closing the connection
        processor.close()
        
        # Verify that the driver was closed
        mock_driver.close.assert_called_once()
        
        # Verify logging
        mock_logger.info.assert_called_with("Neo4j connection closed.")

    @patch('graph_ingest.ingest_scripts.ingest_compute_pagerank.PageRankProcessor')
    def test_main_function(self, mock_processor_class):
        """Test the main function."""
        # Configure the mock
        mock_processor = MagicMock()
        mock_processor_class.return_value = mock_processor
        
        # Call main
        main()
        
        # Verify that the processor was created and its methods were called
        mock_processor_class.assert_called_once()
        mock_processor.run_pagerank.assert_called_once()
        mock_processor.close.assert_called_once() 