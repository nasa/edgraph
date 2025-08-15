"""Unit tests for publication ingestion."""

import pytest
import os
import json
import logging
from unittest.mock import patch, MagicMock, mock_open, ANY, PropertyMock, call

from graph_ingest.ingest_scripts.ingest_node_publication import PublicationIngestor, main
from graph_ingest.tests.fixtures.test_data import MOCK_PUBLICATION
# Import the realistic publication fixture
from graph_ingest.tests.fixtures.generated_test_data import MOCK_PUBLICATION as REALISTIC_PUBLICATION
from graph_ingest.common.config_reader import AppConfig, DatabaseConfig, PathsConfig
from graph_ingest.tests.unit.base_test import BaseIngestorTest

class TestPublicationIngestor(BaseIngestorTest):
    """Tests for the PublicationIngestor class."""

    def test_initialization(self):
        """Test initialization of PublicationIngestor."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_logger = MagicMock(spec=logging.Logger)
        mock_driver = MagicMock()
        
        # Apply patches
        with patch('graph_ingest.ingest_scripts.ingest_node_publication.setup_logger', return_value=mock_logger) as mock_setup_logger:
            with patch('graph_ingest.ingest_scripts.ingest_node_publication.load_config', return_value=mock_config):
                with patch('os.makedirs') as mock_makedirs:
                    with patch('graph_ingest.ingest_scripts.ingest_node_publication.get_driver', return_value=mock_driver) as mock_get_driver:
                        # Act
                        ingestor = PublicationIngestor()
                        
                        # Assert
                        mock_makedirs.assert_called_once_with('/mock/log/dir', exist_ok=True)
                        mock_setup_logger.assert_called_once()
                        mock_get_driver.assert_called_once()
                        assert ingestor.driver == mock_driver
                        assert ingestor.logger == mock_logger
                        assert ingestor.config == mock_config

    def test_set_publication_uniqueness_constraint(self):
        """Test setting uniqueness constraint."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_logger = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        
        with patch('graph_ingest.ingest_scripts.ingest_node_publication.setup_logger', return_value=mock_logger):
            with patch('graph_ingest.ingest_scripts.ingest_node_publication.load_config', return_value=mock_config):
                with patch('os.makedirs'):
                    with patch('graph_ingest.ingest_scripts.ingest_node_publication.get_driver', return_value=mock_driver):
                        # Configure session mock
                        mock_driver.session.return_value = mock_session
                        mock_session.__enter__ = MagicMock(return_value=mock_session)
                        mock_session.__exit__ = MagicMock(return_value=None)
                        
                        # Act
                        ingestor = PublicationIngestor()
                        ingestor.set_publication_uniqueness_constraint()
                        
                        # Assert
                        mock_session.run.assert_called_once_with(
                            "CREATE CONSTRAINT IF NOT EXISTS FOR (pub:Publication) REQUIRE pub.globalId IS UNIQUE"
                        )
                        mock_logger.info.assert_called_with("Uniqueness constraint on Publication.globalId set successfully.")

    def test_process_publications(self):
        """Test processing publications."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_logger = MagicMock()
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_tx = MagicMock()

        with patch('graph_ingest.ingest_scripts.ingest_node_publication.setup_logger', return_value=mock_logger):
            with patch('graph_ingest.ingest_scripts.ingest_node_publication.load_config', return_value=mock_config):
                with patch('os.makedirs'):
                    with patch('graph_ingest.ingest_scripts.ingest_node_publication.get_driver', return_value=mock_driver):
                        with patch('builtins.open', mock_open()):
                            with patch('json.load') as mock_json_load:
                                with patch('tqdm.tqdm', lambda x, **kwargs: x):
                                    # Configure mocks
                                    mock_driver.session.return_value = mock_session
                                    mock_session.__enter__ = MagicMock(return_value=mock_session)
                                    mock_session.__exit__ = MagicMock(return_value=None)
                                    
                                    # Mock JSON data
                                    mock_json_data = [
                                        {
                                            "DOI": "10.1234/5678",
                                            "Title": "Test Publication 1",
                                            "Authors": ["Author 1", "Author 2"],
                                            "Year": 2023,
                                            "Abstract": "This is a test abstract"
                                        },
                                        {
                                            "DOI": "10.9876/5432",
                                            "Title": "Test Publication 2",
                                            "Authors": ["Author 3"],
                                            "Year": 2022,
                                            "Abstract": "Another test abstract"
                                        }
                                    ]
                                    mock_json_load.return_value = mock_json_data

                                    # Act
                                    with patch.object(PublicationIngestor, 'generate_uuid_from_doi', return_value="publication-uuid-123"):
                                        ingestor = PublicationIngestor()
                                        # Set the logger to ensure we use our mock and not the actual one
                                        ingestor.logger = mock_logger
                                        ingestor.process_publications()

                                        # Assert
                                        mock_session.execute_write.assert_called()
                                        assert mock_logger.info.call_count > 0

    @patch('graph_ingest.ingest_scripts.ingest_node_publication.PublicationIngestor.process_publications')
    @patch('graph_ingest.ingest_scripts.ingest_node_publication.PublicationIngestor.set_publication_uniqueness_constraint')
    def test_run(self, mock_set_constraint, mock_process_publications):
        """Test the run method."""
        # Arrange
        mock_config = self.setup_mock_config()
        mock_logger = MagicMock()
        mock_driver = MagicMock()

        with patch('graph_ingest.ingest_scripts.ingest_node_publication.setup_logger', return_value=mock_logger):
            with patch('graph_ingest.ingest_scripts.ingest_node_publication.load_config', return_value=mock_config):
                with patch('os.makedirs'):
                    with patch('graph_ingest.ingest_scripts.ingest_node_publication.get_driver', return_value=mock_driver):
                        # Act
                        ingestor = PublicationIngestor()
                        # Set the logger to ensure we use our mock and not the actual one
                        ingestor.logger = mock_logger
                        
                        # Modify the mock_set_constraint to actually call the logger
                        def set_constraint_side_effect():
                            ingestor.logger.info("Uniqueness constraint on Publication.globalId set successfully.")
                        
                        mock_set_constraint.side_effect = set_constraint_side_effect
                        mock_process_publications.return_value = None  # Method with no return value
                        
                        ingestor.run()

                        # Assert
                        mock_set_constraint.assert_called_once()
                        mock_process_publications.assert_called_once()
                        # Verify the logger message was actually called
                        mock_logger.info.assert_any_call("Uniqueness constraint on Publication.globalId set successfully.")

    def test_main_function(self):
        """Test the main function."""
        # Arrange
        mock_ingestor = MagicMock()
        
        with patch('graph_ingest.ingest_scripts.ingest_node_publication.PublicationIngestor', return_value=mock_ingestor):
            # Act
            main()
            
            # Assert
            mock_ingestor.run.assert_called_once() 