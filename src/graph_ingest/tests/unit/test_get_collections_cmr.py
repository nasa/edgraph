import pytest
from unittest.mock import patch, mock_open, MagicMock
import json
import os
from graph_ingest.common.config_reader import AppConfig, DatabaseConfig, PathsConfig
from graph_ingest.tests.unit.base_test import BaseIngestorTest

# Mock os.makedirs at the module level to prevent filesystem operations during import
makedirs_patcher = patch('os.makedirs')
makedirs_mock = makedirs_patcher.start()

# Mock the logger.setup at the module level to prevent filesystem operations during import
logger_patcher = patch('graph_ingest.common.logger_setup.setup_logger')
logger_mock = logger_patcher.start()

# Import the functions directly
from graph_ingest.ingest_scripts.get_collections_cmr import main, MetadataFetcher

def tearDownModule():
    """Clean up the module-level patchers"""
    makedirs_patcher.stop()
    logger_patcher.stop()

class TestMetadataFetcher(BaseIngestorTest):
    """Tests for the MetadataFetcher class."""

    @patch('requests.get')
    @patch('os.makedirs')
    @patch('builtins.open', new_callable=MagicMock)
    @patch('pandas.read_csv')
    @patch('graph_ingest.common.logger_setup.setup_logger')
    def test_process_doi_success(self, mock_logger_setup, mock_read_csv, mock_open, mock_makedirs, mock_get):
        """
        Test that the process_doi method behaves correctly when the metadata fetch is successful.
        """
        # Arrange
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "items": [
                {
                    "meta": {
                        "concept-id": "C1234567-TEST"
                    },
                    "umm": {
                        "EntryTitle": "Test Dataset Monthly",
                        "Abstract": "This is a test dataset",
                        "DataCenters": [
                            {
                                "ShortName": "TEST_CENTER"
                            }
                        ]
                    }
                }
            ]
        }
        mock_get.return_value = mock_response

        # Create fetcher instance with mock config
        fetcher = MetadataFetcher(self.setup_mock_config())

        # Act
        doi = "10.5067/IAGYM8QHCD5"
        result = fetcher.process_doi(doi)

        # Assert
        assert result == (doi, True, True, "monthly", False)
        mock_get.assert_called_once_with('https://cmr.earthdata.nasa.gov/search/collections.umm_json?doi=10.5067%2FIAGYM8QHCD5')
        
        expected_path = os.path.join(self.setup_mock_config().paths.dataset_metadata_directory, "TEST_CENTER", "10.5067_IAGYM8QHCD5.json")
        mock_open.assert_called_once_with(expected_path, "w")

    @patch('requests.get')
    @patch('os.makedirs')
    @patch('builtins.open', new_callable=MagicMock)
    @patch('pandas.read_csv')
    @patch('graph_ingest.common.logger_setup.setup_logger')
    def test_process_doi_api_failure(self, mock_logger_setup, mock_read_csv, mock_open, mock_makedirs, mock_get):
        """
        Test that the process_doi method returns appropriate values when the API request fails.
        """
        # Arrange
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {}
        mock_response.content = b"Not found"
        mock_get.return_value = mock_response

        # Create fetcher instance with mock config
        fetcher = MetadataFetcher(self.setup_mock_config())

        # Act
        doi = "10.5067/IAGYM8QHCD5"
        result = fetcher.process_doi(doi)

        # Assert
        assert result == (doi, False, False, "Unknown", False)
        mock_get.assert_called_once_with('https://cmr.earthdata.nasa.gov/search/collections.umm_json?doi=10.5067%2FIAGYM8QHCD5')
        mock_open.assert_not_called()

    @patch('requests.get')
    @patch('os.makedirs')
    @patch('builtins.open', new_callable=MagicMock)
    @patch('pandas.read_csv')
    @patch('graph_ingest.common.logger_setup.setup_logger')
    def test_process_doi_exception_handling(self, mock_logger_setup, mock_read_csv, mock_open, mock_makedirs, mock_get):
        """
        Test that the process_doi method properly handles exceptions.
        """
        # Arrange
        mock_get.side_effect = Exception("Test error")

        # Create fetcher instance with mock config
        fetcher = MetadataFetcher(self.setup_mock_config())

        # Act
        doi = "10.5067/IAGYM8QHCD5"
        result = fetcher.process_doi(doi)

        # Assert
        assert result == (doi, False, False, "Unknown", False)
        mock_get.assert_called_once_with('https://cmr.earthdata.nasa.gov/search/collections.umm_json?doi=10.5067%2FIAGYM8QHCD5')
        mock_open.assert_not_called()
        mock_makedirs.assert_not_called()

    @patch('graph_ingest.ingest_scripts.get_collections_cmr.MetadataFetcher')
    @patch('graph_ingest.ingest_scripts.get_collections_cmr.load_config')
    @patch('graph_ingest.ingest_scripts.get_collections_cmr.setup_logger')
    def test_main_function(self, mock_setup_logger, mock_load_config, mock_fetcher_class):
        """
        Test the main function with the MetadataFetcher class.
        """
        # Configure mocks
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_fetcher = MagicMock()
        mock_fetcher_class.return_value = mock_fetcher
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger

        # Act
        main()

        # Assert
        mock_load_config.assert_called_once()
        mock_fetcher_class.assert_called_once_with(mock_config)
        mock_fetcher.run.assert_called_once()
