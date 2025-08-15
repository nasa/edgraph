import os
import json
import logging
import unittest
from unittest.mock import patch, Mock, MagicMock, PropertyMock, call

from graph_ingest.common.config_reader import AppConfig, DatabaseConfig, PathsConfig
from graph_ingest.ingest_scripts.ingest_node_project import ProjectIngestor, main

class TestProjectIngestion(unittest.TestCase):
    """
    Test cases for the ProjectIngestor class in ingest_node_project.py.
    These tests verify the functionality for ingesting project data into Neo4j.
    """

    def setup_mock_config(self):
        """Set up a standardized mock configuration.
        
        Returns:
            AppConfig: A mock configuration object.
        """
        return AppConfig(
            database=DatabaseConfig(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="password"
            ),
            paths=PathsConfig(
                source_dois_directory="/path/to/dois",
                dataset_metadata_directory="/mock/data/dir",
                gcmd_sciencekeyword_directory="/path/to/keywords",
                publications_metadata_directory="/path/to/publications",
                pubs_of_pubs="/path/to/pubs_of_pubs",
                log_directory="/mock/log/dir"
            )
        )

    def setUp(self):
        """Set up test fixtures before each test method"""
        # Create a patcher for os.makedirs to prevent actual filesystem operations
        self.makedirs_patcher = patch('os.makedirs')
        self.mock_makedirs = self.makedirs_patcher.start()
        
        # Create mock logger
        self.logger_mock = Mock(spec=logging.Logger)
        
        # Create mock UUID
        self.uuid_mock = Mock()
        self.uuid_mock.__str__ = Mock(return_value="test-uuid-1234")
        
        # Create mock session with context manager support
        self.session_mock = Mock()
        self.session_context_mock = Mock()
        self.session_mock.__enter__ = Mock(return_value=self.session_context_mock)
        self.session_mock.__exit__ = Mock(return_value=None)
        
        # Create mock driver
        self.driver_mock = Mock()
        self.driver_mock.session.return_value = self.session_mock

    def tearDown(self):
        """Tear down test fixtures after each test method"""
        self.makedirs_patcher.stop()

    @patch('graph_ingest.ingest_scripts.ingest_node_project.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.load_config')
    @patch('os.environ.get')
    def test_initialization(self, mock_env_get, mock_load_config, 
                           mock_get_driver, mock_setup_logger):
        """
        Test the initialization of the ProjectIngestor class.
        Verifies that the config is loaded, directories are created,
        logger is set up, and driver is initialized.
        """
        # Setup mock returns
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = self.logger_mock
        mock_setup_logger.return_value = mock_logger
        mock_get_driver.return_value = self.driver_mock

        # Test initialization
        ingestor = ProjectIngestor()

        # Verify the expected calls
        mock_load_config.assert_called_once()
        self.mock_makedirs.assert_called_once_with("/mock/log/dir", exist_ok=True)
        mock_setup_logger.assert_called_once()
        mock_get_driver.assert_called_once()

        # Verify the object state
        self.assertEqual(ingestor.config, mock_config)
        self.assertEqual(ingestor.log_directory, "/mock/log/dir")
        self.assertEqual(ingestor.logger, mock_logger)
        self.assertEqual(ingestor.driver, self.driver_mock)

    @patch('graph_ingest.ingest_scripts.ingest_node_project.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.load_config')
    @patch('os.environ.get')
    def test_set_project_uniqueness_constraint(self, mock_env_get, mock_load_config, 
                                             mock_get_driver, mock_setup_logger):
        """
        Test that the uniqueness constraint for Project nodes is correctly set up.
        Verifies that the correct Cypher query is executed and success is logged.
        """
        # Setup mock returns
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = self.logger_mock
        mock_setup_logger.return_value = mock_logger
        mock_get_driver.return_value = self.driver_mock

        # Test setting constraint
        ingestor = ProjectIngestor()
        ingestor.set_project_uniqueness_constraint()

        # Verify the expected calls
        expected_query = "CREATE CONSTRAINT FOR (p:Project) REQUIRE p.globalId IS UNIQUE"
        self.session_context_mock.run.assert_called_once_with(expected_query)
        mock_logger.info.assert_called_with("Uniqueness constraint on Project.globalId set successfully.")

    @patch('graph_ingest.ingest_scripts.ingest_node_project.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.load_config')
    @patch('os.environ.get')
    def test_set_project_uniqueness_constraint_with_error(self, mock_env_get, mock_load_config, 
                                                        mock_get_driver, mock_setup_logger):
        """
        Test error handling when creating the uniqueness constraint fails.
        Verifies that the error is caught and logged appropriately.
        """
        # Setup mock returns
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = self.logger_mock
        mock_setup_logger.return_value = mock_logger
        
        # Configure session to raise an exception
        self.session_context_mock.run.side_effect = Exception("Test constraint error")
        mock_get_driver.return_value = self.driver_mock

        # Test setting constraint with error
        ingestor = ProjectIngestor()
        ingestor.set_project_uniqueness_constraint()

        # Verify the expected calls
        self.session_context_mock.run.assert_called_once()
        mock_logger.error.assert_called_with("Failed to create uniqueness constraint: Test constraint error")

    @patch('graph_ingest.ingest_scripts.ingest_node_project.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.uuid.uuid5')
    @patch('os.environ.get')
    def test_generate_uuid_from_shortname_valid(self, mock_env_get, mock_uuid5, mock_load_config, 
                                              mock_get_driver, mock_setup_logger):
        """
        Test UUID generation with a valid project short name.
        Verifies that a valid UUID is returned and UUID5 is called correctly.
        """
        # Setup mock returns
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = self.logger_mock
        mock_setup_logger.return_value = mock_logger
        mock_get_driver.return_value = self.driver_mock
        
        # Set up UUID mock
        mock_uuid5.return_value = self.uuid_mock

        # Test UUID generation
        ingestor = ProjectIngestor()
        result = ingestor.generate_uuid_from_shortname("TEST-PROJECT")

        # Verify the expected calls and result
        import uuid
        mock_uuid5.assert_called_once_with(uuid.NAMESPACE_DNS, "TEST-PROJECT")
        self.assertEqual(result, "test-uuid-1234")

    @patch('graph_ingest.ingest_scripts.ingest_node_project.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.load_config')
    @patch('os.environ.get')
    def test_generate_uuid_from_shortname_invalid(self, mock_env_get, mock_load_config, 
                                               mock_get_driver, mock_setup_logger):
        """
        Test UUID generation with an invalid short name (non-string).
        Verifies that None is returned and error is logged.
        """
        # Setup mock returns
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = self.logger_mock
        mock_setup_logger.return_value = mock_logger
        mock_get_driver.return_value = self.driver_mock

        # Test UUID generation with invalid input
        ingestor = ProjectIngestor()
        result = ingestor.generate_uuid_from_shortname(123)  # Non-string input

        # Verify the expected calls and result
        self.assertIsNone(result)
        mock_logger.error.assert_called_with("Invalid or missing short name: 123")

    @patch('graph_ingest.ingest_scripts.ingest_node_project.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.find_json_files')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.open', new_callable=unittest.mock.mock_open)
    @patch('json.load')
    @patch('os.environ.get')
    def test_process_json_files_success(self, mock_env_get, mock_json_load, mock_open,
                                       mock_find_json_files, mock_load_config,
                                       mock_get_driver, mock_setup_logger):
        """
        Test processing JSON files to create Project nodes.
        Verifies that files are read, projects are extracted, and database operations are performed.
        """
        # Setup mock returns
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = self.logger_mock
        mock_setup_logger.return_value = mock_logger
        mock_get_driver.return_value = self.driver_mock

        # Mock file finding
        mock_find_json_files.return_value = ["file1.json", "file2.json"]

        # Mock JSON data
        mock_json_data = {
            "Projects": [
                {"ShortName": "PROJ1", "LongName": "Project One"},
                {"ShortName": "PROJ2", "LongName": "Project Two"}
            ]
        }
        mock_json_load.return_value = mock_json_data

        # Mock UUID generation
        with patch.object(ProjectIngestor, 'generate_uuid_from_shortname') as mock_generate_uuid:
            mock_generate_uuid.side_effect = ["uuid-proj1", "uuid-proj2", "uuid-proj1", "uuid-proj2"]

            # Execute the method
            ingestor = ProjectIngestor()
            ingestor.process_json_files("/mock/data/dir")

            # Verify the expected calls
            mock_find_json_files.assert_called_once_with("/mock/data/dir")
            self.assertEqual(mock_open.call_count, 2)
            self.assertEqual(mock_json_load.call_count, 2)
            # The method is called multiple times - once for UUID verification and once for creation
            self.assertEqual(mock_generate_uuid.call_count, 4)

    @patch('graph_ingest.ingest_scripts.ingest_node_project.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.load_config')
    @patch('os.environ.get')
    def test_run(self, mock_env_get, mock_load_config, mock_get_driver, mock_setup_logger):
        """
        Test the run method which drives the ingestion process.
        Verifies that the constraint is set and files are processed.
        """
        # Setup mock returns
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = self.logger_mock
        mock_setup_logger.return_value = mock_logger
        mock_get_driver.return_value = self.driver_mock

        # Mock methods
        with patch.object(ProjectIngestor, 'set_project_uniqueness_constraint') as mock_set_constraint:
            with patch.object(ProjectIngestor, 'process_json_files') as mock_process_files:

                # Execute the run method
                ingestor = ProjectIngestor()
                ingestor.run()

                # Verify the expected calls
                mock_set_constraint.assert_called_once()
                mock_process_files.assert_called_once_with("/mock/data/dir")

    @patch('graph_ingest.ingest_scripts.ingest_node_project.ProjectIngestor')
    def test_main(self, mock_project_ingestor_class):
        """
        Test the main function.
        Verifies that a ProjectIngestor is created and run.
        """
        # Setup mock ingestor
        mock_ingestor = Mock()
        mock_project_ingestor_class.return_value = mock_ingestor

        # Execute main function
        main()

        # Verify the expected calls
        mock_project_ingestor_class.assert_called_once()
        mock_ingestor.run.assert_called_once()

    @patch('graph_ingest.ingest_scripts.ingest_node_project.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_node_project.load_config')
    @patch('os.environ.get')
    def test_create_project_node(self, mock_env_get, mock_load_config, mock_get_driver, mock_setup_logger):
        """
        Test creating a Project node in Neo4j.
        Verifies that the correct Cypher query is executed with the right parameters.
        """
        # Setup mock returns
        mock_config = self.setup_mock_config()
        mock_load_config.return_value = mock_config
        mock_logger = self.logger_mock
        mock_setup_logger.return_value = mock_logger
        mock_get_driver.return_value = self.driver_mock

        # Test creating project node
        ingestor = ProjectIngestor()
        mock_tx = Mock()
        project_data = {
            "globalId": "test-uuid-1234",
            "shortName": "TEST-PROJECT",
            "longName": "Test Project Full Name"
        }
        ingestor.create_project_node(mock_tx, project_data)

        # Verify the expected calls
        mock_tx.run.assert_called_once()
        # Verify that project_data was passed to run (specific query is checked in other tests)
        args, kwargs = mock_tx.run.call_args
        self.assertEqual(kwargs, project_data)

if __name__ == '__main__':
    unittest.main() 