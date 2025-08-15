"""Base test class for all ingestor tests."""

from graph_ingest.common.config_reader import AppConfig, DatabaseConfig, PathsConfig

class BaseIngestorTest:
    """Base class for all ingestor tests."""

    def setup_mock_config(self):
        """Set up a standardized mock configuration.
        
        Returns:
            AppConfig: A mock configuration object
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