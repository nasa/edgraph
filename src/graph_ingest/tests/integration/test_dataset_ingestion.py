"""Integration tests for dataset ingestion."""

import os
import pytest
from unittest.mock import patch, MagicMock
from graph_ingest.ingest_scripts.ingest_node_dataset import DatasetIngestor
from graph_ingest.common.config_reader import AppConfig, DatabaseConfig, PathsConfig

# Create a temporary directory for logs
@pytest.fixture(scope="module", autouse=True)
def setup_test_environment(tmpdir_factory):
    """Create temp directories for testing."""
    log_dir = tmpdir_factory.mktemp("logs")
    os.environ["LOG_DIRECTORY"] = str(log_dir)
    yield
    # Clean up
    if "LOG_DIRECTORY" in os.environ:
        del os.environ["LOG_DIRECTORY"]

@pytest.fixture
def mock_config():
    """Return a mock configuration for testing."""
    return AppConfig(
        database=DatabaseConfig(
            uri="bolt://localhost:7687",
            user="neo4j",
            password="password"
        ),
        paths=PathsConfig(
            source_dois_directory="/tmp/source_dois",
            dataset_metadata_directory="/tmp/dataset_metadata",
            gcmd_sciencekeyword_directory="/tmp/sciencekeywords",
            publications_metadata_directory="/tmp/publications",
            pubs_of_pubs="/tmp/pubs_of_pubs",
            log_directory=os.environ.get("LOG_DIRECTORY", "/tmp/logs")
        )
    )

@patch('graph_ingest.ingest_scripts.ingest_node_dataset.setup_logger')
@patch('graph_ingest.ingest_scripts.ingest_node_dataset.load_config')
def test_dataset_ingestion(mock_load_config, mock_setup_logger, neo4j_driver, sample_dataset, mock_config):
    """Test ingesting a dataset into Neo4j."""
    # Setup mocks
    mock_load_config.return_value = mock_config
    mock_logger = MagicMock()
    mock_setup_logger.return_value = mock_logger
    
    # Initialize the ingestor
    ingestor = DatasetIngestor()
    
    # Set the driver (override the default for testing)
    ingestor.driver = neo4j_driver
    
    # Create dataset node
    with neo4j_driver.session() as session:
        # Set uniqueness constraint
        ingestor.set_uniqueness_constraint()
        
        # Add dataset
        session.execute_write(ingestor.add_datasets, [sample_dataset])
        
        # Verify dataset was created
        result = session.run(
            """
            MATCH (d:Dataset {shortName: $shortName})
            RETURN d
            """,
            shortName=sample_dataset["shortName"]
        )
        
        dataset_node = result.single()
        assert dataset_node is not None
        
        # Verify properties
        dataset = dataset_node["d"]
        assert dataset["shortName"] == sample_dataset["shortName"]
        assert dataset["longName"] == sample_dataset["longName"]
        assert dataset["abstract"] == sample_dataset["abstract"]
        assert dataset["doi"] == sample_dataset["doi"]
        assert dataset["temporalExtentStart"] == sample_dataset["temporalExtentStart"]
        assert dataset["temporalExtentEnd"] == sample_dataset["temporalExtentEnd"]
        assert dataset["temporalFrequency"] == sample_dataset["temporalFrequency"]

@patch('graph_ingest.ingest_scripts.ingest_node_dataset.setup_logger')
@patch('graph_ingest.ingest_scripts.ingest_node_dataset.load_config')
def test_dataset_duplicate_ingestion(mock_load_config, mock_setup_logger, neo4j_driver, sample_dataset, mock_config):
    """Test ingesting the same dataset twice."""
    # Setup mocks
    mock_load_config.return_value = mock_config
    mock_logger = MagicMock()
    mock_setup_logger.return_value = mock_logger
    
    ingestor = DatasetIngestor()
    ingestor.driver = neo4j_driver
    
    # Set uniqueness constraint
    ingestor.set_uniqueness_constraint()
    
    # Add dataset twice
    with neo4j_driver.session() as session:
        session.execute_write(ingestor.add_datasets, [sample_dataset])
        session.execute_write(ingestor.add_datasets, [sample_dataset])
    
        # Verify only one node was created
        result = session.run(
            """
            MATCH (d:Dataset {shortName: $shortName})
            RETURN count(d) as count
            """,
            shortName=sample_dataset["shortName"]
        )
        
        assert result.single()["count"] == 1

@patch('graph_ingest.ingest_scripts.ingest_node_dataset.setup_logger')
@patch('graph_ingest.ingest_scripts.ingest_node_dataset.load_config')
def test_dataset_missing_required_fields(mock_load_config, mock_setup_logger, neo4j_driver, mock_config):
    """Test ingesting a dataset with missing required fields."""
    # Setup mocks
    mock_load_config.return_value = mock_config
    mock_logger = MagicMock()
    mock_setup_logger.return_value = mock_logger
    
    ingestor = DatasetIngestor()
    ingestor.driver = neo4j_driver
    
    # Dataset missing required fields
    invalid_dataset = {
        "shortName": "TEST_INVALID",
        "temporalExtentStart": "2020-01-01",
        "temporalExtentEnd": "2020-12-31",
        # Missing required fields: globalId, doi, longName, daac, abstract, cmrId, temporalFrequency
    }
    
    # Attempt to add invalid dataset
    with pytest.raises(Exception) as exc_info:
        with neo4j_driver.session() as session:
            session.execute_write(ingestor.add_datasets, [invalid_dataset])
    
    assert "ParameterMissing" in str(exc_info.value)
    
    # Verify no node was created
    with neo4j_driver.session() as session:
        result = session.run(
            """
            MATCH (d:Dataset {shortName: $shortName})
            RETURN count(d) as count
            """,
            shortName=invalid_dataset["shortName"]
        )
        
        assert result.single()["count"] == 0

@patch('graph_ingest.ingest_scripts.ingest_node_dataset.setup_logger')
@patch('graph_ingest.ingest_scripts.ingest_node_dataset.load_config')
def test_dataset_uniqueness_constraint(mock_load_config, mock_setup_logger, neo4j_driver, sample_dataset, mock_config):
    """Test that the uniqueness constraint prevents duplicate datasets."""
    # Setup mocks
    mock_load_config.return_value = mock_config
    mock_logger = MagicMock()
    mock_setup_logger.return_value = mock_logger
    
    ingestor = DatasetIngestor()
    ingestor.driver = neo4j_driver
    
    with neo4j_driver.session() as session:
        # Set up uniqueness constraint
        ingestor.set_uniqueness_constraint()
        
        # Add the dataset first time
        session.execute_write(ingestor.add_datasets, [sample_dataset])
        
        # Add the same dataset again
        session.execute_write(ingestor.add_datasets, [sample_dataset])
        
        # Verify only one node exists with these properties
        result = session.run(
            """
            MATCH (d:Dataset {globalId: $globalId})
            RETURN count(d) as count
            """,
            globalId=sample_dataset["globalId"]
        )
        
        # Should only be one node despite trying to add it twice
        assert result.single()["count"] == 1
        
        # Verify all properties are preserved
        result = session.run(
            """
            MATCH (d:Dataset {globalId: $globalId})
            RETURN d
            """,
            globalId=sample_dataset["globalId"]
        )
        
        dataset = result.single()["d"]
        assert dataset["shortName"] == sample_dataset["shortName"]
        assert dataset["longName"] == sample_dataset["longName"]
        assert dataset["abstract"] == sample_dataset["abstract"]
        assert dataset["doi"] == sample_dataset["doi"]

@patch('graph_ingest.ingest_scripts.ingest_node_dataset.setup_logger')
@patch('graph_ingest.ingest_scripts.ingest_node_dataset.load_config')
def test_dataset_temporal_extent(mock_load_config, mock_setup_logger, neo4j_driver, mock_config):
    """Test ingesting datasets with different temporal extent formats."""
    # Setup mocks
    mock_load_config.return_value = mock_config
    mock_logger = MagicMock()
    mock_setup_logger.return_value = mock_logger
    
    ingestor = DatasetIngestor()
    ingestor.driver = neo4j_driver
    
    datasets = [
        {
            "globalId": "test-dataset-1",
            "doi": "10.1234/test1",
            "shortName": "TEST_DATASET_1",
            "longName": "Test Dataset 1",
            "daac": "TEST_DAAC",
            "abstract": "Test dataset 1 abstract",
            "cmrId": "TEST_CMR_1",
            "temporalFrequency": "Daily",
            "temporalExtentStart": "2020-01-01T00:00:00Z",
            "temporalExtentEnd": "2020-12-31T23:59:59Z"
        },
        {
            "globalId": "test-dataset-2",
            "doi": "10.1234/test2",
            "shortName": "TEST_DATASET_2",
            "longName": "Test Dataset 2",
            "daac": "TEST_DAAC",
            "abstract": "Test dataset 2 abstract",
            "cmrId": "TEST_CMR_2",
            "temporalFrequency": "Monthly",
            "temporalExtentStart": None,
            "temporalExtentEnd": None
        },
        {
            "globalId": "test-dataset-3",
            "doi": "10.1234/test3",
            "shortName": "TEST_DATASET_3",
            "longName": "Test Dataset 3",
            "daac": "TEST_DAAC",
            "abstract": "Test dataset 3 abstract",
            "cmrId": "TEST_CMR_3",
            "temporalFrequency": "Yearly",
            "temporalExtentStart": "2020-01-01",  # Different format
            "temporalExtentEnd": "2020-12-31"
        }
    ]
    
    with neo4j_driver.session() as session:
        # Set up uniqueness constraint
        ingestor.set_uniqueness_constraint()
        
        # Add datasets
        session.execute_write(ingestor.add_datasets, datasets)
        
        # Verify all datasets were added
        result = session.run("MATCH (d:Dataset) RETURN count(d) as count")
        assert result.single()["count"] == len(datasets)
        
        # Verify temporal extent handling
        for dataset in datasets:
            result = session.run(
                """
                MATCH (d:Dataset {shortName: $shortName})
                RETURN d.temporalExtentStart, d.temporalExtentEnd
                """,
                shortName=dataset["shortName"]
            )
            temporal = result.single()
            assert temporal is not None
            
            # Verify temporal extent values were stored as provided
            assert temporal["d.temporalExtentStart"] == dataset["temporalExtentStart"]
            assert temporal["d.temporalExtentEnd"] == dataset["temporalExtentEnd"] 