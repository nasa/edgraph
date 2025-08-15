"""Integration tests for publication ingestion."""

import os
import pytest
from unittest.mock import patch, MagicMock
from neo4j import Session, exceptions
from neo4j.exceptions import ClientError
from graph_ingest.ingest_scripts.ingest_node_publication import PublicationIngestor
from graph_ingest.ingest_scripts.ingest_edge_publication_dataset import PublicationDatasetRelationshipIngestor
from graph_ingest.tests.fixtures.generated_test_data import (
    MOCK_PUBLICATION,
    MOCK_DATASET,
    MOCK_EDGE_USES_DATASET
)
from graph_ingest.common.config_reader import AppConfig, DatabaseConfig, PathsConfig

# Reuse the fixture from test_dataset_ingestion.py
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

@patch('graph_ingest.ingest_scripts.ingest_node_publication.load_config')
@patch('graph_ingest.ingest_scripts.ingest_node_publication.setup_logger')
def test_publication_ingestion(mock_setup_logger, mock_load_config, neo4j_driver, mock_config):
    """Test ingesting a publication node."""
    # Mock config and logger
    mock_load_config.return_value = mock_config
    mock_logger = MagicMock()
    mock_setup_logger.return_value = mock_logger
    
    # Create publication ingestor
    ingestor = PublicationIngestor(neo4j_driver)
    
    # Set up uniqueness constraint
    ingestor.set_publication_uniqueness_constraint()
    
    # Create publication node using a transaction
    publication_data = MOCK_PUBLICATION['properties']
    with neo4j_driver.session() as session:
        session.execute_write(lambda tx: ingestor.create_publication_node(tx, publication_data))
    
    # Verify the publication was created
    with neo4j_driver.session() as session:
        result = session.run(
            "MATCH (p:Publication {globalId: $globalId}) RETURN p",
            globalId=publication_data['globalId']
        )
        node = result.single()
        assert node is not None
        assert node['p']['doi'] == publication_data['doi']
        assert node['p']['title'] == publication_data['title']
        assert node['p']['year'] == publication_data['year']

@patch('graph_ingest.ingest_scripts.ingest_node_publication.load_config')
@patch('graph_ingest.ingest_scripts.ingest_node_publication.setup_logger')
@patch('graph_ingest.ingest_scripts.ingest_edge_publication_dataset.load_config')
@patch('graph_ingest.ingest_scripts.ingest_edge_publication_dataset.setup_logger')
def test_publication_dataset_relationship(mock_rel_setup_logger, mock_rel_load_config, mock_pub_setup_logger, mock_pub_load_config, neo4j_driver, mock_config):
    """Test creating a relationship between a publication and a dataset."""
    # Mock config and logger
    mock_pub_load_config.return_value = mock_config
    mock_rel_load_config.return_value = mock_config
    mock_pub_logger = MagicMock()
    mock_rel_logger = MagicMock()
    mock_pub_setup_logger.return_value = mock_pub_logger
    mock_rel_setup_logger.return_value = mock_rel_logger
    
    # Create publication and dataset nodes first
    pub_ingestor = PublicationIngestor(neo4j_driver)
    pub_ingestor.set_publication_uniqueness_constraint()
    
    with neo4j_driver.session() as session:
        session.execute_write(lambda tx: pub_ingestor.create_publication_node(tx, MOCK_PUBLICATION['properties']))
    
    # Create dataset node (assuming dataset constraint exists from dataset tests)
    with neo4j_driver.session() as session:
        session.run("""
            CREATE (d:Dataset {
                globalId: $globalId,
                doi: $doi,
                shortName: $shortName,
                longName: $longName
            })
        """, MOCK_DATASET['properties'])
    
    # Create relationship
    rel_ingestor = PublicationDatasetRelationshipIngestor()
    
    # Create relationship data
    publication_globalId = pub_ingestor.generate_uuid_from_doi(MOCK_PUBLICATION['properties']['doi'])
    dataset_globalId = pub_ingestor.generate_uuid_from_doi(MOCK_DATASET['properties']['doi'])
    
    with neo4j_driver.session() as session:
        session.execute_write(lambda tx: rel_ingestor.create_relationship(tx, publication_globalId, dataset_globalId))
    
    # Verify the relationship was created
    with neo4j_driver.session() as session:
        result = session.run("""
            MATCH (p:Publication {globalId: $pub_id})-[r:USES_DATASET]->(d:Dataset {globalId: $dataset_id})
            RETURN r
        """, pub_id=publication_globalId, dataset_id=dataset_globalId)
        assert result.single() is not None

@patch('graph_ingest.ingest_scripts.ingest_node_publication.load_config')
@patch('graph_ingest.ingest_scripts.ingest_node_publication.setup_logger')
def test_publication_duplicate_ingestion(mock_setup_logger, mock_load_config, neo4j_driver, mock_config):
    """Test that duplicate publications are handled correctly."""
    # Mock config and logger
    mock_load_config.return_value = mock_config
    mock_logger = MagicMock()
    mock_setup_logger.return_value = mock_logger
    
    ingestor = PublicationIngestor(neo4j_driver)
    ingestor.set_publication_uniqueness_constraint()
    
    # Insert first time
    with neo4j_driver.session() as session:
        session.execute_write(lambda tx: ingestor.create_publication_node(tx, MOCK_PUBLICATION['properties']))
    
    # Insert duplicate
    with neo4j_driver.session() as session:
        session.execute_write(lambda tx: ingestor.create_publication_node(tx, MOCK_PUBLICATION['properties']))
    
    # Verify only one node exists
    with neo4j_driver.session() as session:
        result = session.run(
            "MATCH (p:Publication {globalId: $globalId}) RETURN count(p) as count",
            globalId=MOCK_PUBLICATION['properties']['globalId']
        )
        assert result.single()['count'] == 1

@patch('graph_ingest.ingest_scripts.ingest_node_publication.load_config')
@patch('graph_ingest.ingest_scripts.ingest_node_publication.setup_logger')
def test_publication_missing_required_fields(mock_setup_logger, mock_load_config, neo4j_driver, mock_config):
    """Test that creating a publication node with missing required fields raises an error."""
    # Mock config and logger
    mock_load_config.return_value = mock_config
    mock_logger = MagicMock()
    mock_setup_logger.return_value = mock_logger
    
    pub_ingestor = PublicationIngestor(neo4j_driver)
    pub_ingestor.set_publication_uniqueness_constraint()
    
    # Create publication data with missing required field (doi)
    publication_data = {
        'title': 'Test Publication',
        'authors': ['Author 1', 'Author 2']
    }
    
    with neo4j_driver.session() as session:
        with pytest.raises(ClientError) as excinfo:
            session.execute_write(lambda tx: pub_ingestor.create_publication_node(tx, publication_data))
        assert "Expected parameter(s): globalId, doi, year, abstract" in str(excinfo.value)