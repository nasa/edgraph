"""Integration tests for platform and instrument ingestion."""

import os
import pytest
from unittest.mock import patch, MagicMock
from neo4j import Session
from graph_ingest.ingest_scripts.ingest_node_platform import PlatformIngestor
from graph_ingest.ingest_scripts.ingest_node_instrument import InstrumentIngestor
from graph_ingest.ingest_scripts.ingest_edge_platform_instrument import PlatformInstrumentRelationshipIngestor
from graph_ingest.tests.fixtures.generated_test_data import (
    MOCK_PLATFORM,
    MOCK_INSTRUMENT,
    MOCK_EDGE_HAS_INSTRUMENT
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

@patch('graph_ingest.ingest_scripts.ingest_node_platform.load_config')
@patch('graph_ingest.ingest_scripts.ingest_node_platform.setup_logger')
@patch('graph_ingest.ingest_scripts.ingest_node_platform.get_driver')
def test_platform_ingestion(mock_get_driver, mock_setup_logger, mock_load_config, neo4j_driver, mock_config):
    """Test ingesting a platform node."""
    # Mock config and logger
    mock_load_config.return_value = mock_config
    mock_logger = MagicMock()
    mock_setup_logger.return_value = mock_logger
    
    # Configure driver mock
    mock_get_driver.return_value = neo4j_driver
    
    # Create platform ingestor
    ingestor = PlatformIngestor()
    
    # Set up uniqueness constraint
    ingestor.set_platform_uniqueness_constraint()
    
    # Create platform node using a transaction
    platform_data = MOCK_PLATFORM['properties']
    with neo4j_driver.session() as session:
        session.execute_write(lambda tx: ingestor.create_platform_node(tx, platform_data))
    
    # Verify the platform was created
    with neo4j_driver.session() as session:
        result = session.run(
            "MATCH (p:Platform {globalId: $globalId}) RETURN p",
            globalId=platform_data['globalId']
        )
        node = result.single()
        assert node is not None
        assert node['p']['shortName'] == platform_data['shortName']
        assert node['p']['longName'] == platform_data['longName']
        assert node['p']['Type'] == platform_data['Type']

@patch('graph_ingest.ingest_scripts.ingest_node_instrument.load_config')
@patch('graph_ingest.ingest_scripts.ingest_node_instrument.setup_logger')
@patch('graph_ingest.ingest_scripts.ingest_node_instrument.get_driver')
def test_instrument_ingestion(mock_get_driver, mock_setup_logger, mock_load_config, neo4j_driver, mock_config):
    """Test ingesting an instrument node."""
    # Mock config and logger
    mock_load_config.return_value = mock_config
    mock_logger = MagicMock()
    mock_setup_logger.return_value = mock_logger
    
    # Configure driver mock
    mock_get_driver.return_value = neo4j_driver
    
    # Create instrument ingestor
    ingestor = InstrumentIngestor()
    
    # Set up uniqueness constraint
    ingestor.set_instrument_uniqueness_constraint()
    
    # Create instrument node using a transaction
    instrument_data = MOCK_INSTRUMENT['properties']
    with neo4j_driver.session() as session:
        session.execute_write(lambda tx: ingestor.create_instrument_node(tx, instrument_data))
    
    # Verify the instrument was created
    with neo4j_driver.session() as session:
        result = session.run(
            "MATCH (i:Instrument {globalId: $globalId}) RETURN i",
            globalId=instrument_data['globalId']
        )
        node = result.single()
        assert node is not None
        assert node['i']['shortName'] == instrument_data['shortName']
        assert node['i']['longName'] == instrument_data['longName']

@patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.load_config')
@patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.setup_logger')
@patch('graph_ingest.ingest_scripts.ingest_node_platform.load_config')
@patch('graph_ingest.ingest_scripts.ingest_node_platform.setup_logger')
@patch('graph_ingest.ingest_scripts.ingest_node_instrument.load_config')
@patch('graph_ingest.ingest_scripts.ingest_node_instrument.setup_logger')
@patch('graph_ingest.ingest_scripts.ingest_node_platform.get_driver')
@patch('graph_ingest.ingest_scripts.ingest_node_instrument.get_driver')
@patch('graph_ingest.ingest_scripts.ingest_edge_platform_instrument.get_driver')
def test_platform_instrument_relationship(
    mock_rel_get_driver, mock_inst_get_driver, mock_plat_get_driver,
    mock_inst_setup_logger, mock_inst_load_config,
    mock_plat_setup_logger, mock_plat_load_config,
    mock_rel_setup_logger, mock_rel_load_config,
    neo4j_driver, mock_config
):
    """Test creating a relationship between a platform and an instrument."""
    # Mock config and logger for all ingestors
    mock_plat_load_config.return_value = mock_config
    mock_inst_load_config.return_value = mock_config
    mock_rel_load_config.return_value = mock_config
    
    mock_logger = MagicMock()
    mock_plat_setup_logger.return_value = mock_logger
    mock_inst_setup_logger.return_value = mock_logger
    mock_rel_setup_logger.return_value = mock_logger
    
    # Configure driver mocks
    mock_plat_get_driver.return_value = neo4j_driver
    mock_inst_get_driver.return_value = neo4j_driver
    mock_rel_get_driver.return_value = neo4j_driver
    
    # Create platform and instrument nodes first
    platform_ingestor = PlatformIngestor()
    platform_ingestor.set_platform_uniqueness_constraint()
    
    with neo4j_driver.session() as session:
        session.execute_write(lambda tx: platform_ingestor.create_platform_node(tx, MOCK_PLATFORM['properties']))
    
    instrument_ingestor = InstrumentIngestor()
    instrument_ingestor.set_instrument_uniqueness_constraint()
    
    with neo4j_driver.session() as session:
        session.execute_write(lambda tx: instrument_ingestor.create_instrument_node(tx, MOCK_INSTRUMENT['properties']))
    
    # Create relationship
    rel_ingestor = PlatformInstrumentRelationshipIngestor()
    
    # Create relationship data
    platform_globalId = platform_ingestor.generate_uuid_from_shortname(MOCK_PLATFORM['properties']['shortName'])
    instrument_globalId = instrument_ingestor.generate_uuid_from_shortname(MOCK_INSTRUMENT['properties']['shortName'])
    
    with neo4j_driver.session() as session:
        session.execute_write(lambda tx: rel_ingestor.create_relationship(tx, platform_globalId, instrument_globalId))
    
    # Verify the relationship was created
    with neo4j_driver.session() as session:
        result = session.run("""
            MATCH (p:Platform {globalId: $platform_id})-[r:HAS_INSTRUMENT]->(i:Instrument {globalId: $instrument_id})
            RETURN r
        """, platform_id=platform_globalId, instrument_id=instrument_globalId)
        assert result.single() is not None

@patch('graph_ingest.ingest_scripts.ingest_node_platform.load_config')
@patch('graph_ingest.ingest_scripts.ingest_node_platform.setup_logger')
@patch('graph_ingest.ingest_scripts.ingest_node_platform.get_driver')
def test_platform_duplicate_ingestion(mock_get_driver, mock_setup_logger, mock_load_config, neo4j_driver, mock_config):
    """Test that duplicate platforms are handled correctly."""
    # Mock config and logger
    mock_load_config.return_value = mock_config
    mock_logger = MagicMock()
    mock_setup_logger.return_value = mock_logger
    
    # Configure driver mock
    mock_get_driver.return_value = neo4j_driver
    
    ingestor = PlatformIngestor()
    ingestor.set_platform_uniqueness_constraint()
    
    # Insert first time
    with neo4j_driver.session() as session:
        session.execute_write(lambda tx: ingestor.create_platform_node(tx, MOCK_PLATFORM['properties']))
    
    # Insert duplicate
    with neo4j_driver.session() as session:
        session.execute_write(lambda tx: ingestor.create_platform_node(tx, MOCK_PLATFORM['properties']))
    
    # Verify only one node exists
    with neo4j_driver.session() as session:
        result = session.run(
            "MATCH (p:Platform {globalId: $globalId}) RETURN count(p) as count",
            globalId=MOCK_PLATFORM['properties']['globalId']
        )
        assert result.single()['count'] == 1

@patch('graph_ingest.ingest_scripts.ingest_node_instrument.load_config')
@patch('graph_ingest.ingest_scripts.ingest_node_instrument.setup_logger')
@patch('graph_ingest.ingest_scripts.ingest_node_instrument.get_driver')
def test_instrument_duplicate_ingestion(mock_get_driver, mock_setup_logger, mock_load_config, neo4j_driver, mock_config):
    """Test that duplicate instruments are handled correctly."""
    # Mock config and logger
    mock_load_config.return_value = mock_config
    mock_logger = MagicMock()
    mock_setup_logger.return_value = mock_logger
    
    # Configure driver mock
    mock_get_driver.return_value = neo4j_driver
    
    ingestor = InstrumentIngestor()
    ingestor.set_instrument_uniqueness_constraint()
    
    # Insert first time
    with neo4j_driver.session() as session:
        session.execute_write(lambda tx: ingestor.create_instrument_node(tx, MOCK_INSTRUMENT['properties']))
    
    # Insert duplicate
    with neo4j_driver.session() as session:
        session.execute_write(lambda tx: ingestor.create_instrument_node(tx, MOCK_INSTRUMENT['properties']))
    
    # Verify only one node exists
    with neo4j_driver.session() as session:
        result = session.run(
            "MATCH (i:Instrument {globalId: $globalId}) RETURN count(i) as count",
            globalId=MOCK_INSTRUMENT['properties']['globalId']
        )
        assert result.single()['count'] == 1 