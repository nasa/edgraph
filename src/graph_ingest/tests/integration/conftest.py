"""Integration test fixtures and configuration."""

import os
import time
import pytest
from neo4j import GraphDatabase
from typing import Generator

# Test database configuration
TEST_NEO4J_URI = os.getenv("TEST_NEO4J_URI", "bolt://localhost:7687")
TEST_NEO4J_USER = os.getenv("TEST_NEO4J_USER", "neo4j")
TEST_NEO4J_PASSWORD = os.getenv("TEST_NEO4J_PASSWORD", "testpass123")
MAX_RETRIES = 30  # Maximum number of retries
RETRY_INTERVAL = 2  # Seconds between retries

@pytest.fixture(scope="session")
def neo4j_driver() -> Generator:
    """Create a Neo4j driver for integration tests."""
    driver = GraphDatabase.driver(
        TEST_NEO4J_URI,
        auth=(TEST_NEO4J_USER, TEST_NEO4J_PASSWORD)
    )
    
    # Verify connection with retries
    retries = 0
    last_error = None
    while retries < MAX_RETRIES:
        try:
            with driver.session() as session:
                session.run("MATCH (n) RETURN count(n) as count")
                break  # Connection successful
        except Exception as e:
            last_error = e
            retries += 1
            if retries < MAX_RETRIES:
                time.sleep(RETRY_INTERVAL)
            continue
    else:
        pytest.fail(f"Could not connect to test database after {MAX_RETRIES} retries. Last error: {last_error}")
    
    yield driver
    
    # Cleanup after all tests
    driver.close()

@pytest.fixture(autouse=True)
def clean_database(neo4j_driver):
    """Clean the database before each test."""
    with neo4j_driver.session() as session:
        # Delete all nodes and relationships
        session.run("MATCH (n) DETACH DELETE n")
        
        # Verify database is empty
        result = session.run("MATCH (n) RETURN count(n) as count")
        assert result.single()["count"] == 0

@pytest.fixture
def sample_dataset():
    """Sample dataset for testing."""
    return {
        "globalId": "test-sample-dataset",
        "shortName": "TEST_DATASET",
        "longName": "Test Dataset for Integration Testing",
        "abstract": "This is a test dataset for integration testing",
        "doi": "10.5067/TEST/DATASET",
        "daac": "TEST_DAAC",
        "cmrId": "TEST_CMR_ID",
        "temporalExtentStart": "2020-01-01T00:00:00Z",
        "temporalExtentEnd": "2020-12-31T23:59:59Z",
        "temporalFrequency": "daily"
    }

@pytest.fixture
def sample_publication():
    """Sample publication for testing."""
    return {
        "doi": "10.1234/TEST.2020.001",
        "title": "Test Publication for Integration Testing",
        "authors": ["Test Author 1", "Test Author 2"],
        "year": "2020",
        "abstract": "This is a test publication for integration testing"
    }

@pytest.fixture
def sample_platform():
    """Sample platform for testing."""
    return {
        "shortName": "TEST_PLATFORM",
        "longName": "Test Platform for Integration Testing",
        "type": "satellite"
    }

@pytest.fixture
def sample_instrument():
    """Sample instrument for testing."""
    return {
        "shortName": "TEST_INST",
        "longName": "Test Instrument for Integration Testing",
        "type": "sensor"
    }

@pytest.fixture
def sample_datacenter():
    """Sample data center for testing."""
    return {
        "shortName": "TEST_DAAC",
        "longName": "Test DAAC for Integration Testing",
        "url": "https://test-daac.nasa.gov"
    } 