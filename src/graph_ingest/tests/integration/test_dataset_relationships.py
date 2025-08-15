"""Integration tests for dataset relationships."""

import pytest
from neo4j import Session
from graph_ingest.tests.fixtures.generated_test_data import (
    MOCK_DATASET,
    MOCK_PLATFORM,
    MOCK_INSTRUMENT,
    MOCK_DATACENTER,
    MOCK_PROJECT,
    MOCK_EDGE_HAS_DATASET,
    MOCK_EDGE_OF_PROJECT
)

def test_dataset_datacenter_relationship(neo4j_driver):
    """Test creating a relationship between a dataset and a datacenter."""
    # Create dataset and datacenter nodes first
    with neo4j_driver.session() as session:
        # Create dataset
        session.run("""
            CREATE (d:Dataset {
                globalId: $globalId,
                doi: $doi,
                shortName: $shortName,
                longName: $longName,
                abstract: $abstract,
                temporalExtentStart: $temporalExtentStart,
                temporalExtentEnd: $temporalExtentEnd
            })
        """, MOCK_DATASET['properties'])
        
        # Create datacenter
        session.run("""
            CREATE (dc:DataCenter {
                globalId: $globalId,
                shortName: $shortName,
                longName: $longName,
                url: $url
            })
        """, MOCK_DATACENTER['properties'])
    
    # Create relationship
    with neo4j_driver.session() as session:
        session.run("""
            MATCH (dc:DataCenter {globalId: $datacenter_id}), (d:Dataset {globalId: $dataset_id})
            MERGE (dc)-[:HAS_DATASET]->(d)
        """, datacenter_id=MOCK_DATACENTER['properties']['globalId'],
            dataset_id=MOCK_DATASET['properties']['globalId'])
    
    # Verify the relationship was created
    with neo4j_driver.session() as session:
        result = session.run("""
            MATCH (dc:DataCenter {globalId: $datacenter_id})-[r:HAS_DATASET]->(d:Dataset {globalId: $dataset_id})
            RETURN r
        """, datacenter_id=MOCK_DATACENTER['properties']['globalId'],
            dataset_id=MOCK_DATASET['properties']['globalId'])
        assert result.single() is not None

def test_dataset_project_relationship(neo4j_driver):
    """Test creating a relationship between a dataset and a project."""
    # Create dataset and project nodes first
    with neo4j_driver.session() as session:
        # Create dataset
        session.run("""
            CREATE (d:Dataset {
                globalId: $globalId,
                doi: $doi,
                shortName: $shortName,
                longName: $longName,
                abstract: $abstract,
                temporalExtentStart: $temporalExtentStart,
                temporalExtentEnd: $temporalExtentEnd
            })
        """, MOCK_DATASET['properties'])
        
        # Create project
        session.run("""
            CREATE (p:Project {
                globalId: $globalId,
                shortName: $shortName,
                longName: $longName
            })
        """, MOCK_PROJECT['properties'])
    
    # Create relationship
    with neo4j_driver.session() as session:
        session.run("""
            MATCH (d:Dataset {globalId: $dataset_id}), (p:Project {globalId: $project_id})
            MERGE (d)-[:OF_PROJECT]->(p)
        """, dataset_id=MOCK_DATASET['properties']['globalId'],
            project_id=MOCK_PROJECT['properties']['globalId'])
    
    # Verify the relationship was created
    with neo4j_driver.session() as session:
        result = session.run("""
            MATCH (d:Dataset {globalId: $dataset_id})-[r:OF_PROJECT]->(p:Project {globalId: $project_id})
            RETURN r
        """, dataset_id=MOCK_DATASET['properties']['globalId'],
            project_id=MOCK_PROJECT['properties']['globalId'])
        assert result.single() is not None

def test_invalid_dataset_datacenter_relationship(neo4j_driver):
    """Test handling of invalid dataset-datacenter relationships."""
    # Test with non-existent dataset
    with neo4j_driver.session() as session:
        # Create datacenter but not dataset
        session.run("""
            CREATE (dc:DataCenter {
                globalId: $globalId,
                shortName: $shortName,
                longName: $longName,
                url: $url
            })
        """, MOCK_DATACENTER['properties'])
        
        # Attempt to create relationship
        session.run("""
            MATCH (dc:DataCenter {globalId: $datacenter_id})
            MATCH (d:Dataset {globalId: 'non-existent-id'})
            MERGE (dc)-[:HAS_DATASET]->(d)
        """, datacenter_id=MOCK_DATACENTER['properties']['globalId'])
        
        # Verify no relationship was created
        result = session.run("""
            MATCH ()-[r:HAS_DATASET]->()
            RETURN count(r) as count
        """)
        assert result.single()['count'] == 0

def test_invalid_dataset_project_relationship(neo4j_driver):
    """Test handling of invalid dataset-project relationships."""
    # Test with non-existent project
    with neo4j_driver.session() as session:
        # Create dataset but not project
        session.run("""
            CREATE (d:Dataset {
                globalId: $globalId,
                doi: $doi,
                shortName: $shortName,
                longName: $longName,
                abstract: $abstract,
                temporalExtentStart: $temporalExtentStart,
                temporalExtentEnd: $temporalExtentEnd
            })
        """, MOCK_DATASET['properties'])
        
        # Attempt to create relationship
        session.run("""
            MATCH (d:Dataset {globalId: $dataset_id})
            MATCH (p:Project {globalId: 'non-existent-id'})
            MERGE (d)-[:OF_PROJECT]->(p)
        """, dataset_id=MOCK_DATASET['properties']['globalId'])
        
        # Verify no relationship was created
        result = session.run("""
            MATCH ()-[r:OF_PROJECT]->()
            RETURN count(r) as count
        """)
        assert result.single()['count'] == 0 