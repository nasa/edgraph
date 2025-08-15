"""Integration tests for complex queries."""

from datetime import datetime, timedelta
from graph_ingest.tests.fixtures.generated_test_data import (
    MOCK_DATASET,
    MOCK_PLATFORM,
    MOCK_INSTRUMENT,
    MOCK_DATACENTER,
    MOCK_PROJECT
)

def test_query_datasets_by_temporal_range(neo4j_driver):
    """Test querying datasets within a specific temporal range."""
    # Create test datasets with different temporal ranges
    base_date = datetime(2020, 1, 1)
    datasets = [
        {**MOCK_DATASET['properties'],
         'globalId': f"{MOCK_DATASET['properties']['globalId']}-{i}",
         'doi': f'doi-{i}',
         'temporalExtentStart': (base_date + timedelta(days=i*30)).isoformat(),
         'temporalExtentEnd': (base_date + timedelta(days=(i+1)*30)).isoformat()}
        for i in range(3)
    ]
    
    with neo4j_driver.session() as session:
        # Clean up any existing test datasets
        for dataset in datasets:
            session.run("""
                MATCH (d:Dataset)
                WHERE d.globalId = $globalId
                DETACH DELETE d
            """, globalId=dataset['globalId'])
        
        for dataset in datasets:
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
            """, dataset)
        
        # Query datasets within a specific range
        result = session.run("""
            MATCH (d:Dataset)
            WHERE datetime($start) <= datetime(d.temporalExtentStart)
            AND datetime(d.temporalExtentEnd) <= datetime($end)
            RETURN count(d) as count
        """, start=base_date.isoformat(),
            end=(base_date + timedelta(days=90)).isoformat())
        
        # Should find all three datasets
        assert result.single()['count'] == 3

def test_find_datacenter_datasets(neo4j_driver):
    """Test finding all datasets from a specific datacenter."""
    with neo4j_driver.session() as session:
        # Create datacenter
        session.run("""
            CREATE (dc:DataCenter {
                globalId: $globalId,
                shortName: $shortName,
                longName: $longName,
                url: $url
            })
        """, MOCK_DATACENTER['properties'])
        
        # Create multiple datasets
        datasets = [
            {**MOCK_DATASET['properties'],
             'doi': f'doi-{i}',
             'globalId': f'dataset-{i}-id'}
            for i in range(3)
        ]
        
        for dataset in datasets:
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
            """, dataset)
            
            # Create relationship between datacenter and dataset
            session.run("""
                MATCH (dc:DataCenter {globalId: $datacenter_id})
                MATCH (d:Dataset {globalId: $dataset_id})
                MERGE (dc)-[:HAS_DATASET]->(d)
            """, datacenter_id=MOCK_DATACENTER['properties']['globalId'],
                dataset_id=dataset['globalId'])
        
        # Query all datasets from the datacenter
        result = session.run("""
            MATCH (dc:DataCenter {globalId: $datacenter_id})-[:HAS_DATASET]->(d:Dataset)
            RETURN count(d) as count
        """, datacenter_id=MOCK_DATACENTER['properties']['globalId'])
        
        assert result.single()['count'] == 3

def test_find_project_datasets(neo4j_driver):
    """Test finding all datasets from a specific project."""
    with neo4j_driver.session() as session:
        # Create project
        session.run("""
            CREATE (p:Project {
                globalId: $globalId,
                shortName: $shortName,
                longName: $longName
            })
        """, MOCK_PROJECT['properties'])
        
        # Create multiple datasets
        datasets = [
            {**MOCK_DATASET['properties'],
             'doi': f'doi-{i}',
             'globalId': f'dataset-{i}-id'}
            for i in range(3)
        ]
        
        for dataset in datasets:
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
            """, dataset)
            
            # Create relationship between dataset and project
            session.run("""
                MATCH (d:Dataset {globalId: $dataset_id})
                MATCH (p:Project {globalId: $project_id})
                MERGE (d)-[:OF_PROJECT]->(p)
            """, dataset_id=dataset['globalId'],
                project_id=MOCK_PROJECT['properties']['globalId'])
        
        # Query all datasets from the project
        result = session.run("""
            MATCH (d:Dataset)-[:OF_PROJECT]->(p:Project {globalId: $project_id})
            RETURN count(d) as count
        """, project_id=MOCK_PROJECT['properties']['globalId'])
        
        assert result.single()['count'] == 3

def test_find_platform_instruments(neo4j_driver):
    """Test finding all instruments on a specific platform."""
    with neo4j_driver.session() as session:
        # Create platform
        session.run("""
            CREATE (p:Platform {
                globalId: $globalId,
                shortName: $shortName,
                longName: $longName,
                Type: $Type
            })
        """, MOCK_PLATFORM['properties'])
        
        # Create multiple instruments
        instruments = [
            {**MOCK_INSTRUMENT['properties'],
             'shortName': f'instrument-{i}',
             'globalId': f'instrument-{i}-id'}
            for i in range(3)
        ]
        
        for instrument in instruments:
            session.run("""
                CREATE (i:Instrument {
                    globalId: $globalId,
                    shortName: $shortName,
                    longName: $longName
                })
            """, instrument)
            
            # Create relationship between platform and instrument
            session.run("""
                MATCH (p:Platform {shortName: $platform_name})
                MATCH (i:Instrument {shortName: $instrument_name})
                CREATE (i)-[:INSTALLED_ON]->(p)
            """, platform_name=MOCK_PLATFORM['properties']['shortName'],
                instrument_name=instrument['shortName'])
        
        # Query all instruments on the platform
        result = session.run("""
            MATCH (i:Instrument)-[:INSTALLED_ON]->(p:Platform {shortName: $platform_name})
            RETURN count(i) as count
        """, platform_name=MOCK_PLATFORM['properties']['shortName'])
        
        assert result.single()['count'] == 3

def test_find_instrument_datasets(neo4j_driver):
    """Test finding all datasets from a specific instrument."""
    with neo4j_driver.session() as session:
        # Create instrument
        session.run("""
            CREATE (i:Instrument {
                globalId: $globalId,
                shortName: $shortName,
                longName: $longName
            })
        """, MOCK_INSTRUMENT['properties'])
        
        # Create multiple datasets
        datasets = [
            {**MOCK_DATASET['properties'],
             'doi': f'doi-{i}',
             'globalId': f'dataset-{i}-id'}
            for i in range(3)
        ]
        
        for dataset in datasets:
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
            """, dataset)
            
            # Create relationship between dataset and instrument
            session.run("""
                MATCH (d:Dataset {doi: $dataset_doi})
                MATCH (i:Instrument {shortName: $instrument_name})
                CREATE (d)-[:COLLECTED_BY_INSTRUMENT]->(i)
            """, dataset_doi=dataset['doi'],
                instrument_name=MOCK_INSTRUMENT['properties']['shortName'])
        
        # Query all datasets from the instrument
        result = session.run("""
            MATCH (d:Dataset)-[:COLLECTED_BY_INSTRUMENT]->(i:Instrument {shortName: $instrument_name})
            RETURN count(d) as count
        """, instrument_name=MOCK_INSTRUMENT['properties']['shortName'])
        
        assert result.single()['count'] == 3 