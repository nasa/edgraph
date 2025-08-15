"""Integration tests for edge cases."""

import pytest
from neo4j import Session
from graph_ingest.tests.fixtures.generated_test_data import (
    MOCK_DATASET,
    MOCK_PLATFORM,
    MOCK_INSTRUMENT
)

def test_special_characters_in_names(neo4j_driver):
    """Test handling of special characters in names and IDs."""
    special_chars_dataset = {
        **MOCK_DATASET['properties'],
        'shortName': 'Test & Dataset',
        'longName': 'Test Dataset with & and <> symbols',
        'doi': 'doi:10.1234/test&special',
        'abstract': 'Test abstract with special chars: &<>"\''
    }
    
    with neo4j_driver.session() as session:
        # Create dataset with special characters
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
        """, special_chars_dataset)
        
        # Verify dataset can be retrieved
        result = session.run("""
            MATCH (d:Dataset {doi: $doi})
            RETURN d.shortName, d.longName, d.abstract
        """, doi=special_chars_dataset['doi'])
        
        data = result.single()
        assert data['d.shortName'] == special_chars_dataset['shortName']
        assert data['d.longName'] == special_chars_dataset['longName']
        assert data['d.abstract'] == special_chars_dataset['abstract']

def test_large_property_values(neo4j_driver):
    """Test handling of very large property values."""
    large_abstract = 'A' * 10000  # 10KB abstract
    large_dataset = {
        **MOCK_DATASET['properties'],
        'abstract': large_abstract,
        'doi': 'doi:10.1234/large-abstract'
    }
    
    with neo4j_driver.session() as session:
        # Create dataset with large abstract
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
        """, large_dataset)
        
        # Verify dataset can be retrieved with large abstract
        result = session.run("""
            MATCH (d:Dataset {doi: $doi})
            RETURN d.abstract
        """, doi=large_dataset['doi'])
        
        assert result.single()['d.abstract'] == large_abstract

def test_empty_optional_fields(neo4j_driver):
    """Test handling of empty optional fields."""
    dataset_empty_fields = {
        'globalId': MOCK_DATASET['properties']['globalId'],
        'doi': 'doi:10.1234/empty-fields',
        'shortName': MOCK_DATASET['properties']['shortName'],
        'longName': '',  # Empty long name
        'abstract': None,  # Null abstract
        'temporalExtentStart': None,  # Null temporal extent
        'temporalExtentEnd': None
    }
    
    with neo4j_driver.session() as session:
        # Create dataset with empty fields
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
        """, dataset_empty_fields)
        
        # Verify dataset can be retrieved
        result = session.run("""
            MATCH (d:Dataset {doi: $doi})
            RETURN d
        """, doi=dataset_empty_fields['doi'])
        
        data = result.single()['d']
        assert data['longName'] == ''
        assert data['abstract'] is None
        assert data['temporalExtentStart'] is None
        assert data['temporalExtentEnd'] is None

def test_unicode_characters(neo4j_driver):
    """Test handling of Unicode characters in property values."""
    unicode_dataset = {
        **MOCK_DATASET['properties'],
        'shortName': 'Test Dataset 测试数据集',
        'longName': 'Test Dataset with Unicode 测试数据集 and émojis 🚀',
        'doi': 'doi:10.1234/unicode-test',
        'abstract': 'Abstract with Unicode characters: 测试数据集, émojis 🚀, and symbols ∑∞≠∫'
    }
    
    with neo4j_driver.session() as session:
        # Create dataset with Unicode characters
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
        """, unicode_dataset)
        
        # Verify dataset can be retrieved with Unicode characters intact
        result = session.run("""
            MATCH (d:Dataset {doi: $doi})
            RETURN d.shortName, d.longName, d.abstract
        """, doi=unicode_dataset['doi'])
        
        data = result.single()
        assert data['d.shortName'] == unicode_dataset['shortName']
        assert data['d.longName'] == unicode_dataset['longName']
        assert data['d.abstract'] == unicode_dataset['abstract'] 