# EOSDIS Knowledge Graph Architecture Overview

This document provides a high-level overview of the EOSDIS Knowledge Graph architecture, including its components, data flow, and deployment model.

## System Architecture

The EOSDIS Knowledge Graph is built on a containerized architecture using Docker and Neo4j. The system consists of several key components:

1. **Neo4j Database**: The core graph database that stores all nodes and relationships
2. **Ingest Pipeline**: Python-based scripts that extract, transform, and load data into the graph database
3. **Computation Engine**: Algorithms that compute metrics and enhance the graph with additional properties
4. **Query Interface**: Neo4j Browser and API endpoints for accessing the graph data

## Component Details

### 1. Neo4j Database

- **Technology**: Neo4j 5.x Community Edition
- **Plugins**: Graph Data Science Library, APOC
- **Configuration**: Custom configuration for performance optimization
- **Data Volume**: Persisted Docker volume for data durability

### 2. Ingest Pipeline

The ingest pipeline is composed of multiple Python scripts that handle different aspects of data ingestion:

#### Node Ingestion Scripts:
- `ingest_node_dataset.py`: Ingests dataset metadata
- `ingest_node_datacenter.py`: Ingests data center information
- `ingest_node_platform.py`: Ingests platform data
- `ingest_node_instrument.py`: Ingests instrument data
- `ingest_node_project.py`: Ingests project information
- `ingest_node_publication.py`: Ingests publication metadata
- `ingest_node_edge_gcmd_sciencekeywords.py`: Ingests science keyword hierarchies

#### Edge Ingestion Scripts:
- `ingest_edge_datacenter_dataset.py`: Creates relationships between data centers and datasets
- `ingest_edge_dataset_project.py`: Links datasets to projects
- `ingest_edge_dataset_platform.py`: Links datasets to platforms
- `ingest_edge_platform_instrument.py`: Links platforms to instruments
- `ingest_edge_publication_dataset.py`: Links publications to datasets
- `ingest_edge_dataset_sciencekeyword.py`: Links datasets to science keywords
- `ingest_node_edge_publications_publications.py`: Creates citation relationships
- `ingest_edge_publication_applied_research_area.py`: Links publications to research areas

#### Utilities:
- `common/core.py`: Core utility functions
- `common/config_reader.py`: Configuration management
- `common/dbconfig.py`: Database connection handling
- `common/logger_setup.py`: Logging configuration
- `common/neo4j_driver.py`: Neo4j driver management

### 3. Computation Engine

The computation engine applies graph algorithms to enhance the knowledge graph:

- `ingest_compute_pagerank.py`: Computes PageRank for publications and datasets
- `ingest_compute_fastrp.py`: Generates FastRP embeddings for similarity calculations

### 4. Data Collection

- `get_collections_cmr.py`: Retrieves metadata from NASA's Common Metadata Repository

## Data Flow

1. **Data Collection**:
   - Metadata is collected from authoritative sources like CMR
   - Publication data is collected from citation databases

2. **Data Ingestion**:
   - Nodes are created first (datasets, platforms, instruments, etc.)
   - Relationships are established between nodes
   - Basic data validation and cleaning is performed

3. **Computation**:
   - Graph algorithms are applied to compute metrics
   - Embeddings are generated for similarity searches

4. **Data Access**:
   - Data is queried through Neo4j Browser
   - API endpoints provide programmatic access
   - Dataset snapshots are published to Hugging Face

## Deployment Model

The system is deployed using Docker Compose with the following services:

### Neo4j Service
- Contains the Neo4j database instance
- Exposes ports 7474 (HTTP) and 7687 (Bolt)
- Mounts persistent volume for data storage
- Includes Graph Data Science and APOC plugins

### Graph Ingest Service
- Contains the Python ingest scripts and utilities
- Depends on Neo4j service
- Runs ingest scripts in sequence
- Can be enabled/disabled via environment variables

## Scaling Considerations

- **Database Scaling**: Neo4j can be scaled to enterprise edition for larger deployments
- **Batch Processing**: The ingest pipeline uses batch processing for efficiency
- **Memory Management**: Database configuration is optimized for memory usage
- **Incremental Updates**: The system supports incremental data updates

## Future Architectural Improvements

1. **API Layer**: Add a dedicated API layer for programmatic access
2. **Real-time Updates**: Implement near real-time updates for certain data types
3. **Monitoring**: Add comprehensive monitoring and alerting
4. **Caching**: Implement query caching for frequently accessed patterns
5. **Distributed Processing**: Scale the ingest pipeline for larger datasets
