# Ingest Scripts Documentation

This document describes the data ingestion scripts used to populate the EOSDIS Knowledge Graph with data from various sources.

## Overview

The ingestion pipeline consists of multiple Python scripts that extract data from source files, transform the data into the appropriate format, and load it into the Neo4j graph database. The pipeline follows a specific order to ensure that nodes are created before relationships that depend on them.

## Ingestion Process Flow

The ingestion process follows this general sequence:

1. **Data Collection**: Retrieval of data from external sources (e.g., CMR)
2. **Node Creation**: Creation of all node types in the graph
3. **Edge Creation**: Creation of relationships between existing nodes
4. **Graph Computation**: Execution of graph algorithms for metrics and embeddings

The full ingestion sequence is defined in `src/graph_ingest/ingest.sh`:

```bash
python -m graph_ingest.ingest_scripts.get_collections_cmr
python -m graph_ingest.ingest_scripts.ingest_node_dataset
python -m graph_ingest.ingest_scripts.ingest_node_datacenter
python -m graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset
python -m graph_ingest.ingest_scripts.ingest_node_project
python -m graph_ingest.ingest_scripts.ingest_edge_dataset_project
python -m graph_ingest.ingest_scripts.ingest_node_platform
python -m graph_ingest.ingest_scripts.ingest_edge_dataset_platform
python -m graph_ingest.ingest_scripts.ingest_node_instrument
python -m graph_ingest.ingest_scripts.ingest_edge_platform_instrument
python -m graph_ingest.ingest_scripts.ingest_node_publication
python -m graph_ingest.ingest_scripts.ingest_edge_publication_dataset
python -m graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords
python -m graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword
python -m graph_ingest.ingest_scripts.ingest_node_edge_publications_publications
python -m graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area
python -m graph_ingest.ingest_scripts.ingest_compute_pagerank
python -m graph_ingest.ingest_scripts.ingest_compute_fastrp
```

## Script Categories

### Data Collection Scripts

#### `get_collections_cmr.py`

This script retrieves dataset metadata from NASA's Common Metadata Repository (CMR).

- **Functionality**: Makes HTTP requests to the CMR API to fetch metadata for Earth science datasets
- **Output**: Saves dataset metadata as JSON files in the configured metadata directory
- **Dependencies**: Requires internet access to connect to CMR

### Node Ingestion Scripts

#### `ingest_node_dataset.py`

Creates Dataset nodes from metadata files.

- **Input**: JSON files containing dataset metadata
- **Processing**: Extracts relevant fields, generates UUIDs, and creates Dataset nodes
- **Output**: Dataset nodes in Neo4j with properties like doi, shortName, longName, etc.

#### `ingest_node_datacenter.py`

Creates DataCenter nodes representing NASA DAACs.

- **Input**: JSON files containing data center information
- **Processing**: Extracts data center information and creates DataCenter nodes
- **Output**: DataCenter nodes in Neo4j with properties like shortName, longName, url

#### `ingest_node_platform.py`

Creates Platform nodes representing satellites and other observation platforms.

- **Input**: JSON files containing platform information
- **Processing**: Extracts platform details and creates Platform nodes
- **Output**: Platform nodes in Neo4j with properties like shortName, longName, Type

#### `ingest_node_instrument.py`

Creates Instrument nodes representing scientific instruments on platforms.

- **Input**: JSON files containing instrument information
- **Processing**: Extracts instrument details and creates Instrument nodes
- **Output**: Instrument nodes in Neo4j with properties like shortName, longName

#### `ingest_node_project.py`

Creates Project nodes representing research and mission projects.

- **Input**: JSON files containing project information
- **Processing**: Extracts project details and creates Project nodes
- **Output**: Project nodes in Neo4j with properties like shortName, longName

#### `ingest_node_publication.py`

Creates Publication nodes from publication metadata.

- **Input**: JSON file containing publication metadata
- **Processing**: Extracts publication details, generates UUIDs, and creates Publication nodes
- **Output**: Publication nodes in Neo4j with properties like doi, title, abstract, etc.

#### `ingest_node_edge_gcmd_sciencekeywords.py`

Creates ScienceKeyword nodes and their hierarchical relationships.

- **Input**: CSV file containing GCMD science keywords
- **Processing**: Creates science keyword nodes and their parent-child relationships
- **Output**: ScienceKeyword nodes in Neo4j and PARENT_OF relationships between them

### Edge Ingestion Scripts

#### `ingest_edge_datacenter_dataset.py`

Creates relationships between DataCenter and Dataset nodes.

- **Input**: Dataset metadata with data center information
- **Processing**: Matches existing nodes and creates relationships
- **Output**: HAS_DATASET relationships from DataCenter to Dataset nodes

#### `ingest_edge_dataset_project.py`

Creates relationships between Dataset and Project nodes.

- **Input**: Dataset metadata with project information
- **Processing**: Matches existing nodes and creates relationships
- **Output**: OF_PROJECT relationships from Dataset to Project nodes

#### `ingest_edge_dataset_platform.py`

Creates relationships between Dataset and Platform nodes.

- **Input**: Dataset metadata with platform information
- **Processing**: Matches existing nodes and creates relationships
- **Output**: USES_PLATFORM relationships from Dataset to Platform nodes

#### `ingest_edge_platform_instrument.py`

Creates relationships between Platform and Instrument nodes.

- **Input**: Metadata with platform-instrument associations
- **Processing**: Matches existing nodes and creates relationships
- **Output**: INSTALLED_ON relationships from Instrument to Platform nodes

#### `ingest_edge_publication_dataset.py`

Creates relationships between Publication and Dataset nodes.

- **Input**: Publication metadata with dataset DOIs
- **Processing**: Matches publications to datasets by DOI and creates relationships
- **Output**: USES_DATASET relationships from Publication to Dataset nodes

#### `ingest_edge_dataset_sciencekeyword.py`

Creates relationships between Dataset and ScienceKeyword nodes.

- **Input**: Dataset metadata with science keyword information
- **Processing**: Matches datasets to science keywords and creates relationships
- **Output**: HAS_SCIENCE_KEYWORD relationships from Dataset to ScienceKeyword nodes

#### `ingest_node_edge_publications_publications.py`

Creates citation relationships between Publication nodes.

- **Input**: JSON file containing publication citation information
- **Processing**: Matches citing and cited publications and creates relationships
- **Output**: CITES relationships from citing to cited Publication nodes

#### `ingest_edge_publication_applied_research_area.py`

Creates relationships between Publications and AppliedResearchArea nodes.

- **Input**: Publication metadata and research area classifications
- **Processing**: Uses machine learning to classify publications into research areas
- **Output**: APPLIES_TO relationships from Publication to AppliedResearchArea nodes

### Computation Scripts

#### `ingest_compute_pagerank.py`

Computes PageRank scores for Publication nodes.

- **Input**: The citation network in the graph (CITES relationships)
- **Processing**: Runs the PageRank algorithm using Neo4j Graph Data Science
- **Output**: Updates Publication nodes with pageRank property

#### `ingest_compute_fastrp.py`

Generates FastRP embeddings for nodes.

- **Input**: The knowledge graph structure
- **Processing**: Runs the FastRP algorithm using Neo4j Graph Data Science
- **Output**: Updates nodes with embedding vectors for similarity computations

## Common Script Structure

Most ingestion scripts follow a similar class-based structure:

1. **Initialization**: Load configuration, set up logging, connect to Neo4j
2. **Constraint Creation**: Ensure uniqueness constraints exist on node IDs
3. **Data Extraction**: Read and parse input data files
4. **Data Transformation**: Convert and normalize data
5. **Database Operations**: Create nodes or relationships in Neo4j
6. **Main Execution**: Script entry point that orchestrates the process

Example template:

```python
class DataIngestor:
    def __init__(self):
        # Setup configuration, logging, database connection
        
    def set_uniqueness_constraint(self):
        # Ensure uniqueness constraints exist
        
    def process_files(self):
        # Process input files and load data
        
    def add_data(self, tx, data_batch):
        # Database transaction to add data
        
def main():
    # Script entry point
    ingestor = DataIngestor()
    ingestor.set_uniqueness_constraint()
    ingestor.process_files()
    
if __name__ == "__main__":
    main()
```

## Batch Processing

All ingest scripts use batch processing to improve performance and manage memory usage:

- Data is processed in configurable batches (default: 100 items)
- Each batch is committed as a single transaction
- Progress is tracked and logged

## Error Handling

The scripts include robust error handling:

- Database errors are caught and logged
- File processing errors are isolated to prevent entire job failure
- Uniqueness constraint violations are handled gracefully

## Running Ingest Scripts

### Running the Full Pipeline

To run the complete ingestion pipeline:

```bash
cd src
sh graph_ingest/ingest.sh
```

### Running Individual Scripts

To run a specific ingest script:

```bash
cd src
python -m graph_ingest.ingest_scripts.ingest_node_dataset
```

## Extending the Ingest Pipeline

To add a new data source or node type:

1. Create a new ingest script following the common structure
2. Update the data model documentation
3. Add the script to the ingest.sh sequence

## Monitoring and Logs

All scripts produce detailed logs that can be used to monitor the ingestion process:

- Console output for high-level progress information
- Log files with detailed information, warnings, and errors
- Completion statistics (items created, items skipped, etc.)
