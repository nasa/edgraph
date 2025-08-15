# EOSDIS Knowledge Graph

A comprehensive graph database connecting NASA's Earth Observing System Data and Information System (EOSDIS) ecosystem entities using Neo4j.

## Quick Links

- [Documentation Home](docs/README.md)
- [Getting Started](docs/getting-started/quickstart.md)
- [Testing Guide](docs/testing/overview.md)
- [Dataset on Hugging Face](https://huggingface.co/datasets/nasa-gesdisc/nasa-eo-knowledge-graph)


## Documentation

### Getting Started
- [Quick Start Guide](docs/getting-started/quickstart.md) - Get up and running quickly
- [Configuration Guide](docs/getting-started/configuration.md) - Configure the knowledge graph 
### Architecture
- [System Overview](docs/architecture/overview.md) - High-level system architecture
- [Data Model](docs/architecture/data-model.md) - Knowledge graph data model and schema

### Components
- [Ingest Scripts](docs/components/ingest-scripts.md) - Data ingestion pipeline documentation

### Testing
- [Testing Overview](docs/testing/overview.md) - Testing strategy and approach
- [Test Case Tracking](docs/testing/test_case_tracking.md) - Test coverage and tracking

### External Resources
- [Hugging Face Dataset](https://huggingface.co/datasets/nasa-gesdisc/nasa-eo-knowledge-graph) - Knowledge graph data snapshot
- [Neo4j Documentation](https://neo4j.com/docs/) - Neo4j database documentation
- [Cypher Query Language](https://neo4j.com/docs/cypher-manual/current/) - Neo4j query language guide

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.8 or higher (for local development)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/nasa/edgraph
   cd eosdis-knowledge-graph
   ```

### 2. Create a `config.json` file at `src/graph_ingest/config/`. A working example of `config.json` already exist in `src/graph_ingest/config/` configured to work with sample data files. However, neo4j credentials need to be updated.


with the following structure:

```json
{
    "database": {
        "uri": "bolt://<your_neo4j_host>:<bolt_port>",
        "user": "<neo4j_username>",
        "password": "<neo4j_password>"
    },
    "paths": {
        "source_dois_directory": "<path_to_csv_listing_dataset_dois>",
        "dataset_metadata_directory": "<path_to_directory_with_dataset_metadata_files>",
        "gcmd_sciencekeyword_directory": "<path_to_csv_with_gcmd_science_keywords>",
        "publications_metadata_directory": "<path_to_json_with_publication_metadata>",
        "pubs_of_pubs": "<path_to_json_mapping_publications_citing_publications>",
        "log_directory": "<path_to_directory_where_logs_will_be_saved>"
    }
}



3. Start the application:
   ```bash
   docker-compose up --build
   ```

4. Access Neo4j browser at http://localhost:7474

For detailed setup instructions, see the [Installation Guide](docs/getting-started/installation.md).


## Testing

The project includes comprehensive test coverage across different test categories:

### Test Statistics
- Total Tests: 213
  - 212 passing
  - 1 skipped
  - 0 failures
- Overall Coverage: 90%

### Test Categories

1. **Unit Tests**
   - Located in: src/graph_ingest/tests/unit/
   - Purpose: Testing individual components in isolation
   - Features:
     - Mocked dependencies
     - Component-level validation
     - Focused coverage metrics

2. **Integration Tests**
   - Located in: src/graph_ingest/tests/integration/
   - Purpose: Testing component interactions and workflows
   - Key Features:
     - Complex queries across multiple nodes
     - Dataset relationship validation
     - Edge case handling
     - Real database interactions
   - Test Files:
     - Complex queries
     - Dataset relationships
     - Platform-instrument connections
     - Publication management
     - Edge cases

3. **End-to-End Tests**
   - Status: In development
   - Purpose: Testing complete workflows

### Test Infrastructure
- Automated testing through Docker Compose
- Detailed coverage reporting
- Isolated test environments

- Coverage reporting
- Integration tests for:
  - Complex queries across multiple nodes
  - Dataset relationships
  - Edge cases and data validation

For detailed testing information, see our [Testing Guide](docs/testing/overview.md).

## Data Access

The knowledge graph data is available in two primary formats:

1. **Neo4j Database**
   - Full graph database with query capabilities
   - Accessible through Neo4j Browser
   - Supports Cypher query language

2. **Hugging Face Dataset**
   - Snapshot of the knowledge graph data
   - Available at [nasa-gesdisc/nasa-eo-knowledge-graph](https://huggingface.co/datasets/nasa-gesdisc/nasa-eo-knowledge-graph)
   - Includes node and relationship exports
   - Suitable for machine learning applications

## Knowledge Graph Structure

### Data Sources

The knowledge graph integrates data from multiple authoritative sources:

- NASA Common Metadata Repository [CMR](https://www.earthdata.nasa.gov/about/esdis/eosdis/cmr)
- Publication Database
- NASA Global Change Master Directory controlled vocabulary [GCMD](https://www.earthdata.nasa.gov/data/tools/idn/gcmd-keyword-viewer) 

### Node Types

1. **Dataset**
   - Earth science data collections
   - Includes metadata, temporal coverage, and spatial coverage
   - Connected to data centers, platforms, and science keywords

2. **Publication**
   - Scientific papers and research articles
   - Includes DOIs, citations, and publication metadata
   - Links to datasets and other publications

3. **Platform**
   - Satellites and other observation platforms
   - Includes mission information
   - Connected to instruments and datasets

4. **Instrument**
   - Scientific instruments and sensors
   - Connected to platforms
   - Links to datasets through platforms

5. **DataCenter**
   - EOSDIS Distributed Active Archive Centers (DAACs)
   - Data distribution and archival centers
   - Manages and distributes datasets

6. **Project**
   - Research and mission projects
   - Connected to datasets

7. **ScienceKeyword**
   - Hierarchical science categories
   - NASA Global Change Master Directory (GCMD) keywords
   - Used to classify datasets

### Key Relationships

1. **Dataset Relationships**
   - Dataset → Platform (collection-platform association)
   - Dataset → DataCenter (dataset distribution)
   - Dataset → Project (dataset-project association)
   - Dataset → ScienceKeyword (dataset NASA GCMD science keyword association)

2. **Publication Relationships**
   - Publication → Publication (citations)
   - Publication → Dataset (dataset usage in research)
   - Publication → AppliedResearchArea (research applications)

3. **Platform Relationships**
   - Platform → Instrument (platform-instrument configuration)

4. **Science Keyword Relationships**
   - ScienceKeyword → ScienceKeyword (keyword hierarchy)

### Graph Computations

The knowledge graph includes computed metrics:
- PageRank scores for publications and datasets
- FastRP embeddings for graph machine learning

## Use Cases

The knowledge graph enables various applications:

1. **Data Discovery**
   - Find relevant datasets through publication citations
   - Discover datasets and publication in particular Applied Research Areas
   - Navigate science keyword hierarchies

2. **Research Impact Analysis**
   - Track dataset usage in publications
   - Analyze citation networks
   - Measure research impact

3. **Scientific Exploration**
   - Find related research in specific domains
   - Explore instrument capabilities
   - Discover cross-disciplinary connections

## Project Status

Current version: v1.0.0
Last updated: April 2025

## License

This project is licensed under the [NASA Open Source Agreement](LICENSE).

## Citation

If you use this knowledge graph in your research, please cite:
```bibtex
@misc {nasa_goddard_earth_sciences_data_and_information_services_center__(ges-disc)_2024,
    author       = { {NASA Goddard Earth Sciences Data and Information Services Center (GES-DISC)} },
    title        = { nasa-eo-knowledge-graph },
    year         = 2024,
    url          = { https://huggingface.co/datasets/nasa-gesdisc/nasa-eo-knowledge-graph },
    doi          = { 10.57967/hf/3463 },
    publisher    = { Hugging Face }
}
``` 