# EOSDIS Knowledge Graph Data Model

This document provides an architectural overview of the data model used in the EOSDIS Knowledge Graph project. The data model serves as the foundation for storing, querying, and analyzing the connections between NASA Earth observation datasets and the scientific research that uses them.

## Data Model Overview

The EOSDIS Knowledge Graph uses a property graph model implemented in Neo4j. This model was chosen for its flexibility, performance with highly connected data, and ability to represent complex relationships between different types of entities.

The data model consists of:

1. **Node Labels**: Categories of entities in the graph
2. **Relationship Types**: Connections between nodes 
3. **Properties**: Attributes on nodes and relationships
4. **Constraints and Indexes**: Rules and optimizations for the database

## Core Entity Types (Nodes)

The knowledge graph contains seven primary node types:

### Dataset
Represents satellite datasets from NASA DAACs and other governmental and academic data providers.

**Key Properties**:
- `globalId`: Unique identifier
- `doi`: Digital Object Identifier
- `shortName`: Abbreviated dataset name
- `temporalExtentStart`/`temporalExtentEnd`: Time coverage
- `nwCorner`/`seCorner`: Geographic boundaries

### Publication
Scientific publications referencing NASA datasets.

**Key Properties**:
- `globalId`/`DOI`: Unique identifier
- `title`/`abstract`: Content descriptors
- `authors`: List of authors
- `year`: Publication date

### ScienceKeyword
Scientific keywords from controlled vocabularies used to classify datasets and publications.

**Key Properties**:
- `globalId`: Unique hierarchical identifier
- `name`: Keyword name

### Platform
Satellites and other observation platforms.

**Key Properties**:
- `globalId`: Unique identifier
- `shortName`/`longName`: Platform names
- `Type`: Platform category

### Instrument
Scientific instruments mounted on platforms.

**Key Properties**:
- `globalId`: Unique identifier
- `shortName`/`longName`: Instrument names

### Project
NASA projects that datasets are associated with.

**Key Properties**:
- `globalId`: Unique identifier
- `shortName`/`longName`: Project names

### DataCenter
Organizations (primarily NASA DAACs) distributing datasets.

**Key Properties**:
- `globalId`: Unique identifier
- `shortName`/`longName`: Data center names
- `url`: Organization website

## Relationship Structure

The knowledge graph uses the following relationship types to connect nodes:

| Relationship Type | Source Node | Target Node | Description |
|-------------------|-------------|-------------|-------------|
| HAS_DATASET | DataCenter | Dataset | Connects data centers to datasets they distribute |
| OF_PROJECT | Dataset | Project | Links datasets to their associated projects |
| HAS_PLATFORM | Dataset | Platform | Associates datasets with platforms used to collect data |
| HAS_INSTRUMENT | Platform | Instrument | Connects platforms to instruments they carry |
| HAS_SCIENCEKEYWORD | Dataset | ScienceKeyword | Links datasets to relevant science keywords |
| HAS_SUBCATEGORY | ScienceKeyword | ScienceKeyword | Defines hierarchical relationships between keywords |
| CITES | Publication | Publication | Represents citation relationships between publications |
| HAS_APPLIEDRESEARCHAREA | Publication | ScienceKeyword | Associates publications with research areas |

## Architectural Considerations

### Scalability

The data model is designed to scale to millions of nodes and relationships:

- The model uses globally unique identifiers for all node types
- Relationships are designed to be lean (without complex properties)
- Key properties are indexed for efficient retrieval

### Performance

Query performance considerations in the data model:

- Strategic placement of indexes on frequently filtered properties
- PageRank values pre-computed and stored for efficient sorting
- Science keywords organized hierarchically for multi-level querying

### Extensibility

The model is designed to be extensible:

- New node types can be added without disrupting existing structures
- Additional properties can be added to existing node types
- Relationship types can be expanded as new connections are identified

## Implementation

### Database Schema

The data model is implemented in Neo4j with the following constraints and indexes:

#### Uniqueness Constraints
```cypher
CREATE CONSTRAINT dataset_globalid_unique IF NOT EXISTS
FOR (d:Dataset) REQUIRE d.globalId IS UNIQUE;

CREATE CONSTRAINT publication_doi_unique IF NOT EXISTS
FOR (p:Publication) REQUIRE p.DOI IS UNIQUE;

CREATE CONSTRAINT platform_globalid_unique IF NOT EXISTS
FOR (p:Platform) REQUIRE p.globalId IS UNIQUE;

CREATE CONSTRAINT instrument_globalid_unique IF NOT EXISTS
FOR (i:Instrument) REQUIRE i.globalId IS UNIQUE;

CREATE CONSTRAINT datacenter_globalid_unique IF NOT EXISTS
FOR (d:DataCenter) REQUIRE d.globalId IS UNIQUE;

CREATE CONSTRAINT project_globalid_unique IF NOT EXISTS
FOR (p:Project) REQUIRE p.globalId IS UNIQUE;

CREATE CONSTRAINT sciencekeyword_globalid_unique IF NOT EXISTS
FOR (s:ScienceKeyword) REQUIRE s.globalId IS UNIQUE;
```

#### Performance Indexes
```cypher
CREATE INDEX dataset_shortname_index IF NOT EXISTS
FOR (d:Dataset) ON (d.shortName);

CREATE INDEX dataset_doi_index IF NOT EXISTS
FOR (d:Dataset) ON (d.doi);

CREATE INDEX publication_title_index IF NOT EXISTS
FOR (p:Publication) ON (p.title);

CREATE INDEX publication_year_index IF NOT EXISTS
FOR (p:Publication) ON (p.year);

CREATE INDEX dataset_temporal_start_index IF NOT EXISTS
FOR (d:Dataset) ON (d.temporalExtentStart);

CREATE INDEX pagerank_index IF NOT EXISTS
FOR (n) ON (n.pagerank_publication_dataset);
```

## Data Exchange and Integration

### Import/Export Formats

The data model supports several formats for data exchange:

1. **JSON-Lines Format**: Each line represents a single node or relationship, suitable for streaming ingestion
2. **GraphML Format**: XML-based file format for graphs, compatible with visualization tools
3. **Cypher Format**: Neo4j's native query language format for direct database import/export


## References

- [NASA Earth Observation Knowledge Graph Dataset](https://huggingface.co/datasets/nasa-gesdisc/nasa-eo-knowledge-graph)
- [NASA Common Metadata Repository (CMR)](https://cmr.earthdata.nasa.gov/search/)
- [Neo4j Graph Data Modeling Guidelines](https://neo4j.com/developer/guide-data-modeling/)
- [GCMD Keyword Vocabulary](https://gcmd.earthdata.nasa.gov/KeywordViewer/)
- [Gerasimov et al. (2024). Bridging the Gap: Enhancing Prominence and Provenance of NASA Datasets in Research Publications. Data Science Journal, 23(1)](https://doi.org/10.5334/dsj-2024-001)
