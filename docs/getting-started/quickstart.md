# EOSDIS Knowledge Graph Quick Start Guide

This quick start guide will help you get up and running with the EOSDIS Knowledge Graph quickly.

## Overview

The EOSDIS Knowledge Graph is a comprehensive graph database that connects NASA's Earth science data resources. This guide will walk you through the process of deploying the system, loading data, and running basic queries.

## Deployment in 5 Minutes

### Prerequisites

- Docker and Docker Compose installed
- Git installed

### Step 1: Clone the Repository

```bash
git clone https://github.com/nasa/edgraph
cd eosdis-knowledge-graph
```

### 2. Create a `config.json` file at `src/graph_ingest/config/`

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

### Step 3: Launch the System

```bash
docker-compose up -d
```

This command will:
1. Download and build the required Docker images
2. Create a Neo4j database container
3. Create a graph ingest service container
4. Start the data ingestion process

### Step 4: Access Neo4j Browser

Once the containers are running, you can access the Neo4j browser:

- Open your web browser and navigate to [http://localhost:7474](http://localhost:7474)
- Default database: neo4j

## Basic Operations

### Checking Knowledge Graph Status

Run this query in the Neo4j Browser to see what types of nodes are in the graph:

```cypher
MATCH (n)
RETURN labels(n) as NodeType, count(*) as Count
ORDER BY Count DESC;
```

### Finding Datasets

To find datasets by keyword:

```cypher
MATCH (d:Dataset)
WHERE d.shortName CONTAINS "MODIS" OR d.longName CONTAINS "MODIS"
RETURN d.shortName, d.doi, d.abstract
LIMIT 10;
```

### Exploring Relationships

To see how datasets are connected to platforms:

```cypher
MATCH (d:Dataset)-[:USES_PLATFORM]->(p:Platform)
RETURN d.shortName, p.shortName
LIMIT 10;
```

### Finding Publications Using a Dataset

To find publications that reference a specific dataset:

```cypher
MATCH (p:Publication)-[:USES_DATASET]->(d:Dataset)
WHERE d.shortName CONTAINS "MODIS"
RETURN p.title, p.doi, p.publicationDate
LIMIT 20;
```

### Exploring Citation Networks

To explore citation networks between publications:

```cypher
MATCH (p1:Publication)-[:CITES]->(p2:Publication)
RETURN p1.title, p2.title
LIMIT 10;
```

## Common Tasks

### Exporting Data

To export data from the knowledge graph:

```cypher
MATCH (d:Dataset)
WHERE d.shortName CONTAINS "MODIS"
CALL apoc.export.json.query(
  "MATCH (d:Dataset) WHERE d.shortName CONTAINS 'MODIS' RETURN d",
  "modis_datasets.json", {}
)
YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
RETURN file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done;
```

### Running Graph Algorithms

To find influential publications using PageRank:

```cypher
MATCH (p:Publication)
WHERE p.pageRank IS NOT NULL
RETURN p.title, p.doi, p.pageRank
ORDER BY p.pageRank DESC
LIMIT 20;
```

### Finding Similar Datasets

To find datasets similar to a specific dataset (if FastRP embeddings have been computed):

```cypher
MATCH (d:Dataset {shortName: "MOD04_L2"})
MATCH (other:Dataset)
WHERE other.shortName <> "MOD04_L2"
WITH d, other, gds.similarity.cosine(d.embedding, other.embedding) AS similarity
WHERE similarity > 0.8
RETURN other.shortName, similarity
ORDER BY similarity DESC
LIMIT 10;
```

## Accessing the Dataset on Hugging Face

If you want to access a snapshot of the knowledge graph data without running the full system:

1. Visit the [NASA EO Knowledge Graph dataset on Hugging Face](https://huggingface.co/datasets/nasa-gesdisc/nasa-eo-knowledge-graph)
2. Download the dataset files
3. Use the data for machine learning, analysis, or visualization

## Next Steps

Now that you're up and running, you might want to:

- Explore the [Data Model](../architecture/data-model.md) to understand the graph structure
- Check the [Architecture Overview](../architecture/overview.md) for system design details

## Troubleshooting

If you encounter issues:

- Check Docker container logs: `docker-compose logs -f`
- Verify Neo4j is running: `docker ps | grep neo4j`
- Make sure ports 7474 and 7687 are not in use by other applications
