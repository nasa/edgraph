# Configuration Guide

This guide explains how to configure the EOSDIS Knowledge Graph for different environments and use cases.

## Configuration Overview

The EOSDIS Knowledge Graph uses a JSON-based configuration system that allows for customization of:

- Database connections
- Data source locations
- Logging settings
- Processing parameters

## Configuration File

The main configuration file is located at `src/graph_ingest/config/config.json`. This file contains all the settings needed to run the knowledge graph ingestion pipeline.

### Default Configuration

Here's the default configuration structure:

```json
{
    "database": {
        "uri": "bolt://neo4j:7687",
        "user": "neo4j",
        "password": "test"
    },
    "paths": {
        "source_dois_directory": "/app/graph_ingest/data/all_eosdis_dois.csv",
        "dataset_metadata_directory": "/app/graph_ingest/data/collection_metadata",
        "gcmd_sciencekeyword_directory": "/app/graph_ingest/data/gcmd_sciencekeywords/sciencekeywords.csv",
        "publications_metadata_directory": "/app/graph_ingest/data/eos_combined_dois_zot.json",
        "pubs_of_pubs": "/app/graph_ingest/data/publications_citing_publications.json",
        "log_directory": "/app/graph_ingest/logs/"
    }
}
```

## Configuration Sections

### Database Configuration

The `database` section configures the connection to the Neo4j database:

```json
"database": {
    "uri": "bolt://neo4j:7687",
    "user": "neo4j",
    "password": "your_password"
}
```

- **uri**: The Neo4j connection URI
  - Format: `bolt://hostname:port`
  - For Docker setup: `bolt://neo4j:7687`
  - For local development: `bolt://localhost:7687`
- **user**: Neo4j database username (default: `neo4j`)
- **password**: Neo4j database password

### Paths Configuration

The `paths` section defines the locations of data files and directories:

```json
"paths": {
    "source_dois_directory": "/app/graph_ingest/data/all_eosdis_dois.csv",
    "dataset_metadata_directory": "/app/graph_ingest/data/collection_metadata",
    "gcmd_sciencekeyword_directory": "/app/graph_ingest/data/gcmd_sciencekeywords/sciencekeywords.csv",
    "publications_metadata_directory": "/app/graph_ingest/data/eos_combined_dois_zot.json",
    "pubs_of_pubs": "/app/graph_ingest/data/publications_citing_publications.json",
    "log_directory": "/app/graph_ingest/logs/"
}
```

- **source_dois_directory**: Path to the CSV file containing EOSDIS DOIs
- **dataset_metadata_directory**: Directory containing dataset metadata JSON files
- **gcmd_sciencekeyword_directory**: Path to the CSV file containing GCMD science keywords
- **publications_metadata_directory**: Path to the JSON file containing publication metadata
- **pubs_of_pubs**: Path to the JSON file containing publication citation relationships
- **log_directory**: Directory where log files will be written

## Environment-Specific Configuration

### Docker Environment

When running in Docker, the configuration files use container paths:

- Paths begin with `/app/` which is the working directory in the container
- Database URI uses the service name (`neo4j`) as hostname

### Local Development

For local development, use relative paths:

```json
"paths": {
    "source_dois_directory": "./graph_ingest/data/all_eosdis_dois.csv",
    "dataset_metadata_directory": "./graph_ingest/data/collection_metadata",
    "gcmd_sciencekeyword_directory": "./graph_ingest/data/gcmd_sciencekeywords/sciencekeywords.csv",
    "publications_metadata_directory": "./graph_ingest/data/eos_combined_dois_zot.json",
    "pubs_of_pubs": "./graph_ingest/data/publications_citing_publications.json",
    "log_directory": "./graph_ingest/logs/"
}
```

## Environment Variables

In addition to the configuration file, the system also uses environment variables:

- `NEO4J_URI`: Overrides the database URI
- `NEO4J_USER`: Overrides the database username
- `NEO4J_PASSWORD`: Overrides the database password
- `ENABLE_INGEST`: Controls whether ingest runs automatically (1=enabled, 0=disabled)

Environment variables can be set in the `config.json` file.


```json
{
    "database": {
        "uri": "bolt://<your_neo4j_host>:<bolt_port>",
        "user": "<neo4j_username>",
        "password": "<neo4j_password>"
    },
  ...
}
```

## Advanced Configuration

### Logging Configuration

Logging is configured programmatically in `graph_ingest/common/logger_setup.py`. The default configuration:

- Creates log files in the directory specified by `paths.log_directory`
- Sets different log levels for console and file outputs
- Formats logs with timestamps and log levels

To modify logging behavior, edit the `setup_logger` function in this file.

### Neo4j Database Configuration

The Neo4j database configuration is controlled by the Docker Compose file:

```yaml
neo4j:
  image: neo4j:5.22.0-community
  environment:
    NEO4J_AUTH: ${NEO4J_USER}/${NEO4J_PASSWORD}
    NEO4J_PLUGINS: '["graph-data-science", "apoc"]'
    NEO4J_ACCEPT_LICENSE_AGREEMENT: "yes"
    NEO4J_apoc_export_file_enabled: 'true'
    NEO4J_apoc_import_file_enabled: 'true'
    NEO4J_apoc_import_file_use__neo4j__config: 'true'
    NEO4J_dbms_security_procedures_unrestricted: 'gds.*,apoc.*'
```

These settings:
- Enable Graph Data Science and APOC plugins
- Configure memory allocation
- Set security and access policies

## Customizing the Ingest Pipeline

### Selective Ingestion

To run only specific parts of the ingestion pipeline, edit the `ingest.sh` script:

```bash
# Run only dataset and datacenter ingestion
python -m graph_ingest.ingest_scripts.ingest_node_dataset
python -m graph_ingest.ingest_scripts.ingest_node_datacenter
python -m graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset
```

### Batch Processing

The ingestion scripts include batch processing parameters that can be modified:

- In each ingest script, look for the `batch_size` parameter (default: 100)
- Adjust this value based on your system's memory and performance characteristics

## Testing Configuration

For testing, a separate configuration is provided in `docker-compose.test.yml`:

```yaml
services:
  test:
    build:
      context: ./src
      dockerfile: tests/Dockerfile.test
    environment:
      NEO4J_URI: bolt://neo4j:7687
      NEO4J_USER: neo4j
      NEO4J_PASSWORD: test
    depends_on:
      neo4j:
        condition: service_healthy
```

## Troubleshooting Configuration Issues

### Common Issues

1. **Neo4j Connection Failures**:
   - Verify the Neo4j container is running
   - Confirm the URI, username, and password are correct
   - Check for network connectivity between services

2. **File Not Found Errors**:
   - Verify the data files exist at the configured paths
   - Check for permission issues on the data directories
   - Ensure Docker volume mounts are correctly configured

3. **Memory Issues**:
   - Adjust Neo4j memory settings in Docker Compose
   - Reduce batch sizes in ingestion scripts
   - Increase container memory limits

For more detailed troubleshooting, check the application logs in the configured log directory.
