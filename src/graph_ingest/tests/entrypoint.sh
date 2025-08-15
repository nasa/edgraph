#!/bin/bash
set -e

# Make wait-for-it.sh executable
echo "Setting permissions for wait-for-it.sh..."
chmod +x /app/graph_ingest/wait-for-it.sh

# Wait for Neo4j to be ready
echo "Waiting for Neo4j to be ready..."
/app/graph_ingest/wait-for-it.sh neo4j 7687

# Run tests
echo "Running tests..."
python -m pytest graph_ingest/tests/unit/ -v --cov=graph_ingest.ingest_scripts --cov-report=term-missing --cov-report=xml:/app/test-results/coverage.xml 