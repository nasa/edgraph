#!/bin/bash
# Script to set up test environment and run tests

# Exit on error
set -e

# Set working directory
cd "$(dirname "$0")"
BASE_DIR=$(pwd)
SRC_DIR=$(cd ../../.. && pwd)

echo "Setting up test environment..."

# Check if we're in a virtual environment, if not create one
if [[ -z "$VIRTUAL_ENV" ]]; then
    echo "Creating virtual environment for testing..."
    if [[ ! -d ".venv" ]]; then
        python -m venv .venv
    fi
    
    # Activate virtual environment
    source .venv/bin/activate
    
    # Install test requirements
    pip install -r requirements_test.txt
else
    echo "Using existing virtual environment: $VIRTUAL_ENV"
    pip install -r requirements_test.txt
fi

# Run tests with specified arguments or default to all tests
cd "$SRC_DIR"

if [[ $# -eq 0 ]]; then
    echo "Running all tests..."
    python -m pytest graph_ingest/tests/ -v
else
    echo "Running specified tests: $@"
    python -m pytest "$@"
fi

echo "Tests completed."

# Deactivate virtual environment if we created it
if [[ -z "$VIRTUAL_ENV_BEFORE" ]]; then
    deactivate 2>/dev/null || true
fi 