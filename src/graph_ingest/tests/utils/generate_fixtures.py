#!/usr/bin/env python
"""
Script to extract node and edge examples from sample data files and create proper fixtures
for testing the EOSDIS Knowledge Graph ingest scripts.

This script reads the provided sample data files and extracts representative examples
of each node and edge type to create consistent fixtures for testing.
"""

import json
import os
import pprint
import re
from pathlib import Path

# Paths to sample data files
SAMPLE_DIR = Path('../sample_data')
NODES_FILE = SAMPLE_DIR / 'nodes.json'
EDGES_FILE = SAMPLE_DIR / 'edges.json'
OUTPUT_FILE = Path('../fixtures/generated_test_data.py')

def clean_json_content(content):
    """Clean JSON content by removing control characters."""
    # Remove control characters
    content = re.sub(r'[\x00-\x1F\x7F]', '', content)
    return content

def load_json_file(file_path):
    """Load and parse a JSON file."""
    try:
        # Use utf-8-sig encoding to handle UTF-8 BOM
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            content = f.read()
        
        # Clean the content
        cleaned_content = clean_json_content(content)
        
        # Parse the cleaned content
        return json.loads(cleaned_content)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        # Try a more robust approach
        try:
            with open(file_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
                content = f.read()
            # Use a more aggressive cleaning
            cleaned_content = re.sub(r'[^\x20-\x7E\s]', '', content)
            return json.loads(cleaned_content)
        except Exception as e2:
            print(f"Second attempt failed: {e2}")
            raise

def extract_node_examples(nodes_data):
    """Extract one example of each node type from the nodes data."""
    node_types = {}
    
    for item in nodes_data:
        node = item.get('n', {})
        if not node:
            continue
            
        labels = node.get('labels', [])
        if not labels:
            continue
            
        node_type = labels[0]
        
        # Skip if we already have an example of this node type
        if node_type in node_types:
            continue
            
        # Store the node by its type
        node_types[node_type] = {
            'identity': node.get('identity'),
            'labels': node.get('labels'),
            'properties': node.get('properties', {})
        }
    
    return node_types

def extract_edge_examples(edges_data):
    """Extract one example of each edge type from the edges data."""
    edge_types = {}
    
    for item in edges_data:
        edge = item.get('r', {})
        if not edge or 'type' not in edge:
            continue
            
        edge_type = edge.get('type')
        
        # Skip if we already have an example of this edge type
        if edge_type in edge_types:
            continue
            
        # Store both nodes and the relationship
        edge_types[edge_type] = {
            'source_node': item.get('a', {}),
            'relationship': edge,
            'target_node': item.get('b', {})
        }
    
    return edge_types

def format_as_python_dict(obj, indent=4):
    """Format a Python object as a well-formatted dictionary string."""
    return pprint.pformat(obj, indent=indent, width=100)

def generate_fixtures_file(node_examples, edge_examples):
    """Generate a Python file with fixture objects."""
    
    # Create content for the file
    content = [
        '"""',
        'Generated test fixtures based on real sample data.',
        'This file contains example data for each node and edge type in the EOSDIS Knowledge Graph.',
        '"""',
        '',
        '# Generated from sample data files',
        ''
    ]
    
    # Add node fixtures
    content.append('# Node Type Examples')
    for node_type, node_data in node_examples.items():
        var_name = f'MOCK_{node_type.upper()}'
        content.append(f'{var_name} = {format_as_python_dict(node_data)}')
        content.append('')
    
    # Add edge fixtures
    content.append('# Edge Type Examples')
    for edge_type, edge_data in edge_examples.items():
        var_name = f'MOCK_EDGE_{edge_type}'
        content.append(f'{var_name} = {format_as_python_dict(edge_data)}')
        content.append('')
    
    # Write to file
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        f.write('\n'.join(content))
    
    print(f"Generated fixtures file at {OUTPUT_FILE}")

def main():
    """Main function to extract examples and generate fixtures."""
    print("Loading sample data files...")
    
    try:
        nodes_data = load_json_file(NODES_FILE)
        edges_data = load_json_file(EDGES_FILE)
        
        print(f"Loaded {len(nodes_data)} nodes and {len(edges_data)} edges")
        
        # Extract example nodes and edges
        node_examples = extract_node_examples(nodes_data)
        edge_examples = extract_edge_examples(edges_data)
        
        print(f"Extracted examples for {len(node_examples)} node types and {len(edge_examples)} edge types")
        
        # Generate fixtures file
        generate_fixtures_file(node_examples, edge_examples)
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 