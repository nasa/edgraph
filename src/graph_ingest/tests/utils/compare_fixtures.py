#!/usr/bin/env python
"""
Helper script to compare old and new fixtures.
This script loads both the original test_data.py fixtures and the
newly generated fixtures and prints a comparison to help with migration.
"""

import importlib.util
import os
import sys
from pathlib import Path

def import_module_from_path(module_name, file_path):
    """Import a module from a file path."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

def get_mock_objects(module):
    """Extract all MOCK_* objects from a module."""
    mock_objects = {}
    for name in dir(module):
        if name.startswith('MOCK_'):
            mock_objects[name] = getattr(module, name)
    return mock_objects

def compare_fixtures():
    """Compare old and new fixtures."""
    # Paths
    fixtures_dir = Path('../fixtures')
    old_fixtures_path = fixtures_dir / 'test_data.py'
    new_fixtures_path = fixtures_dir / 'generated_test_data.py'
    
    # Import modules
    old_fixtures = import_module_from_path('old_fixtures', old_fixtures_path)
    new_fixtures = import_module_from_path('new_fixtures', new_fixtures_path)
    
    # Get mock objects
    old_mocks = get_mock_objects(old_fixtures)
    new_mocks = get_mock_objects(new_fixtures)
    
    # Compare
    print(f"{'=' * 80}")
    print("FIXTURE COMPARISON")
    print(f"{'=' * 80}")
    
    print("\nFixtures in OLD test_data.py:")
    for name in sorted(old_mocks.keys()):
        print(f"- {name}")
    
    print("\nFixtures in NEW generated_test_data.py:")
    for name in sorted(new_mocks.keys()):
        print(f"- {name}")
    
    print("\nMigration guidance:")
    for old_name in sorted(old_mocks.keys()):
        # Find similar new fixtures
        similar_new = []
        old_type = old_name.replace('MOCK_', '')
        
        for new_name in new_mocks.keys():
            if old_type in new_name:
                similar_new.append(new_name)
        
        if similar_new:
            print(f"\n{old_name} could be replaced with:")
            for new_name in similar_new:
                print(f"- {new_name}")
        else:
            print(f"\n{old_name} has no direct replacement in the generated fixtures.")

if __name__ == "__main__":
    compare_fixtures()
