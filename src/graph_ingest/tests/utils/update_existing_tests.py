#!/usr/bin/env python
"""
Script to update existing test files to use the newly generated fixtures.
This script will:
1. Locate existing test files
2. Identify mock data usage patterns
3. Update imports to include the new fixtures
4. Suggest replacements for existing mock data
"""

import os
import re
from pathlib import Path

# Paths
TEST_DIR = Path('../unit')
FIXTURES_DIR = Path('../fixtures')
GENERATED_FIXTURES = 'generated_test_data.py'
EXISTING_FIXTURES = 'test_data.py'

def find_test_files():
    """Find all Python test files in the test directory."""
    test_files = []
    for root, _, files in os.walk(TEST_DIR):
        for file in files:
            if file.startswith('test_') and file.endswith('.py'):
                test_files.append(os.path.join(root, file))
    return test_files

def analyze_test_file(file_path):
    """Analyze a test file for mock data usage."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Look for import statements
    import_pattern = re.compile(r'from\s+.*fixtures\s+import\s+(.*)')
    imports = import_pattern.findall(content)
    
    # Look for mock data usage (approximate)
    mock_pattern = re.compile(r'MOCK_([A-Z_]+)')
    mock_usages = mock_pattern.findall(content)
    
    # Count frequencies of usage
    mock_counts = {}
    for mock in mock_usages:
        if mock in mock_counts:
            mock_counts[mock] += 1
        else:
            mock_counts[mock] = 1
    
    return {
        'imports': imports,
        'mock_usages': list(set(mock_usages)),
        'mock_frequencies': mock_counts
    }

def generate_import_recommendations(analysis, file_path):
    """Generate recommendations for updating imports."""
    rel_path = os.path.relpath(file_path, TEST_DIR)
    
    recommendations = []
    
    # Check if fixtures are imported
    has_fixtures_import = False
    for import_line in analysis['imports']:
        if 'test_data' in import_line:
            has_fixtures_import = True
            break
    
    if not has_fixtures_import and analysis['mock_usages']:
        # Need to add fixtures import
        recommendations.append(
            f"Add import: from ..fixtures.test_data import {', '.join('MOCK_' + usage for usage in analysis['mock_usages'])}"
        )
    
    # Check if we need to add generated fixtures
    needed_from_generated = []
    for mock in analysis['mock_usages']:
        # Check if the mock might be in the generated fixtures
        # This is a heuristic - we don't know for sure without checking the generated file
        if mock in ['DATASET', 'DATACENTER', 'PROJECT', 'PLATFORM', 'INSTRUMENT', 'PUBLICATION', 'SCIENCEKEYWORD']:
            needed_from_generated.append(f"MOCK_{mock}")
    
    if needed_from_generated:
        recommendations.append(
            f"Consider adding: from ..fixtures.generated_test_data import {', '.join(needed_from_generated)}"
        )
    
    return recommendations

def generate_updates_for_file(file_path, analysis):
    """Generate a report of suggested updates for a test file."""
    rel_path = os.path.relpath(file_path, TEST_DIR)
    
    print(f"\n{'=' * 80}")
    print(f"Analysis for {rel_path}")
    print(f"{'=' * 80}")
    
    print("\nCurrent imports from fixtures:")
    if analysis['imports']:
        for imp in analysis['imports']:
            print(f"- {imp}")
    else:
        print("- No imports from fixtures found")
    
    print("\nMock data usage:")
    if analysis['mock_frequencies']:
        for mock, count in analysis['mock_frequencies'].items():
            print(f"- MOCK_{mock}: {count} occurrences")
    else:
        print("- No mock data usage found")
    
    print("\nRecommendations:")
    recommendations = generate_import_recommendations(analysis, file_path)
    if recommendations:
        for rec in recommendations:
            print(f"- {rec}")
    else:
        print("- No recommendations")

def main():
    """Main function to analyze tests and provide update guidance."""
    print("Finding test files...")
    test_files = find_test_files()
    print(f"Found {len(test_files)} test files")
    
    print("\nAnalyzing test files for mock data usage...")
    for file_path in test_files:
        analysis = analyze_test_file(file_path)
        if analysis['mock_usages']:
            generate_updates_for_file(file_path, analysis)
    
    # Create a helper script to compare old and new fixtures
    create_comparison_script()
    
    print("\nAnalysis complete. A helper script to compare old and new fixtures has been created.")
    print("Run it with: python compare_fixtures.py")
    
    return 0

def create_comparison_script():
    """Create a script to help compare old and new fixtures."""
    script_path = Path('../utils/compare_fixtures.py')
    
    script_content = """#!/usr/bin/env python
\"\"\"
Helper script to compare old and new fixtures.
This script loads both the original test_data.py fixtures and the
newly generated fixtures and prints a comparison to help with migration.
\"\"\"

import importlib.util
import os
import sys
from pathlib import Path

def import_module_from_path(module_name, file_path):
    \"\"\"Import a module from a file path.\"\"\"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

def get_mock_objects(module):
    \"\"\"Extract all MOCK_* objects from a module.\"\"\"
    mock_objects = {}
    for name in dir(module):
        if name.startswith('MOCK_'):
            mock_objects[name] = getattr(module, name)
    return mock_objects

def compare_fixtures():
    \"\"\"Compare old and new fixtures.\"\"\"
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
    
    print("\\nFixtures in OLD test_data.py:")
    for name in sorted(old_mocks.keys()):
        print(f"- {name}")
    
    print("\\nFixtures in NEW generated_test_data.py:")
    for name in sorted(new_mocks.keys()):
        print(f"- {name}")
    
    print("\\nMigration guidance:")
    for old_name in sorted(old_mocks.keys()):
        # Find similar new fixtures
        similar_new = []
        old_type = old_name.replace('MOCK_', '')
        
        for new_name in new_mocks.keys():
            if old_type in new_name:
                similar_new.append(new_name)
        
        if similar_new:
            print(f"\\n{old_name} could be replaced with:")
            for new_name in similar_new:
                print(f"- {new_name}")
        else:
            print(f"\\n{old_name} has no direct replacement in the generated fixtures.")

if __name__ == "__main__":
    compare_fixtures()
"""
    
    with open(script_path, 'w') as f:
        f.write(script_content)

if __name__ == "__main__":
    exit(main()) 