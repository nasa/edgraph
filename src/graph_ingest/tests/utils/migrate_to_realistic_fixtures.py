#!/usr/bin/env python
"""
Consolidated script to perform all steps of the fixture migration process:
1. Extract realistic fixtures from sample data
2. Generate a new fixtures file
3. Update test_data.py with the new fixtures
4. Analyze existing tests and provide migration guidance
"""

import os
import subprocess
import sys
from pathlib import Path

# Directory structure
CURRENT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
FIXTURES_DIR = TEST_DIR / 'fixtures'

def run_script(script_path):
    """Run a Python script and return its output."""
    result = subprocess.run([sys.executable, script_path], 
                           capture_output=True, 
                           text=True)
    
    print(result.stdout)
    if result.stderr:
        print("ERRORS:")
        print(result.stderr)
    
    return result.returncode == 0

def merge_fixtures():
    """Merge the generated fixtures into test_data.py."""
    generated_path = FIXTURES_DIR / 'generated_test_data.py'
    test_data_path = FIXTURES_DIR / 'test_data.py'
    
    if not generated_path.exists():
        print(f"Error: {generated_path} not found. Run generate_fixtures.py first.")
        return False
    
    if not test_data_path.exists():
        print(f"Error: {test_data_path} not found.")
        return False
    
    # Read the original test_data.py
    with open(test_data_path, 'r') as f:
        test_data_content = f.read()
    
    # Read the generated fixtures
    with open(generated_path, 'r') as f:
        generated_content = f.read()
    
    # Find where to insert the new fixtures in test_data.py
    # We'll add them after the imports
    import_end = test_data_content.rfind('import')
    if import_end == -1:
        # No imports found, just append to the top
        insert_pos = 0
    else:
        # Find the end of the line with the last import
        import_end = test_data_content.find('\n', import_end)
        insert_pos = import_end + 1
    
    # Extract the mock objects from generated_content
    start_marker = '# Node Type Examples'
    end_marker = '# Generated fixtures end'
    content_to_insert = f"\n\n{start_marker}\n"
    content_to_insert += "# The following fixtures are generated from real sample data\n"
    
    # Extract all mock definitions
    new_fixtures = []
    in_fixture = False
    for line in generated_content.split('\n'):
        if line.startswith('MOCK_'):
            in_fixture = True
            new_fixtures.append(line)
        elif in_fixture and line.strip() == '':
            in_fixture = False
        elif in_fixture:
            new_fixtures.append(line)
    
    content_to_insert += '\n'.join(new_fixtures)
    content_to_insert += f"\n\n{end_marker}\n"
    
    # Create the updated content
    updated_content = (
        test_data_content[:insert_pos] + 
        content_to_insert + 
        test_data_content[insert_pos:]
    )
    
    # Create a backup of the original file
    backup_path = test_data_path.with_suffix('.py.bak')
    with open(backup_path, 'w') as f:
        f.write(test_data_content)
    
    # Write the updated file
    with open(test_data_path, 'w') as f:
        f.write(updated_content)
    
    print(f"Updated {test_data_path} with generated fixtures")
    print(f"Backup saved to {backup_path}")
    return True

def main():
    """Main function to run all migration steps."""
    print(f"{'=' * 80}")
    print("EOSDIS Knowledge Graph - Fixture Migration Utility")
    print(f"{'=' * 80}")
    
    # Step 1: Generate fixtures from sample data
    print("\nStep 1: Generating fixtures from sample data...")
    generate_script = CURRENT_DIR / 'generate_fixtures.py'
    if not run_script(generate_script):
        print("Failed to generate fixtures. Exiting.")
        return 1
    
    # Step 2: Merge fixtures into test_data.py
    print("\nStep 2: Merging fixtures into test_data.py...")
    if not merge_fixtures():
        print("Failed to merge fixtures. Exiting.")
        return 1
    
    # Step 3: Analyze existing tests for migration
    print("\nStep 3: Analyzing existing tests for migration...")
    update_script = CURRENT_DIR / 'update_existing_tests.py'
    if not run_script(update_script):
        print("Failed to analyze tests. Exiting.")
        return 1
    
    # Step 4: Compare old and new fixtures
    print("\nStep 4: Comparing old and new fixtures...")
    compare_script = CURRENT_DIR / 'compare_fixtures.py'
    if not run_script(compare_script):
        print("Failed to compare fixtures. Exiting.")
        return 1
    
    print("\nMigration process complete!")
    print("Next steps:")
    print("1. Review the updated fixtures in test_data.py")
    print("2. Update existing tests based on the migration recommendations")
    print("3. Run your test suite to verify the changes")
    
    return 0

if __name__ == "__main__":
    exit(main()) 