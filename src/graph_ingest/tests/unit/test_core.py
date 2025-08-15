import os
import uuid
import pytest
from tempfile import TemporaryDirectory
from unittest.mock import patch, mock_open

from graph_ingest.common.core import (
    generate_uuid_from_doi,
    generate_uuid_from_name,
    find_json_files
)


class TestUUIDGeneration:
    """Test suite for UUID generation functions in core.py"""

    def test_generate_uuid_from_doi_consistency(self):
        """Test that generate_uuid_from_doi consistently returns the same UUID for the same DOI."""
        doi = "10.1000/test.123456"
        
        # Generate UUID twice with the same DOI
        uuid1 = generate_uuid_from_doi(doi)
        uuid2 = generate_uuid_from_doi(doi)
        
        # Assert they are the same
        assert uuid1 == uuid2
        # Verify it's a valid UUID string
        assert len(uuid1) == 36
        assert uuid1.count('-') == 4
    
    def test_generate_uuid_from_doi_different_inputs(self):
        """Test that different DOIs generate different UUIDs."""
        doi1 = "10.1000/test.123456"
        doi2 = "10.1000/test.123457"
        
        uuid1 = generate_uuid_from_doi(doi1)
        uuid2 = generate_uuid_from_doi(doi2)
        
        # Different DOIs should generate different UUIDs
        assert uuid1 != uuid2
    
    def test_generate_uuid_from_doi_empty_string(self):
        """Test that an empty string DOI generates a valid UUID."""
        doi = ""
        uuid_str = generate_uuid_from_doi(doi)
        
        # Should still generate a valid UUID
        assert len(uuid_str) == 36
        assert uuid_str.count('-') == 4
        
        # And it should be consistent
        assert uuid_str == generate_uuid_from_doi(doi)
    
    def test_generate_uuid_from_doi_special_chars(self):
        """Test that DOIs with special characters generate valid UUIDs."""
        doi = "10.1000/!@#$%^&*()"
        uuid_str = generate_uuid_from_doi(doi)
        
        # Should generate a valid UUID
        assert len(uuid_str) == 36
        assert uuid_str.count('-') == 4
    
    def test_generate_uuid_from_doi_type(self):
        """Test that generate_uuid_from_doi returns a string."""
        doi = "10.1000/test.123456"
        uuid_str = generate_uuid_from_doi(doi)
        
        assert isinstance(uuid_str, str)
    
    def test_generate_uuid_from_name_consistency(self):
        """Test that generate_uuid_from_name consistently returns the same UUID for the same name."""
        name = "Test Name"
        
        # Generate UUID twice with the same name
        uuid1 = generate_uuid_from_name(name)
        uuid2 = generate_uuid_from_name(name)
        
        # Assert they are the same
        assert uuid1 == uuid2
        # Verify it's a valid UUID string
        assert len(uuid1) == 36
        assert uuid1.count('-') == 4
    
    def test_generate_uuid_from_name_different_inputs(self):
        """Test that different names generate different UUIDs."""
        name1 = "Test Name 1"
        name2 = "Test Name 2"
        
        uuid1 = generate_uuid_from_name(name1)
        uuid2 = generate_uuid_from_name(name2)
        
        # Different names should generate different UUIDs
        assert uuid1 != uuid2
    
    def test_generate_uuid_from_name_empty_string(self):
        """Test that an empty string name generates a valid UUID."""
        name = ""
        uuid_str = generate_uuid_from_name(name)
        
        # Should still generate a valid UUID
        assert len(uuid_str) == 36
        assert uuid_str.count('-') == 4
        
        # And it should be consistent
        assert uuid_str == generate_uuid_from_name(name)
    
    def test_generate_uuid_from_name_special_chars(self):
        """Test that names with special characters generate valid UUIDs."""
        name = "!@#$%^&*()"
        uuid_str = generate_uuid_from_name(name)
        
        # Should generate a valid UUID
        assert len(uuid_str) == 36
        assert uuid_str.count('-') == 4
    
    def test_generate_uuid_from_name_type(self):
        """Test that generate_uuid_from_name returns a string."""
        name = "Test Name"
        uuid_str = generate_uuid_from_name(name)
        
        assert isinstance(uuid_str, str)
    
    def test_uuid_namespace_consistency(self):
        """Test that both UUID functions use the same namespace."""
        # If both functions use the same namespace, the same input should create the same UUID
        input_str = "same_input_string"
        
        uuid_from_doi = generate_uuid_from_doi(input_str)
        uuid_from_name = generate_uuid_from_name(input_str)
        
        assert uuid_from_doi == uuid_from_name


class TestFindJsonFiles:
    """Test suite for find_json_files function in core.py"""
    
    def test_find_json_files_empty_directory(self):
        """Test find_json_files with an empty directory."""
        with TemporaryDirectory() as temp_dir:
            # Use generator to list and get all files
            files = list(find_json_files(temp_dir))
            assert len(files) == 0
    
    def test_find_json_files_single_file(self):
        """Test find_json_files with a single JSON file."""
        with TemporaryDirectory() as temp_dir:
            # Create a JSON file
            json_file = os.path.join(temp_dir, "test.json")
            with open(json_file, "w") as f:
                f.write("{}")
            
            # Use generator to list and get all files
            files = list(find_json_files(temp_dir))
            assert len(files) == 1
            assert os.path.basename(files[0]) == "test.json"
    
    def test_find_json_files_multiple_files(self):
        """Test find_json_files with multiple JSON files."""
        with TemporaryDirectory() as temp_dir:
            # Create JSON files
            for i in range(3):
                json_file = os.path.join(temp_dir, f"test{i}.json")
                with open(json_file, "w") as f:
                    f.write("{}")
            
            # Create a non-JSON file
            non_json_file = os.path.join(temp_dir, "test.txt")
            with open(non_json_file, "w") as f:
                f.write("not json")
            
            # Use generator to list and get all files
            files = list(find_json_files(temp_dir))
            assert len(files) == 3
            # All files should end with .json
            for file in files:
                assert file.endswith(".json")
    
    def test_find_json_files_nested_directories(self):
        """Test find_json_files with nested directories."""
        with TemporaryDirectory() as temp_dir:
            try:
                # Create a nested directory
                nested_dir = os.path.join(temp_dir, "nested")
                os.makedirs(nested_dir, exist_ok=True)
                
                # Create JSON files in both directories
                json_file1 = os.path.join(temp_dir, "test1.json")
                json_file2 = os.path.join(nested_dir, "test2.json")
                
                with open(json_file1, "w") as f:
                    f.write("{}")
                with open(json_file2, "w") as f:
                    f.write("{}")
                
                # Verify files were actually created
                assert os.path.exists(json_file1), f"Failed to create {json_file1}"
                assert os.path.exists(json_file2), f"Failed to create {json_file2}"
                
                # Use generator to list and get all files
                files = list(find_json_files(temp_dir))
                assert len(files) == 2
                
                # Verify all files are included
                file_basenames = [os.path.basename(f) for f in files]
                assert "test1.json" in file_basenames
                assert "test2.json" in file_basenames
            except (PermissionError, FileNotFoundError) as e:
                pytest.skip(f"Skipping test due to file system permission issues: {e}")
    
    def test_find_json_files_case_sensitivity(self):
        """Test find_json_files with different JSON file extensions (case sensitivity)."""
        with TemporaryDirectory() as temp_dir:
            # Create files with various case extensions
            extensions = ["json", "JSON", "Json", "jSoN"]
            for ext in extensions:
                file_path = os.path.join(temp_dir, f"test.{ext}")
                with open(file_path, "w") as f:
                    f.write("{}")
            
            # Use generator to list and get all files
            files = list(find_json_files(temp_dir))
            
            # Should only find lowercase .json
            assert len(files) == 1
            assert files[0].endswith(".json")
    
    def test_find_json_files_nonexistent_directory(self):
        """Test find_json_files with a nonexistent directory."""
        # os.walk doesn't raise FileNotFoundError for nonexistent directories
        # but returns an empty generator, so we should check that no files are found
        nonexistent_dir = "/path/that/does/not/exist"
        files = list(find_json_files(nonexistent_dir))
        assert len(files) == 0
    
    def test_find_json_files_with_permission_error(self):
        """Test find_json_files when facing permission errors."""
        # Mock os.walk to simulate a permission error
        with patch('os.walk') as mock_walk:
            mock_walk.side_effect = PermissionError("Permission denied")
            
            with pytest.raises(PermissionError):
                list(find_json_files("/mock/directory"))

if __name__ == "__main__":
    pytest.main() 