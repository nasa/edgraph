"""Utility functions for mocking in tests."""

import json
import builtins
from unittest.mock import patch, mock_open, MagicMock

def patch_open_with_json_data(file_content_map):
    """
    Returns a patcher for builtins.open that will handle JSON data correctly.

    This avoids JSONDecodeError: Expecting value: line 1 column 1 (char 0) 
    which happens when patching file reading.

    Args:
        file_content_map (dict): Map of file paths to content (either strings or Python objects)

    Returns:
        MagicMock: A properly configured mock for builtins.open
    """
    # Convert any Python objects to proper JSON strings
    processed_content = {}
    for path, content in file_content_map.items():
        if not isinstance(content, str):
            content = json.dumps(content)
        processed_content[path] = content

    def _open_mock(filename, *args, **kwargs):
        mock = mock_open(read_data=processed_content.get(filename, ""))()
        return mock

    return patch('builtins.open', side_effect=_open_mock)

def patch_json_load_with_data(json_data_list):
    """
    Returns a patcher for json.load that will return the provided JSON data.

    Args:
        json_data_list (list): List of Python objects to return as JSON data.
                               Each call to json.load will return the next item.

    Returns:
        patch: A patcher for json.load
    """
    mock_json_load = MagicMock(side_effect=json_data_list)
    return patch('json.load', mock_json_load)

def setup_test_file_system(json_files):
    """
    Sets up mocks for file system operations with JSON data.

    Args:
        json_files (dict): Map of file paths to content (either strings or Python objects)

    Returns:
        tuple: (open_patch, json_load_patch)
    """
    # Prepare the data for json.load mocking
    json_data_list = []
    for path, content in json_files.items():
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except json.JSONDecodeError:
                content = {}
        json_data_list.append(content)

    # Create the patchers
    open_patch = patch_open_with_json_data(json_files)
    json_load_patch = patch_json_load_with_data(json_data_list)

    return open_patch, json_load_patch 