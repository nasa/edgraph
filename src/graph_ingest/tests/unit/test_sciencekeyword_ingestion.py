"""Unit tests for GCMD Science Keywords ingestion."""

import os
import json
import logging
import pytest
import pandas as pd
from io import StringIO
from unittest.mock import patch, MagicMock, mock_open, ANY, PropertyMock, call
import tqdm

from graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords import GCMDScienceKeywordIngestor, main
from graph_ingest.tests.fixtures.test_data import MOCK_SCIENCEKEYWORD
# Import the realistic science keyword fixture
from graph_ingest.tests.fixtures.test_data import MOCK_SCIENCEKEYWORD as REALISTIC_SCIENCEKEYWORD
from graph_ingest.common.config_reader import AppConfig, DatabaseConfig, PathsConfig
from graph_ingest.tests.unit.base_test import BaseIngestorTest

class TestGCMDScienceKeywordIngestor(BaseIngestorTest):
    """Tests for the GCMDScienceKeywordIngestor class."""

    def test_initialization(self):
        """Test that the ingestor initializes correctly with mock configuration."""
        with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.load_config', return_value=self.setup_mock_config()):
            with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.get_driver') as mock_get_driver:
                with patch('os.makedirs') as mock_makedirs:
                    mock_driver = MagicMock()
                    mock_get_driver.return_value = mock_driver
                    mock_logger = MagicMock()
                    
                    with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.setup_logger', return_value=mock_logger):
                        ingestor = GCMDScienceKeywordIngestor()
                        
                        # Assert basic properties
                        assert ingestor.config is not None
                        assert ingestor.driver == mock_driver
                        assert ingestor.logger == mock_logger
                        mock_makedirs.assert_called()

    def test_generate_uuid_from_string(self):
        """Test UUID generation from string."""
        with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.load_config', return_value=self.setup_mock_config()):
            with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.get_driver') as mock_get_driver:
                with patch('os.makedirs') as mock_makedirs:
                    with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.setup_logger') as mock_setup_logger:
                        mock_driver = MagicMock()
                        mock_get_driver.return_value = mock_driver
                        mock_logger = MagicMock()
                        mock_setup_logger.return_value = mock_logger
                        
                        ingestor = GCMDScienceKeywordIngestor()
                        uuid1 = ingestor.generate_global_id_from_string("EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION")
                        uuid2 = ingestor.generate_global_id_from_string("EARTH SCIENCE > ATMOSPHERE > ATMOSPHERIC RADIATION")
                        uuid3 = ingestor.generate_global_id_from_string("EARTH SCIENCE > OCEANS > OCEAN CHEMISTRY")
                        
                        # Assert
                        assert uuid1 is not None
                        assert uuid1 == uuid2  # Same input should produce same UUID
                        assert uuid1 != uuid3  # Different input should produce different UUID

    def test_set_sciencekeyword_uniqueness_constraint(self):
        """Test setting uniqueness constraint."""
        with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.load_config', return_value=self.setup_mock_config()):
            with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.get_driver') as mock_get_driver:
                with patch('os.makedirs') as mock_makedirs:
                    with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.setup_logger') as mock_setup_logger:
                        mock_session = MagicMock()
                        mock_driver = MagicMock()
                        mock_driver.session.return_value.__enter__.return_value = mock_session
                        mock_get_driver.return_value = mock_driver
                        mock_logger = MagicMock()
                        mock_setup_logger.return_value = mock_logger
                        
                        ingestor = GCMDScienceKeywordIngestor()
                        ingestor.set_sciencekeyword_uniqueness_constraint()
                        
                        # Assert
                        mock_session.run.assert_called_once()
                        args, kwargs = mock_session.run.call_args
                        assert "CREATE CONSTRAINT" in args[0]

    def test_process_csv(self):
        """Test processing CSV file with mock data."""
        mock_csv_data = """Topic,Term,Variable_Level_1,Variable_Level_2,Variable_Level_3,Detailed_Variable
Earth Science,Atmosphere,Air Quality,Emissions,Carbon Monoxide,Surface Concentration
Earth Science,Atmosphere,Air Quality,Emissions,Carbon Dioxide,"""

        # Mock the pd.read_csv function to avoid csv parsing issues
        mock_df = pd.DataFrame({
            'Topic': ['Earth Science', 'Earth Science'],
            'Term': ['Atmosphere', 'Atmosphere'],
            'Variable_Level_1': ['Air Quality', 'Air Quality'],
            'Variable_Level_2': ['Emissions', 'Emissions'],
            'Variable_Level_3': ['Carbon Monoxide', 'Carbon Dioxide'],
            'Detailed_Variable': ['Surface Concentration', '']
        })

        with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.load_config', return_value=self.setup_mock_config()):
            with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.get_driver') as mock_get_driver, \
                 patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.pd.read_csv', return_value=mock_df), \
                 patch('builtins.open', mock_open(read_data=mock_csv_data)), \
                 patch.object(GCMDScienceKeywordIngestor, 'execute_batch') as mock_execute_batch, \
                 patch('os.makedirs') as mock_makedirs:
                
                mock_driver = MagicMock()
                mock_get_driver.return_value = mock_driver

                ingestor = GCMDScienceKeywordIngestor()
                ingestor.process_csv("mock_file.csv", batch_size=1000)

                # Verify the execute_batch was called at least once
                assert mock_execute_batch.call_count > 0

    def test_execute_batch(self):
        """Test batch execution of operations."""
        with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.load_config', return_value=self.setup_mock_config()), \
             patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.get_driver') as mock_get_driver, \
             patch('os.makedirs'), \
             patch('tqdm.tqdm', lambda x, **kwargs: x), \
             patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.setup_logger') as mock_setup_logger:
            
            mock_tx = MagicMock()
            mock_session = MagicMock()
            mock_session.begin_transaction.return_value.__enter__.return_value = mock_tx
            mock_driver = MagicMock()
            mock_driver.session.return_value.__enter__.return_value = mock_session
            mock_get_driver.return_value = mock_driver
            mock_logger = MagicMock()
            mock_setup_logger.return_value = mock_logger
            
            ingestor = GCMDScienceKeywordIngestor()
            
            # Sample batch of operations - use format expected by execute_batch
            operations = [
                ("node", "EARTH SCIENCE > ATMOSPHERE", "uuid1"),
                ("node", "EARTH SCIENCE > ATMOSPHERE > CLOUDS", "uuid2"),
                ("rel", "uuid1", "uuid2")
            ]
            
            ingestor.execute_batch(operations)
            
            # Assert
            assert mock_tx.run.called  # At least some calls were made
            mock_tx.commit.assert_called_once()

    def test_run(self):
        """Test the full run process."""
        with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.load_config', return_value=self.setup_mock_config()), \
             patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.get_driver') as mock_get_driver, \
             patch('os.makedirs'), \
             patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.setup_logger') as mock_setup_logger, \
             patch.object(GCMDScienceKeywordIngestor, 'process_csv') as mock_process_csv, \
             patch.object(GCMDScienceKeywordIngestor, 'set_sciencekeyword_uniqueness_constraint') as mock_set_constraint:
            
            mock_driver = MagicMock()
            mock_get_driver.return_value = mock_driver
            mock_logger = MagicMock()
            mock_setup_logger.return_value = mock_logger
            
            ingestor = GCMDScienceKeywordIngestor()
            ingestor.run()
            
            # Assert
            mock_set_constraint.assert_called_once()
            mock_process_csv.assert_called_once()

    def test_main_function(self):
        """Test the main function."""
        with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.load_config', return_value=self.setup_mock_config()):
            with patch('graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords.GCMDScienceKeywordIngestor') as mock_ingestor_class:
                mock_ingestor = MagicMock()
                mock_ingestor_class.return_value = mock_ingestor

                main()

                mock_ingestor.run.assert_called_once() 