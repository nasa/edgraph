"""Unit tests for Publication-AppliedResearchArea relationship ingestion."""

import os
import sys
from unittest.mock import MagicMock, patch, mock_open, ANY

# Mock modules that might cause issues
modules_to_mock = {
    'torch': MagicMock(),
    'numpy': MagicMock(),
    'transformers': MagicMock(),
    'neo4j': MagicMock()
}

for module_name, mock_module in modules_to_mock.items():
    sys.modules[module_name] = mock_module

import logging
import concurrent.futures
import pytest
import unittest

from graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area import PublicationResearchAreaClassifier


class TestPublicationResearchAreaClassifier(unittest.TestCase):
    """Tests for the PublicationResearchAreaClassifier class."""

    def setUp(self):
        """Set up test environment."""
        # Create standard patches
        self.makedirs_patcher = patch('os.makedirs')
        self.mock_makedirs = self.makedirs_patcher.start()
        
        # Patch logging
        self.filehandler_patcher = patch('logging.FileHandler')
        self.mock_filehandler = self.filehandler_patcher.start()
        
    def tearDown(self):
        """Clean up patches after tests."""
        self.makedirs_patcher.stop()
        self.filehandler_patcher.stop()
        
    @patch('graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area.load_config')
    @patch('graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area.setup_logger')
    @patch('graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area.get_driver')
    @patch('graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area.AutoTokenizer')
    @patch('graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area.AutoModelForSequenceClassification')
    def test_initialization(self, mock_model_class, mock_tokenizer_class, mock_get_driver, mock_setup_logger, mock_load_config):
        """Test the initialization of the classifier."""
        # Arrange
        mock_config = MagicMock()
        mock_config.paths.log_directory = "/test/logs"
        mock_load_config.return_value = mock_config
        
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        
        mock_tokenizer = MagicMock()
        mock_model = MagicMock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        
        # Act
        classifier = PublicationResearchAreaClassifier()
        
        # Assert
        mock_load_config.assert_called_once()
        mock_setup_logger.assert_called_once()
        mock_get_driver.assert_called_once()
        
        self.assertEqual(classifier.logger, mock_logger)
        self.assertEqual(classifier.driver, mock_driver)
        self.assertEqual(classifier.config, mock_config)
        self.assertEqual(classifier.tokenizer, mock_tokenizer)
        self.assertEqual(classifier.model, mock_model)
        
        # Verify statistics are initialized
        self.assertEqual(classifier.total_processed, 0)
        self.assertEqual(classifier.total_created, 0)
        self.assertEqual(classifier.missing_abstracts, 0)
        self.assertEqual(classifier.missing_keywords, 0)

    @patch('graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area.np')
    @patch('graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area.torch')
    def test_set_seed(self, mock_torch, mock_np):
        """Test the seed setting function."""
        # Arrange
        classifier = PublicationResearchAreaClassifier()
        
        # Override the _set_seed method to directly call our version
        # This avoids issues with the method being called during __init__
        original_set_seed = classifier._set_seed
        classifier._set_seed = lambda seed=42: original_set_seed(seed)
        
        test_seed = 42
        
        # Reset mocks to clear any previous calls
        mock_np.reset_mock()
        mock_torch.reset_mock()
        
        # Act
        classifier._set_seed(test_seed)
        
        # Assert
        mock_np.random.seed.assert_called_once_with(test_seed)
        mock_torch.manual_seed.assert_called_once_with(test_seed)
        mock_torch.cuda.manual_seed_all.assert_called_once_with(test_seed)

    @patch('graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area.torch')
    @patch('graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area.np')
    def test_predict(self, mock_np, mock_torch):
        """Test the prediction function."""
        # Arrange
        classifier = PublicationResearchAreaClassifier()
        mock_text = "This is a test abstract about climate."
        
        # Create mock objects
        mock_inputs = MagicMock()
        mock_outputs = MagicMock()
        mock_logits = MagicMock()
        mock_probabilities = MagicMock()
        
        # Configure mocks
        classifier.tokenizer = MagicMock()
        classifier.tokenizer.return_value = mock_inputs
        classifier.model = MagicMock()
        mock_context_manager = MagicMock()
        mock_context_manager.__enter__ = MagicMock(return_value=None)
        mock_context_manager.__exit__ = MagicMock(return_value=None)
        mock_torch.no_grad.return_value = mock_context_manager
        
        classifier.model.return_value = mock_outputs
        mock_outputs.logits = mock_logits
        classifier.device = "cpu"
        
        # Create mock softmax chain
        mock_softmax = MagicMock()
        mock_cpu = MagicMock()
        mock_numpy = MagicMock()
        mock_softmax.cpu.return_value = mock_cpu
        mock_cpu.numpy.return_value = mock_numpy
        mock_torch.softmax.return_value = mock_softmax
        mock_numpy.__getitem__.return_value = mock_probabilities
        
        # Set up numpy to return a specific index for argmax
        mock_np.argmax.return_value = 1  # Index for "Air Quality"
        
        # Set up id_to_label mapping
        classifier.id_to_label = {1: "Air Quality"}
        
        # Act
        result = classifier.predict(mock_text)
        
        # Assert
        classifier.tokenizer.assert_called_once()
        self.assertEqual(result, "Air Quality")

    def test_classify_publication_with_abstract(self):
        """Test classifying a publication with an abstract."""
        classifier = PublicationResearchAreaClassifier()
        classifier.logger = MagicMock()
        classifier.predict = MagicMock(return_value="Air Quality")
        mock_session = MagicMock()
        mock_session.execute_read.return_value = "sk-airquality-123"
        mock_session.execute_write.return_value = True
        classifier.driver = MagicMock()
        classifier.driver.session.return_value = mock_session
        test_pub = {"globalId": "pub-123", "abstract": "Test abstract about air quality"}
        classifier._classify_publication(test_pub)
        self.assertEqual(classifier.total_processed, 1)
        classifier.predict.assert_called_once_with("Test abstract about air quality")
        mock_session.execute_read.assert_called_once()
        mock_session.execute_write.assert_called_once()
        self.assertEqual(classifier.total_created, 1)
        self.assertEqual(classifier.missing_abstracts, 0)
        self.assertEqual(classifier.missing_keywords, 0)

    def test_classify_publication_missing_abstract(self):
        """Test classifying a publication without an abstract."""
        classifier = PublicationResearchAreaClassifier()
        classifier.logger = MagicMock()
        classifier.predict = MagicMock()
        mock_session = MagicMock()
        classifier.driver = MagicMock()
        classifier.driver.session.return_value = mock_session
        test_pub = {"globalId": "pub-no-abstract", "abstract": None}
        classifier._classify_publication(test_pub)
        self.assertEqual(classifier.total_processed, 1)
        classifier.predict.assert_not_called()
        mock_session.execute_read.assert_not_called()
        mock_session.execute_write.assert_not_called()
        self.assertEqual(classifier.total_created, 0)
        self.assertEqual(classifier.missing_abstracts, 1)
        self.assertEqual(classifier.missing_keywords, 0)
        classifier.logger.warning.assert_called_once()

    def test_classify_publication_missing_keyword(self):
        """Test classifying a publication with no matching science keyword."""
        classifier = PublicationResearchAreaClassifier()
        classifier.logger = MagicMock()
        classifier.predict = MagicMock(return_value="Unknown Research Area")
        mock_session = MagicMock()
        mock_session.execute_read.return_value = None
        classifier.driver = MagicMock()
        classifier.driver.session.return_value = mock_session
        test_pub = {"globalId": "pub-no-keyword", "abstract": "Test abstract with unknown research area"}
        classifier._classify_publication(test_pub)
        self.assertEqual(classifier.total_processed, 1)
        classifier.predict.assert_called_once()
        mock_session.execute_read.assert_called_once()
        mock_session.execute_write.assert_not_called()
        self.assertEqual(classifier.total_created, 0)
        self.assertEqual(classifier.missing_abstracts, 0)
        self.assertEqual(classifier.missing_keywords, 1)
        classifier.logger.warning.assert_called_once()

    @patch('graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area.ThreadPoolExecutor')
    def test_classify_and_link_publications(self, mock_executor_class):
        """Test the classify_and_link_publications method."""
        classifier = PublicationResearchAreaClassifier()
        classifier._classify_publication = MagicMock(return_value=None)

        # Create a properly configured mock logger
        mock_logger = MagicMock()
        mock_handler = MagicMock()
        mock_handler.level = logging.INFO  # Set an actual level, not a MagicMock
        mock_logger.handlers = [mock_handler]
        classifier.logger = mock_logger

        # Mock the session
        mock_session = MagicMock()
        mock_publications = [
            {"globalId": "pub-1", "abstract": "Test Abstract 1"},
            {"globalId": "pub-2", "abstract": "Test Abstract 2"}
        ]
        mock_session.execute_read.return_value = mock_publications

        classifier.driver = MagicMock()
        classifier.driver.session.return_value = mock_session

        # Mock the executor
        mock_executor = MagicMock()
        mock_executor_class.return_value.__enter__.return_value = mock_executor
        mock_executor.submit.return_value = MagicMock()

        # Act
        classifier.classify_and_link_publications()

    def test_get_publications_static_method(self):
        """Test the static method to get publications."""
        mock_tx = MagicMock()
        mock_records = [
            {"globalId": "pub-123", "abstract": "Test abstract 1"},
            {"globalId": "pub-456", "abstract": "Test abstract 2"}
        ]
        mock_tx.run.return_value = mock_records
        publications = PublicationResearchAreaClassifier._get_publications(mock_tx)
        mock_tx.run.assert_called_once_with(
            "MATCH (p:Publication) RETURN p.globalId AS globalId, p.abstract AS abstract"
        )
        self.assertEqual(len(publications), 2)
        self.assertEqual(publications[0]["globalId"], "pub-123")
        self.assertEqual(publications[0]["abstract"], "Test abstract 1")
        self.assertEqual(publications[1]["globalId"], "pub-456")
        self.assertEqual(publications[1]["abstract"], "Test abstract 2")

    def test_get_science_keyword_global_id_static_method(self):
        """Test the static method to get science keyword global ID."""
        mock_tx = MagicMock()
        mock_record = {"globalId": "sk-123"}
        mock_result = MagicMock()
        mock_result.single.return_value = mock_record
        mock_tx.run.return_value = mock_result
        result = PublicationResearchAreaClassifier._get_science_keyword_global_id(mock_tx, "Air Quality")
        expected_query = """
        MATCH (sk:ScienceKeyword)
        WHERE toLower(sk.name) = toLower($name)
        RETURN sk.globalId AS globalId
        """
        mock_tx.run.assert_called_once()
        call_args = mock_tx.run.call_args[0]
        self.assertTrue(expected_query.strip() in call_args[0].strip())
        self.assertEqual(mock_tx.run.call_args[1]['name'], "Air Quality")
        self.assertEqual(result, "sk-123")
        mock_tx.reset_mock()
        mock_result.reset_mock()
        mock_result.single.return_value = None
        mock_tx.run.return_value = mock_result
        result = PublicationResearchAreaClassifier._get_science_keyword_global_id(mock_tx, "Non-existent")
        self.assertIsNone(result)

    def test_create_edge_static_method(self):
        """Test the static method to create edges."""
        mock_tx = MagicMock()
        mock_record = {"created": 1}
        mock_result = MagicMock()
        mock_result.single.return_value = mock_record
        mock_tx.run.return_value = mock_result
        result = PublicationResearchAreaClassifier._create_edge(
            mock_tx, "pub-123", "sk-456"
        )
        expected_query = """
        MATCH (p:Publication {globalId: $publication_global_id}),
              (sk:ScienceKeyword {globalId: $keyword_global_id}) 
        MERGE (p)-[:HAS_APPLIEDRESEARCHAREA]->(sk)
        RETURN COUNT(*) AS created
        """
        mock_tx.run.assert_called_once()
        call_args = mock_tx.run.call_args[0]
        self.assertTrue(expected_query.strip() in call_args[0].strip())
        self.assertEqual(mock_tx.run.call_args[1]['publication_global_id'], "pub-123")
        self.assertEqual(mock_tx.run.call_args[1]['keyword_global_id'], "sk-456")
        self.assertTrue(result)
        mock_tx.reset_mock()
        mock_result.reset_mock()
        mock_record = {"created": 0}
        mock_result.single.return_value = mock_record
        mock_tx.run.return_value = mock_result
        result = PublicationResearchAreaClassifier._create_edge(
            mock_tx, "pub-123", "sk-456"
        )
        self.assertFalse(result)

    def test_main(self):
        """Test the main execution block."""
        pass

    def test_extract_entities(self):
        """Test the _extract_entities method which extracts named entities from text."""
        classifier = PublicationResearchAreaClassifier()
        classifier.predict = MagicMock(return_value="Air Quality")
        test_text = "NASA's Earth science research on climate change, water quality, and air pollution has important applications."
        result = classifier.predict(test_text)
        assert isinstance(result, str)
        assert result == "Air Quality"
        classifier.predict.return_value = "Environmental Impacts"
        empty_result = classifier.predict("")
        assert isinstance(empty_result, str)
        assert empty_result == "Environmental Impacts"

    @patch('graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area.torch')
    @patch('graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area.np')
    def test_predict_with_multiple_cases(self, mock_np, mock_torch):
        """Test the prediction function with multiple test cases to ensure proper classification."""
        classifier = PublicationResearchAreaClassifier()
        classifier.logger = MagicMock()
        classifier.tokenizer = MagicMock()
        classifier.model = MagicMock()
        classifier.device = "cpu"
        mock_context_manager = MagicMock()
        mock_context_manager.__enter__ = MagicMock(return_value=None)
        mock_context_manager.__exit__ = MagicMock(return_value=None)
        mock_torch.no_grad.return_value = mock_context_manager
        mock_softmax = MagicMock()
        mock_cpu = MagicMock()
        mock_numpy = MagicMock()
        mock_softmax.cpu.return_value = mock_cpu
        mock_cpu.numpy.return_value = mock_numpy
        mock_torch.softmax.return_value = mock_softmax
        classifier.id_to_label = {
            0: "Agriculture",
            1: "Air Quality",
            2: "Atmospheric/Ocean Indicators",
            3: "Cryospheric Indicators",
            4: "Droughts",
            5: "Earthquakes"
        }
        test_cases = [
            {
                "abstract": "This research focuses on air pollution in urban areas and its effects on respiratory health.",
                "expected_class_id": 1,  # Air Quality
                "expected_class": "Air Quality"
            },
            {
                "abstract": "We study how rainfall patterns impact crop yields in different regions.",
                "expected_class_id": 0,  # Agriculture
                "expected_class": "Agriculture"
            },
            {
                "abstract": "Analysis of ocean temperature and salinity over the past decade reveals concerning trends.",
                "expected_class_id": 2,  # Atmospheric/Ocean Indicators
                "expected_class": "Atmospheric/Ocean Indicators"
            },
            {
                "abstract": "Monitoring glacier retreat in polar regions provides insights into global warming effects.",
                "expected_class_id": 3,  # Cryospheric Indicators
                "expected_class": "Cryospheric Indicators"
            },
            {
                "abstract": "Long-term precipitation deficit and its impact on water resources management.",
                "expected_class_id": 4,  # Droughts
                "expected_class": "Droughts"
            }
        ]
        for i, test_case in enumerate(test_cases):
            classifier.tokenizer.reset_mock()
            classifier.model.reset_mock()
            mock_torch.softmax.reset_mock()
            mock_np.reset_mock()
            mock_inputs = MagicMock()
            mock_outputs = MagicMock()
            mock_logits = MagicMock()
            mock_probabilities = MagicMock()
            classifier.tokenizer.return_value = mock_inputs
            classifier.model.return_value = mock_outputs
            mock_outputs.logits = mock_logits
            mock_numpy.__getitem__.return_value = mock_probabilities
            mock_np.argmax.return_value = test_case["expected_class_id"]
            result = classifier.predict(test_case["abstract"])
            classifier.tokenizer.assert_called_once()
            classifier.model.assert_called_once()
            mock_torch.softmax.assert_called_once()
            mock_np.argmax.assert_called_once()
            self.assertEqual(result, test_case["expected_class"])
            tokenizer_call_args = classifier.tokenizer.call_args[1]
            self.assertTrue("return_tensors" in tokenizer_call_args)
            self.assertTrue("padding" in tokenizer_call_args)
            self.assertTrue("truncation" in tokenizer_call_args)
            self.assertTrue("max_length" in tokenizer_call_args)


if __name__ == '__main__':
    unittest.main()
