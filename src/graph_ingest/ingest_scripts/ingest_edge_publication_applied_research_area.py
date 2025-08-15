import os
import torch
import numpy as np
import logging
from typing import Any, Dict, List, Optional
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

from graph_ingest.common.config_reader import load_config, AppConfig
from graph_ingest.common.logger_setup import setup_logger
from graph_ingest.common.dbconfig import get_driver
from transformers import AutoTokenizer, AutoModelForSequenceClassification


class PublicationResearchAreaClassifier:
    """
    Classifies publications using a pre-trained Hugging Face model and links them to ScienceKeyword nodes in Neo4j.
    """

    def __init__(self) -> None:
        """
        Initialize the classifier by setting up the ML model, logging, and database connection.
        """
        self.config: AppConfig = load_config()
        self.log_directory: str = self.config.paths.log_directory
        os.makedirs(self.log_directory, exist_ok=True)

        self.logger: logging.Logger = setup_logger(
            __name__, "publication_research_area.log", level=logging.DEBUG, file_level=logging.INFO
        )
        
        self.driver = get_driver()

        self.missing_logger: logging.Logger = logging.getLogger("missing_datasets")
        missing_handler = logging.FileHandler(
            os.path.join(self.log_directory, "missing_datasets.log")
        )
        missing_handler.setLevel(logging.WARNING)
        formatter = logging.Formatter("%(asctime)s|%(name)s|%(levelname)s|%(message)s")
        missing_handler.setFormatter(formatter)
        self.missing_logger.addHandler(missing_handler)

        # Load ML Model
        self._load_model()

        # Statistics
        self.total_processed = 0
        self.total_created = 0
        self.missing_abstracts = 0
        self.missing_keywords = 0

    def _set_seed(self, seed: int = 42) -> None:
        np.random.seed(seed)
        torch.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)

    def _load_model(self) -> None:
        self._set_seed()
        model_name: str = "arminmehrabian/nasa-impact-nasa-smd-ibm-st-v2-classification-finetuned"
        
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.eval()

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)

        self.label_to_id = {
            "Agriculture": 0, "Air Quality": 1, "Atmospheric/Ocean Indicators": 2, "Cryospheric Indicators": 3,
            "Droughts": 4, "Earthquakes": 5, "Ecosystems": 6, "Energy Production/Use": 7, "Environmental Impacts": 8,
            "Floods": 9, "Greenhouse Gases": 10, "Habitat Conversion/Fragmentation": 11, "Heat": 12,
            "Land Surface/Agriculture Indicators": 13, "Public Health": 14, "Severe Storms": 15,
            "Sun-Earth Interactions": 16, "Validation": 17, "Volcanic Eruptions": 18, "Water Quality": 19, "Wildfires": 20
        }
        self.id_to_label = {v: k for k, v in self.label_to_id.items()}

    def predict(self, text: str) -> str:
        inputs = self.tokenizer(
            text, return_tensors="pt", padding=True, truncation=True, max_length=512
        ).to(self.device)

        with torch.no_grad():
            outputs = self.model(**inputs)

        logits = outputs.logits
        probabilities = torch.softmax(logits, dim=-1).cpu().numpy()[0]

        return self.id_to_label[np.argmax(probabilities)]

    def classify_and_link_publications(self) -> None:
        with self.driver.session() as session:
            publications = session.execute_read(self._get_publications)

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(self._classify_publication, pub) for pub in publications]

                for future in tqdm(futures, desc="Classifying publications"):
                    future.result()

            total_skipped = self.missing_abstracts + self.missing_keywords
            self.logger.info(f"Total Processed: {self.total_processed} | Edges Created: {self.total_created} | Total Skipped: {total_skipped}")

    @staticmethod
    def _get_publications(tx: Any) -> List[Dict[str, str]]:
        query = "MATCH (p:Publication) RETURN p.globalId AS globalId, p.abstract AS abstract"
        result = tx.run(query)
        return [{"globalId": record["globalId"], "abstract": record["abstract"]} for record in result]

    @staticmethod
    def _get_science_keyword_global_id(tx: Any, research_area_name: str) -> Optional[str]:
        query = """
        MATCH (sk:ScienceKeyword)
        WHERE toLower(sk.name) = toLower($name)
        RETURN sk.globalId AS globalId
        """
        result = tx.run(query, name=research_area_name)
        record = result.single()
        return record["globalId"] if record else None

    @staticmethod
    def _create_edge(tx: Any, publication_global_id: str, keyword_global_id: str) -> bool:
        query = """
        MATCH (p:Publication {globalId: $publication_global_id}),
              (sk:ScienceKeyword {globalId: $keyword_global_id}) 
        MERGE (p)-[:HAS_APPLIEDRESEARCHAREA]->(sk)
        RETURN COUNT(*) AS created
        """
        result = tx.run(query, publication_global_id=publication_global_id, keyword_global_id=keyword_global_id)
        return result.single()["created"] > 0

    def _classify_publication(self, pub: Dict[str, str]) -> None:
        self.total_processed += 1
        if not pub["abstract"]:
            self.logger.warning(f"Skipping {pub['globalId']} | Reason: Missing abstract")
            self.missing_abstracts += 1
            return

        research_area = self.predict(pub["abstract"])
        self.logger.info(f"Publication {pub['globalId']} classified as {research_area}")

        keyword_global_id = self.driver.session().execute_read(self._get_science_keyword_global_id, research_area)

        if not keyword_global_id:
            self.logger.warning(f"Skipping {pub['globalId']} | Reason: No matching ScienceKeyword for {research_area}")
            self.missing_keywords += 1
            return

        success = self.driver.session().execute_write(self._create_edge, pub["globalId"], keyword_global_id)

        if success:
            self.total_created += 1


if __name__ == "__main__":
    classifier = PublicationResearchAreaClassifier()
    classifier.classify_and_link_publications()
