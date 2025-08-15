import os
import json
import uuid
import logging
from typing import Any, Dict, List, Optional, Tuple
from tqdm import tqdm

from graph_ingest.common.config_reader import load_config, AppConfig  # Updated import
from graph_ingest.common.logger_setup import setup_logger
from graph_ingest.common.dbconfig import get_driver


class PublicationDatasetRelationshipIngestor:
    """
    Creates relationships between Publication and Dataset nodes in Neo4j by processing a JSON file.
    """

    def __init__(self) -> None:
        """
        Initialize the PublicationDatasetRelationshipIngestor by loading configuration,
        setting up the log directory, configuring the logger(s), and initializing the Neo4j driver.
        """
        self.config: AppConfig = load_config()  # Updated assignment
        self.log_directory: str = self.config.paths.log_directory  # Updated access
        os.makedirs(self.log_directory, exist_ok=True)

        # General logger
        self.logger: logging.Logger = setup_logger(
            __name__, "publication_dataset_connections.log", level=logging.DEBUG, file_level=logging.INFO
        )

        # Logger for missing dataset nodes
        self.missing_logger: logging.Logger = logging.getLogger("missing_datasets")
        missing_handler = logging.FileHandler(os.path.join(self.log_directory, "missing_datasets.log"))
        missing_handler.setLevel(logging.WARNING)
        formatter = logging.Formatter("%(asctime)s|%(name)s|%(levelname)s|%(message)s")
        missing_handler.setFormatter(formatter)
        self.missing_logger.addHandler(missing_handler)

        self.driver = get_driver()

    def generate_uuid_from_doi(self, doi: str) -> Optional[str]:
        if not doi or not isinstance(doi, str):
            self.logger.error(f"Invalid DOI for UUID generation: {doi}")
            return None
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, doi))

    def validate_nodes_before_processing(
        self, tx: Any, publication_globalId: str, dataset_globalId: Optional[str] = None, dataset_shortname: Optional[str] = None
    ) -> bool:
        pub_exists = tx.run(
            "MATCH (pub:Publication {globalId: $publication_globalId}) RETURN pub",
            publication_globalId=publication_globalId
        ).single() is not None

        dataset_exists = False
        if dataset_globalId:
            dataset_exists = tx.run(
                "MATCH (ds:Dataset {globalId: $dataset_globalId}) RETURN ds",
                dataset_globalId=dataset_globalId
            ).single() is not None

        if not dataset_exists and dataset_shortname:
            dataset_exists = tx.run(
                "MATCH (ds:Dataset {shortName: $dataset_shortname}) RETURN ds",
                dataset_shortname=dataset_shortname
            ).single() is not None

        if not pub_exists:
            self.missing_logger.warning(f"Missing Publication | globalId: {publication_globalId}")
            return False
        if not dataset_exists:
            self.missing_logger.warning(f"Missing Dataset | globalId: {dataset_globalId} | Shortname: {dataset_shortname}")
            return False
        return True

    def create_relationship(self, tx: Any, publication_globalId: str,
                            dataset_globalId: Optional[str] = None, dataset_shortname: Optional[str] = None) -> bool:
        if not self.validate_nodes_before_processing(tx, publication_globalId, dataset_globalId, dataset_shortname):
            return False

        query: str = """
        MATCH (pub:Publication {globalId: $publication_globalId})
        MATCH (ds:Dataset {globalId: COALESCE($dataset_globalId, ds.globalId), shortName: COALESCE($dataset_shortname, ds.shortName)})
        MERGE (pub)-[:USES_DATASET]->(ds)
        RETURN ds
        """
        result = tx.run(query, publication_globalId=publication_globalId, dataset_globalId=dataset_globalId, dataset_shortname=dataset_shortname)
        return result.single() is not None

    def process_relationships(self, batch_size: int = 100) -> None:
        file_path: str = self.config.paths.publications_metadata_directory  # Updated access
        try:
            with open(file_path, "r") as file:
                publications_data: List[Dict[str, Any]] = json.load(file)
        except Exception as e:
            self.logger.error(f"Failed to load JSON file: {file_path}. Error: {e}")
            return

        relationships: List[Tuple[str, Optional[str], Optional[str]]] = []

        for publication in publications_data:
            publication_doi: Optional[str] = publication.get("DOI")
            if not publication_doi:
                continue
            publication_globalId: Optional[str] = self.generate_uuid_from_doi(publication_doi)
            if publication_globalId is None:
                continue

            dataset_dois = [tag["tag"].split("doi:")[1] for tag in publication.get("tags", []) if tag["tag"].startswith("doi:")]
            dataset_shortnames = [ref.get("Shortname") for ref in publication.get("Cited-References", []) if ref.get("Shortname")]

            for dataset_doi in dataset_dois:
                dataset_globalId: Optional[str] = self.generate_uuid_from_doi(dataset_doi)
                relationships.append((publication_globalId, dataset_globalId, None))
            for dataset_shortname in dataset_shortnames:
                relationships.append((publication_globalId, None, dataset_shortname))

        self.logger.info(f"Total relationships to process: {len(relationships)}")

        with self.driver.session() as session:
            for i in tqdm(range(0, len(relationships), batch_size), desc="Creating Relationships", unit="batch"):
                batch = relationships[i:i + batch_size]
                for pub_globalId, ds_globalId, ds_shortname in batch:
                    session.execute_write(self.create_relationship, pub_globalId, ds_globalId, ds_shortname)

        self.logger.info(f"Successfully created {len(relationships)} relationships.")

    def run(self) -> None:
        self.process_relationships()


def main() -> None:
    ingestor = PublicationDatasetRelationshipIngestor()
    ingestor.run()
    ingestor.logger.info("Relationship creation between Publications and Datasets completed.")
    print("Relationship creation process completed.")


if __name__ == "__main__":
    main()
