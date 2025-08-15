import os
import json
import logging
from typing import Any, Dict, List
from tqdm import tqdm

from graph_ingest.common.dbconfig import get_driver
from graph_ingest.common.logger_setup import setup_logger
from graph_ingest.common.core import generate_uuid_from_doi, generate_uuid_from_name, find_json_files
from graph_ingest.common.config_reader import load_config, AppConfig  # Updated import


class RelationshipIngestor:
    """
    Ingests relationships between DataCenter and Dataset nodes into Neo4j.
    
    Loads configuration, sets up logging and a Neo4j driver,
    and processes JSON files to create relationships.
    """
    def __init__(self) -> None:
        """
        Initialize the RelationshipIngestor by loading configuration,
        setting up the log directory, logger, and Neo4j driver.
        """
        self.config: AppConfig = load_config()  # Updated type hint and assignment
        self.log_directory: str = self.config.paths.log_directory  # Updated access
        os.makedirs(self.log_directory, exist_ok=True)
        self.logger: logging.Logger = setup_logger(
            __name__, "relationship_creation.log", level=logging.DEBUG, file_level=logging.INFO
        )
        self.driver = get_driver()

    def batch_create_relationships(self, tx: Any, relationships: List[Dict[str, Any]]) -> None:
        query: str = """
        UNWIND $rels as rel
        MATCH (dc:DataCenter {globalId: rel.dataCenterId}), (ds:Dataset {globalId: rel.datasetId})
        MERGE (dc)-[:HAS_DATASET]->(ds)
        """
        tx.run(query, rels=relationships)

    def process_files(self, directory: str, batch_size: int = 100) -> None:
        json_files = list(find_json_files(directory))
        relationships: List[Dict[str, Any]] = []
        total_relationships = 0
        failed_files = 0

        self.logger.info(f"Starting relationship creation process for directory: {directory}")

        with self.driver.session() as session:
            for json_file in tqdm(json_files, desc="Processing Relationships", unit="file"):
                try:
                    with open(json_file, "r") as file:
                        data = json.load(file)
                        doi = data.get("DOI", {}).get("DOI")
                        if doi:
                            dataset_global_id = generate_uuid_from_doi(doi)
                            if "DataCenters" in data:
                                for center in data["DataCenters"]:
                                    data_center_name = center.get("ShortName")
                                    if data_center_name:
                                        data_center_id = generate_uuid_from_name(data_center_name)
                                        relationships.append({
                                            "dataCenterId": data_center_id,
                                            "datasetId": dataset_global_id,
                                        })
                                    if len(relationships) >= batch_size:
                                        self.logger.info(f"Processing batch of {len(relationships)} relationships.")
                                        session.execute_write(self.batch_create_relationships, relationships)
                                        total_relationships += len(relationships)
                                        relationships = []
                except Exception as e:
                    self.logger.error(f"Failed to process file: {json_file}. Error: {e}")
                    failed_files += 1

            if relationships:
                self.logger.info(f"Processing final batch of {len(relationships)} relationships.")
                session.execute_write(self.batch_create_relationships, relationships)
                total_relationships += len(relationships)

        self.logger.info(f"Total relationships created: {total_relationships}")
        if failed_files > 0:
            self.logger.warning(f"Failed to process {failed_files} files.")
        else:
            self.logger.info("All files processed successfully.")


def main() -> None:
    ingestor = RelationshipIngestor()
    dataset_directory: str = ingestor.config.paths.dataset_metadata_directory  # Updated access
    ingestor.process_files(dataset_directory)
    ingestor.logger.info("Relationship creation process completed.")
    print("Relationship creation process completed.")


if __name__ == "__main__":
    main()
