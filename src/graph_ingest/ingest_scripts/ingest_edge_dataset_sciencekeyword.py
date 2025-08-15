import os
import json
import uuid
import logging
from typing import List, Tuple, Optional
from tqdm import tqdm
from neo4j import GraphDatabase

from graph_ingest.common.config_reader import load_config, AppConfig  # Updated import
from graph_ingest.common.logger_setup import setup_logger


class DatasetScienceKeywordIngestor:
    """
    Ingests relationships between Datasets and Science Keywords in Neo4j.
    """

    def __init__(self) -> None:
        """
        Initializes the ingestor by loading configuration, setting up logging,
        and establishing database connection parameters.
        """
        self.config: AppConfig = load_config()  # Updated to use AppConfig
        self.log_directory = self.config.paths.log_directory  # Updated access
        os.makedirs(self.log_directory, exist_ok=True)

        self.logger = setup_logger(
            __name__, "neo4j_dataset_sciencekeywords_relationships.log", level=logging.DEBUG, file_level=logging.INFO
        )

        # Setup Neo4j connection using config values
        self.uri = self.config.database.uri
        self.user = self.config.database.user
        self.password = self.config.database.password

    def generate_uuid_from_doi(self, doi: str) -> Optional[str]:
        if not doi or not isinstance(doi, str):
            self.logger.error(f"Invalid DOI for UUID generation: {doi}")
            return None
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, doi))

    def generate_uuid_from_string(self, input_string: str) -> Optional[str]:
        if not input_string or not isinstance(input_string, str):
            self.logger.error(f"Invalid string for UUID generation: {input_string}")
            return None
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, input_string))

    def create_relationship(self, tx, dataset_uuid: str, keyword_uuid: str) -> None:
        query = """
        MATCH (d:Dataset {globalId: $dataset_uuid}), (k:ScienceKeyword {globalId: $keyword_uuid})
        MERGE (d)-[:HAS_SCIENCEKEYWORD]->(k)
        """
        tx.run(query, dataset_uuid=dataset_uuid, keyword_uuid=keyword_uuid)

    def find_json_files(self, directory: str) -> List[str]:
        json_files = []
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(".json"):
                    json_files.append(os.path.join(root, file))
        return json_files

    def process_json_files(self, directory: str, batch_size: int = 100) -> None:
        relationships: List[Tuple[str, str]] = []
        json_files = self.find_json_files(directory)

        self.logger.info(f"Found {len(json_files)} JSON files in directory: {directory}")

        for json_file in tqdm(json_files, desc="Processing JSON files", unit="file"):
            try:
                with open(json_file, "r") as file:
                    data = json.load(file)
                    doi = data.get("DOI", {}).get("DOI", "")

                    if doi:
                        dataset_uuid = self.generate_uuid_from_doi(doi)
                        if dataset_uuid and "ScienceKeywords" in data:
                            for item in data["ScienceKeywords"]:
                                for level in [
                                    "Topic", "Term", "Variable_Level_1",
                                    "Variable_Level_2", "Variable_Level_3", "Detailed_Variable"
                                ]:
                                    keyword = item.get(level)
                                    if keyword:
                                        keyword_uuid = self.generate_uuid_from_string(keyword)
                                        if keyword_uuid:
                                            relationships.append((dataset_uuid, keyword_uuid))
                                        else:
                                            self.logger.warning(f"Failed to generate UUID for keyword: {keyword}")

            except Exception as e:
                self.logger.error(f"Failed to process file: {json_file}. Error: {e}")

        self.logger.info(f"Total relationships to process: {len(relationships)}")

        created, failed = 0, 0
        with GraphDatabase.driver(self.uri, auth=(self.user, self.password)) as driver:
            with driver.session() as session:
                for i in tqdm(range(0, len(relationships), batch_size), desc="Creating Relationships", unit="batch"):
                    batch = relationships[i:i + batch_size]
                    for dataset_uuid, keyword_uuid in batch:
                        try:
                            session.execute_write(self.create_relationship, dataset_uuid, keyword_uuid)
                            created += 1
                        except Exception as e:
                            failed += 1
                            self.logger.error(f"Failed to create relationship: Dataset UUID={dataset_uuid}, Keyword UUID={keyword_uuid}. Error: {e}")

        self.logger.info(f"Total relationships processed: {len(relationships)}")
        self.logger.info(f"Relationships created: {created}")
        self.logger.info(f"Relationships failed: {failed}")

    def run(self) -> None:
        """
        Runs the full ingestion process.
        """
        dataset_directory = self.config.paths.dataset_metadata_directory  # Updated access
        self.process_json_files(dataset_directory)
        self.logger.info("Dataset and ScienceKeyword relationship processing completed.")
        print("Dataset-ScienceKeyword relationship creation completed.")


def main() -> None:
    ingestor = DatasetScienceKeywordIngestor()
    ingestor.run()


if __name__ == "__main__":
    main()
