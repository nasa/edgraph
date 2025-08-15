import os
import json
import uuid
import logging
from typing import Any, Dict, List, Optional, Tuple
from tqdm import tqdm

from graph_ingest.common.dbconfig import get_driver
from graph_ingest.common.logger_setup import setup_logger
from graph_ingest.common.core import find_json_files
from graph_ingest.common.config_reader import load_config, AppConfig  # Updated import


class DatasetPlatformRelationshipIngestor:
    """
    Ingests relationships between Dataset and Platform nodes into Neo4j by processing JSON files.
    """

    def __init__(self) -> None:
        """
        Initialize the DatasetPlatformRelationshipIngestor by loading configuration,
        setting up the log directory, creating a logger, and initializing the Neo4j driver.
        """
        self.config: AppConfig = load_config()  # Updated to use AppConfig
        self.log_directory: str = self.config.paths.log_directory  # Updated access
        os.makedirs(self.log_directory, exist_ok=True)
        self.logger: logging.Logger = setup_logger(
            __name__, "dataset_platform_relations.log", level=logging.DEBUG, file_level=logging.INFO
        )
        self.driver = get_driver()

    def generate_uuid_from_doi(self, doi: str) -> Optional[str]:
        if not doi:
            self.logger.error("Missing DOI for UUID generation.")
            return None
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(doi)))

    def generate_uuid_from_shortname(self, shortname: str) -> Optional[str]:
        if not shortname:
            self.logger.error("Missing short name for UUID generation.")
            return None
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, shortname))

    def create_platform_dataset_relationship(self, tx: Any, dataset_uuid: str, platform_uuid: str) -> None:
        query: str = """
        MATCH (d:Dataset {globalId: $dataset_uuid}), (p:Platform {globalId: $platform_uuid})
        MERGE (d)-[:HAS_PLATFORM]->(p)
        """
        tx.run(query, dataset_uuid=dataset_uuid, platform_uuid=platform_uuid)
        self.logger.info(f"Relationship created between Dataset UUID={dataset_uuid} and Platform UUID={platform_uuid}")

    def process_json_files(self, directory: str, batch_size: int = 100) -> None:
        relationships: List[Tuple[str, str]] = []
        json_files: List[str] = list(find_json_files(directory))
        self.logger.info(f"Found {len(json_files)} JSON files in directory: {directory}")

        for json_file in tqdm(json_files, desc="Processing JSON files", unit="file"):
            try:
                with open(json_file, "r") as file:
                    data = json.load(file)
                    doi: str = data.get("DOI", {}).get("DOI", "")
                    if doi and "Platforms" in data:
                        dataset_uuid: Optional[str] = self.generate_uuid_from_doi(doi)
                        if not dataset_uuid:
                            continue
                        for platform in data["Platforms"]:
                            short_name: Optional[str] = platform.get("ShortName")
                            if short_name:
                                platform_uuid: Optional[str] = self.generate_uuid_from_shortname(short_name)
                                if dataset_uuid and platform_uuid:
                                    relationships.append((dataset_uuid, platform_uuid))
                                else:
                                    self.logger.warning(
                                        f"Invalid Dataset UUID or Platform UUID for DOI {doi} and Platform {short_name}."
                                    )
                    else:
                        self.logger.warning(f"No DOI or Platforms found in file: {json_file}")
            except Exception as e:
                self.logger.error(f"Failed to process file: {json_file}. Error: {e}")

        self.logger.info(f"Total relationships to process: {len(relationships)}")
        total_created: int = 0

        with self.driver.session() as session:
            for i in tqdm(range(0, len(relationships), batch_size), desc="Creating Relationships", unit="batch"):
                batch: List[Tuple[str, str]] = relationships[i:i + batch_size]
                try:
                    session.execute_write(lambda tx: [
                        self.create_platform_dataset_relationship(tx, ds_uuid, pl_uuid)
                        for ds_uuid, pl_uuid in batch
                    ])
                    total_created += len(batch)
                    self.logger.info(f"Processed batch of {len(batch)} relationships. Total created so far: {total_created}")
                except Exception as e:
                    self.logger.error(f"Failed to process batch: {batch}. Error: {e}")

        self.logger.info(f"Total relationships created: {total_created}")

    def run(self) -> None:
        """
        Execute the dataset-platform relationship ingestion process.
        """
        directory: str = self.config.paths.dataset_metadata_directory  # Updated access
        self.process_json_files(directory)


def main() -> None:
    ingestor = DatasetPlatformRelationshipIngestor()
    ingestor.run()
    ingestor.logger.info("Dataset-Platform relationship processing completed.")
    print("Dataset-Platform relationship processing completed.")


if __name__ == "__main__":
    main()
