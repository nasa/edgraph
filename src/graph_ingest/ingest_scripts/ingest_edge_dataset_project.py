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


class DatasetProjectRelationshipIngestor:
    """
    Ingests relationships between Dataset and Project nodes into Neo4j by processing JSON files.
    """

    def __init__(self) -> None:
        """
        Initialize the DatasetProjectRelationshipIngestor by loading configuration,
        setting up the log directory and logger, and initializing the Neo4j driver.
        """
        self.config: AppConfig = load_config()  # Updated type hint and assignment
        self.log_directory: str = self.config.paths.log_directory  # Updated access
        os.makedirs(self.log_directory, exist_ok=True)
        self.logger: logging.Logger = setup_logger(
            __name__, "dataset_project_relations.log", level=logging.DEBUG, file_level=logging.INFO
        )
        self.driver = get_driver()

    def generate_uuid_from_shortname(self, shortname: str) -> Optional[str]:
        if shortname and isinstance(shortname, str):
            return str(uuid.uuid5(uuid.NAMESPACE_DNS, shortname))
        self.logger.error(f"Invalid or missing short name: {shortname}")
        return None

    def create_relationship(self, tx: Any, dataset_globalId: str, project_globalId: str) -> None:
        if dataset_globalId and project_globalId:
            query: str = """
            MATCH (d:Dataset {globalId: $dataset_globalId}), (p:Project {globalId: $project_globalId})
            MERGE (d)-[:OF_PROJECT]->(p)
            """
            tx.run(query, dataset_globalId=dataset_globalId, project_globalId=project_globalId)

    def process_json_files(self, directory: str, batch_size: int = 100) -> None:
        relationships: List[Tuple[str, str]] = []
        json_files: List[str] = list(find_json_files(directory))
        self.logger.info(f"Found {len(json_files)} JSON files in directory: {directory}")

        for json_file in tqdm(json_files, desc="Processing JSON files", unit="file"):
            try:
                with open(json_file, "r") as file:
                    data = json.load(file)
                    doi: str = data.get("DOI", {}).get("DOI", "")
                    dataset_globalId: Optional[str] = self.generate_uuid_from_shortname(doi) if doi else None
                    if dataset_globalId and "Projects" in data:
                        for project in data["Projects"]:
                            project_short_name: Optional[str] = project.get("ShortName")
                            if project_short_name:
                                project_globalId: Optional[str] = self.generate_uuid_from_shortname(project_short_name)
                                if project_globalId:
                                    relationships.append((dataset_globalId, project_globalId))
                                else:
                                    self.logger.warning(f"Failed to generate UUID for project: {project_short_name}")
                    else:
                        self.logger.warning(f"No DOI or Projects found in file: {json_file}")
            except Exception as e:
                self.logger.error(f"Failed to process file: {json_file}. Error: {e}")

        self.logger.info(f"Total relationships to create: {len(relationships)}")

        total_created: int = 0
        with self.driver.session() as session:
            for i in tqdm(range(0, len(relationships), batch_size), desc="Creating Relationships", unit="batch"):
                batch: List[Tuple[str, str]] = relationships[i:i + batch_size]
                try:
                    session.execute_write(lambda tx: [self.create_relationship(tx, ds, ps) for ds, ps in batch])
                    total_created += len(batch)
                    self.logger.info(f"Processed batch of {len(batch)} relationships. Total created so far: {total_created}")
                except Exception as e:
                    self.logger.error(f"Failed to process batch: {batch}. Error: {e}")

        self.logger.info(f"Total relationships created: {total_created}")

    def run(self) -> None:
        """
        Execute the dataset-project relationship creation process.
        """
        directory: str = self.config.paths.dataset_metadata_directory  # Updated access
        self.process_json_files(directory)


def main() -> None:
    ingestor = DatasetProjectRelationshipIngestor()
    ingestor.run()
    ingestor.logger.info("Relationship creation between Datasets and Projects completed.")
    print("Relationship creation process completed.")


if __name__ == "__main__":
    main()
