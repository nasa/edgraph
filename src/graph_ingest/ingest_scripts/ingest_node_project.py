import os
import json
import uuid
import logging
from typing import Any, Dict, List, Optional
from tqdm import tqdm

from graph_ingest.common.dbconfig import get_driver
from graph_ingest.common.logger_setup import setup_logger
from graph_ingest.common.core import find_json_files
from graph_ingest.common.config_reader import load_config, AppConfig  # Updated import


class ProjectIngestor:
    """
    Ingests project metadata into Neo4j by processing JSON files.
    """

    def __init__(self) -> None:
        """
        Initialize the ProjectIngestor by loading configuration, creating the log directory,
        setting up the logger, and initializing the Neo4j driver.
        """
        self.config: AppConfig = load_config()  # Updated type hint and assignment
        self.log_directory: str = self.config.paths.log_directory  # Updated access
        os.makedirs(self.log_directory, exist_ok=True)
        self.logger: logging.Logger = setup_logger(
            __name__, "projects_index.log", level=logging.DEBUG, file_level=logging.INFO
        )
        self.driver = get_driver()

    def set_project_uniqueness_constraint(self) -> None:
        query: str = "CREATE CONSTRAINT FOR (p:Project) REQUIRE p.globalId IS UNIQUE"
        with self.driver.session() as session:
            try:
                session.run(query)
                self.logger.info("Uniqueness constraint on Project.globalId set successfully.")
            except Exception as e:
                self.logger.error(f"Failed to create uniqueness constraint: {e}")

    def generate_uuid_from_shortname(self, shortname: str) -> Optional[str]:
        if shortname and isinstance(shortname, str):
            return str(uuid.uuid5(uuid.NAMESPACE_DNS, shortname))
        self.logger.error(f"Invalid or missing short name: {shortname}")
        return None

    def create_project_node(self, tx: Any, project_data: Dict[str, Any]) -> None:
        query: str = """
        MERGE (p:Project {globalId: $globalId})
        ON CREATE SET p.shortName = $shortName, p.longName = $longName
        """
        tx.run(query, **project_data)

    def process_json_files(self, directory: str, batch_size: int = 100) -> None:
        projects: List[Dict[str, Any]] = []
        json_files: List[str] = list(find_json_files(directory))
        self.logger.info(f"Found {len(json_files)} JSON files in directory: {directory}")

        for json_file in tqdm(json_files, desc="Reading JSON files", unit="file"):
            try:
                with open(json_file, "r") as file:
                    data = json.load(file)
                    if "Projects" in data:
                        for project in data["Projects"]:
                            short_name: Optional[str] = project.get("ShortName")
                            if short_name:
                                project_uuid: Optional[str] = self.generate_uuid_from_shortname(short_name)
                                if project_uuid is not None:
                                    project_data: Dict[str, Any] = {
                                        "globalId": project_uuid,
                                        "shortName": short_name,
                                        "longName": project.get("LongName", ""),
                                    }
                                    projects.append(project_data)
            except Exception as e:
                self.logger.error(f"Failed to process file: {json_file}. Error: {e}")

        self.logger.info(f"Total projects to process: {len(projects)}")
        total_created: int = 0

        with self.driver.session() as session:
            for i in tqdm(range(0, len(projects), batch_size), desc="Creating Project nodes", unit="batch"):
                batch: List[Dict[str, Any]] = projects[i:i + batch_size]
                try:
                    session.execute_write(lambda tx: [self.create_project_node(tx, proj) for proj in batch])
                    total_created += len(batch)
                    self.logger.info(f"Processed batch of {len(batch)} projects. Total created so far: {total_created}")
                except Exception as e:
                    self.logger.error(f"Failed to process batch: {batch}. Error: {e}")

        self.logger.info(f"Total projects created: {total_created}")

    def run(self) -> None:
        """
        Execute the project ingestion process.
        """
        directory: str = self.config.paths.dataset_metadata_directory  # Updated access
        self.set_project_uniqueness_constraint()
        self.process_json_files(directory)


def main() -> None:
    ingestor = ProjectIngestor()
    ingestor.run()
    print("Project node creation process completed.")


if __name__ == "__main__":
    main()
