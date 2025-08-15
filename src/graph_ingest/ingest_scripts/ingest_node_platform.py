import os
import json
import uuid
import logging
from typing import Any, Dict, List, Optional
from tqdm import tqdm

from graph_ingest.common.dbconfig import get_driver
from graph_ingest.common.logger_setup import setup_logger
from graph_ingest.common.core import find_json_files
from graph_ingest.common.config_reader import load_config, AppConfig


class PlatformIngestor:
    """
    Ingests Platform metadata into Neo4j by processing JSON files.
    """

    def __init__(self) -> None:
        """
        Initialize the PlatformIngestor by loading configuration, setting up the log directory,
        creating a logger, and initializing the Neo4j driver.
        """
        self.config: AppConfig = load_config()
        self.log_directory: str = self.config.paths.log_directory
        os.makedirs(self.log_directory, exist_ok=True)
        self.logger: logging.Logger = setup_logger(
            __name__, "platform_creation.log", level=logging.DEBUG, file_level=logging.INFO
        )
        self.driver = get_driver()

    def set_platform_uniqueness_constraint(self) -> None:
        query: str = "CREATE CONSTRAINT FOR (p:Platform) REQUIRE p.globalId IS UNIQUE"
        with self.driver.session() as session:
            try:
                session.run(query)
                self.logger.info("Uniqueness constraint on Platform.globalId set successfully.")
            except Exception as e:
                self.logger.error(f"Failed to create uniqueness constraint: {e}")

    def generate_uuid_from_shortname(self, shortname: str) -> Optional[str]:
        if not shortname or not isinstance(shortname, str):
            self.logger.error(f"Invalid or missing short name: {shortname}")
            return None
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, shortname))

    def create_platform_node(self, tx: Any, platform_data: Dict[str, Any]) -> None:
        query: str = """
        MERGE (p:Platform {globalId: $globalId})
        ON CREATE SET p.Type = $Type, p.shortName = $shortName, p.longName = $longName
        """
        tx.run(query, **platform_data)

    def process_json_files(self, directory: str, batch_size: int = 100) -> None:
        platforms: List[Dict[str, Any]] = []
        json_files: List[str] = list(find_json_files(directory))
        self.logger.info(f"Found {len(json_files)} JSON files in directory: {directory}")

        for json_file in tqdm(json_files, desc="Processing JSON files", unit="file"):
            try:
                with open(json_file, "r") as file:
                    data = json.load(file)
                    if "Platforms" in data:
                        for platform in data["Platforms"]:
                            short_name: Optional[str] = platform.get("ShortName")
                            if short_name:
                                platform_uuid: Optional[str] = self.generate_uuid_from_shortname(short_name)
                                if platform_uuid:
                                    platform_data: Dict[str, Any] = {
                                        "globalId": platform_uuid,
                                        "Type": platform.get("Type", ""),
                                        "shortName": short_name,
                                        "longName": platform.get("LongName", ""),
                                    }
                                    platforms.append(platform_data)
                                else:
                                    self.logger.warning(f"Failed to generate UUID for platform: {short_name}")
            except Exception as e:
                self.logger.error(f"Failed to process file: {json_file}. Error: {e}")

        self.logger.info(f"Total platforms to process: {len(platforms)}")
        total_created: int = 0

        with self.driver.session() as session:
            for i in tqdm(range(0, len(platforms), batch_size), desc="Creating Platforms", unit="batch"):
                batch: List[Dict[str, Any]] = platforms[i:i + batch_size]
                try:
                    session.execute_write(lambda tx: [self.create_platform_node(tx, p) for p in batch])
                    total_created += len(batch)
                    self.logger.info(f"Processed batch of {len(batch)} platforms. Total created so far: {total_created}")
                except Exception as e:
                    self.logger.error(f"Failed to process batch: {batch}. Error: {e}")

        self.logger.info(f"Total platforms created: {total_created}")

    def run(self) -> None:
        """
        Execute the platform ingestion process.
        """
        directory: str = self.config.paths.dataset_metadata_directory 
        self.set_platform_uniqueness_constraint()
        self.process_json_files(directory)
        self.logger.info("Platform node creation process completed.")


def main() -> None:
    ingestor = PlatformIngestor()
    ingestor.run()
    print("Platform node creation process completed.")


if __name__ == "__main__":
    main()
