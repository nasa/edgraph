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


class PlatformInstrumentRelationshipIngestor:
    """
    Ingests relationships between Platform and Instrument nodes into Neo4j by processing JSON files.
    """

    def __init__(self) -> None:
        """
        Initialize the PlatformInstrumentRelationshipIngestor by loading configuration,
        setting up the log directory, creating a logger, and initializing the Neo4j driver.
        """
        self.config: AppConfig = load_config()  # Updated to use AppConfig
        self.log_directory: str = self.config.paths.log_directory  # Updated access
        os.makedirs(self.log_directory, exist_ok=True)
        self.logger: logging.Logger = setup_logger(
            __name__, "platform_instrument_relations.log", level=logging.DEBUG, file_level=logging.INFO
        )
        self.driver = get_driver()

    def generate_uuid_from_shortname(self, shortname: str) -> Optional[str]:
        if not shortname or not isinstance(shortname, str):
            self.logger.error(f"Invalid or missing short name: {shortname}")
            return None
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, shortname))

    def create_relationship(self, tx: Any, platform_globalId: str, instrument_globalId: str) -> None:
        query: str = """
        MATCH (p:Platform {globalId: $platform_globalId}), (i:Instrument {globalId: $instrument_globalId})
        MERGE (p)-[:HAS_INSTRUMENT]->(i)
        """
        tx.run(query, platform_globalId=platform_globalId, instrument_globalId=instrument_globalId)
        self.logger.info(f"Relationship created between Platform {platform_globalId} and Instrument {instrument_globalId}")

    def process_json_files(self, directory: str, batch_size: int = 100) -> None:
        relationships: List[Tuple[str, str]] = []
        json_files: List[str] = list(find_json_files(directory))
        self.logger.info(f"Found {len(json_files)} JSON files in directory: {directory}")

        for json_file in tqdm(json_files, desc="Processing JSON files", unit="file"):
            try:
                with open(json_file, "r") as file:
                    data = json.load(file)
                    if "Platforms" in data:
                        for platform in data["Platforms"]:
                            platform_short_name: Optional[str] = platform.get("ShortName")
                            if platform_short_name and "Instruments" in platform:
                                platform_globalId: Optional[str] = self.generate_uuid_from_shortname(platform_short_name)
                                if not platform_globalId:
                                    continue
                                for instrument in platform["Instruments"]:
                                    instrument_short_name: Optional[str] = instrument.get("ShortName")
                                    instrument_globalId: Optional[str] = self.generate_uuid_from_shortname(instrument_short_name) if instrument_short_name else None
                                    if platform_globalId and instrument_globalId:
                                        relationships.append((platform_globalId, instrument_globalId))
                                    else:
                                        self.logger.warning(f"Invalid relationship data: Platform={platform_short_name}, Instrument={instrument_short_name}")
            except Exception as e:
                self.logger.error(f"Failed to process file: {json_file}. Error: {e}")

        self.logger.info(f"Total relationships to process: {len(relationships)}")
        total_created: int = 0

        with self.driver.session() as session:
            for i in tqdm(range(0, len(relationships), batch_size), desc="Creating Relationships", unit="batch"):
                batch: List[Tuple[str, str]] = relationships[i:i + batch_size]
                try:
                    session.execute_write(lambda tx: [
                        self.create_relationship(tx, plat_id, instr_id)
                        for plat_id, instr_id in batch
                    ])
                    total_created += len(batch)
                    self.logger.info(f"Processed batch of {len(batch)} relationships. Total created so far: {total_created}")
                except Exception as e:
                    self.logger.error(f"Failed to process batch: {batch}. Error: {e}")

        self.logger.info(f"Total relationships created: {total_created}")

    def run(self) -> None:
        """
        Execute the platform-instrument relationship ingestion process.
        """
        directory: str = self.config.paths.dataset_metadata_directory  # Updated access
        self.process_json_files(directory)


def main() -> None:
    ingestor = PlatformInstrumentRelationshipIngestor()
    ingestor.run()
    ingestor.logger.info("Relationship creation between Platforms and Instruments completed.")
    print("Relationship creation process completed.")


if __name__ == "__main__":
    main()
