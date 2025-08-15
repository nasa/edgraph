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


class InstrumentIngestor:
    """
    Ingests Instrument metadata into Neo4j by processing JSON files.
    """

    def __init__(self) -> None:
        """
        Initialize the InstrumentIngestor by loading configuration, creating the log directory,
        setting up the logger, and initializing the Neo4j driver.
        """
        self.config: AppConfig = load_config()
        self.log_directory: str = self.config.paths.log_directory
        os.makedirs(self.log_directory, exist_ok=True)
        self.logger: logging.Logger = setup_logger(
            __name__, "instruments.log", level=logging.DEBUG, file_level=logging.INFO
        )
        self.driver = get_driver()

    def set_instrument_uniqueness_constraint(self) -> None:
        query: str = "CREATE CONSTRAINT FOR (i:Instrument) REQUIRE i.globalId IS UNIQUE"
        with self.driver.session() as session:
            try:
                session.run(query)
                self.logger.info("Uniqueness constraint on Instrument.globalId set successfully.")
            except Exception as e:
                self.logger.error(f"Failed to create uniqueness constraint: {e}")

    def generate_uuid_from_shortname(self, shortname: str) -> Optional[str]:
        if not shortname or not isinstance(shortname, str):
            self.logger.error(f"Invalid or missing short name: {shortname}")
            return None
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, shortname))

    def create_instrument_node(self, tx: Any, instrument_data: Dict[str, Any]) -> None:
        query: str = """
        MERGE (i:Instrument {globalId: $globalId})
        ON CREATE SET i.shortName = $shortName, i.longName = $longName
        """
        tx.run(query, **instrument_data)

    def process_json_files(self, directory: str, batch_size: int = 100) -> None:
        instruments: List[Dict[str, Any]] = []
        json_files: List[str] = list(find_json_files(directory))
        self.logger.info(f"Found {len(json_files)} JSON files in directory: {directory}")

        for json_file in tqdm(json_files, desc="Processing JSON files", unit="file"):
            try:
                with open(json_file, "r") as file:
                    data = json.load(file)
                    if "Platforms" in data:
                        for platform in data["Platforms"]:
                            if "Instruments" in platform:
                                for instrument in platform["Instruments"]:
                                    short_name: Optional[str] = instrument.get("ShortName")
                                    if short_name:
                                        instrument_uuid: Optional[str] = self.generate_uuid_from_shortname(short_name)
                                        if instrument_uuid:
                                            instrument_data: Dict[str, Any] = {
                                                "globalId": instrument_uuid,
                                                "shortName": short_name,
                                                "longName": instrument.get("LongName", ""),
                                            }
                                            instruments.append(instrument_data)
                                        else:
                                            self.logger.warning(f"Failed to generate UUID for instrument: {short_name}")
            except Exception as e:
                self.logger.error(f"Failed to process file: {json_file}. Error: {e}")

        self.logger.info(f"Total instruments to process: {len(instruments)}")
        total_created: int = 0

        with self.driver.session() as session:
            for i in tqdm(range(0, len(instruments), batch_size), desc="Creating Instruments", unit="batch"):
                batch: List[Dict[str, Any]] = instruments[i:i + batch_size]
                try:
                    session.execute_write(lambda tx: [self.create_instrument_node(tx, instr) for instr in batch])
                    total_created += len(batch)
                    self.logger.info(f"Processed batch of {len(batch)} instruments. Total created so far: {total_created}")
                except Exception as e:
                    self.logger.error(f"Failed to process batch: {batch}. Error: {e}")

        self.logger.info(f"Total instruments created: {total_created}")

    def run(self) -> None:
        """
        Execute the instrument ingestion process.
        """
        directory: str = self.config.paths.dataset_metadata_directory
        self.set_instrument_uniqueness_constraint()
        self.process_json_files(directory)
        self.logger.info("Instrument node creation process completed.")


def main() -> None:
    ingestor = InstrumentIngestor()
    ingestor.run()
    print("Instrument node creation process completed.")


if __name__ == "__main__":
    main()
