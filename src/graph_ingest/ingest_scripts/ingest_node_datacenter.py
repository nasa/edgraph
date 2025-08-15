import os
import json
import logging
from typing import Any, Dict, List
from tqdm import tqdm

from graph_ingest.common.dbconfig import get_driver
from graph_ingest.common.logger_setup import setup_logger
from graph_ingest.common.core import find_json_files, generate_uuid_from_name
from graph_ingest.common.config_reader import load_config, AppConfig  # Updated import


class DataCenterIngestor:
    """
    Ingests DataCenter metadata into Neo4j by processing JSON files.
    
    Loads configuration, sets up logging and a Neo4j driver,
    and processes JSON files to create DataCenter nodes.
    """
    def __init__(self) -> None:
        """
        Initialize the DataCenterIngestor by loading configuration,
        creating the log directory, setting up the logger, and initializing the Neo4j driver.
        """
        self.config: AppConfig = load_config()  # Updated type hint and assignment
        self.log_directory: str = self.config.paths.log_directory  # Updated access
        os.makedirs(self.log_directory, exist_ok=True)
        self.logger: logging.Logger = setup_logger(
            __name__, "neo4j_datacenter_index_logs.log", level=logging.DEBUG, file_level=logging.WARNING
        )
        self.driver = get_driver()

    def set_uniqueness_constraint(self) -> None:
        constraint_query: str = "CREATE CONSTRAINT FOR (dc:DataCenter) REQUIRE dc.globalId IS UNIQUE"
        with self.driver.session() as session:
            try:
                session.run(constraint_query)
                self.logger.info("Uniqueness constraint on DataCenter.globalId set successfully.")
            except Exception as e:
                self.logger.error(f"Failed to create uniqueness constraint: {e}")

    def add_data_centers_batch(self, tx: Any, data_centers_batch: List[Dict[str, Any]]) -> None:
        query: str = (
            "MERGE (dc:DataCenter {globalId: $globalId}) "
            "ON CREATE SET dc.shortName = $shortName, dc.longName = $longName, dc.url = $url"
        )
        for data_center in data_centers_batch:
            global_id = generate_uuid_from_name(data_center["shortName"])
            tx.run(
                query,
                globalId=global_id,
                shortName=data_center["shortName"],
                longName=data_center["longName"],
                url=data_center["url"],
            )

    def process_files(self, batch_size: int = 100) -> None:
        start_dir: str = self.config.paths.dataset_metadata_directory  # Updated access
        json_files = list(find_json_files(start_dir))
        data_centers_batch: List[Dict[str, Any]] = []

        with self.driver.session() as session:
            for json_file in tqdm(json_files, desc="Processing files", unit="file"):
                with open(json_file, "r") as file:
                    data = json.load(file)
                    if "DataCenters" in data:
                        for center in data["DataCenters"]:
                            data_center: Dict[str, str] = {
                                "shortName": center.get("ShortName", "N/A"),
                                "longName": center.get("LongName", "N/A"),
                                "url": center.get("ContactInformation", {}).get("RelatedUrls", [{}])[0].get("URL", "N/A"),
                            }
                            data_centers_batch.append(data_center)
                            if len(data_centers_batch) >= batch_size:
                                session.execute_write(self.add_data_centers_batch, data_centers_batch)
                                data_centers_batch = []
            if data_centers_batch:
                session.execute_write(self.add_data_centers_batch, data_centers_batch)


def main() -> None:
    ingestor = DataCenterIngestor()
    ingestor.set_uniqueness_constraint()
    ingestor.process_files()
    print("Data Centers have been added to the Neo4j database.")


if __name__ == "__main__":
    main()
