import os
import json
from typing import Any, Dict, List, Optional, Tuple
from tqdm import tqdm
import logging

from graph_ingest.common.dbconfig import get_driver
from graph_ingest.common.logger_setup import setup_logger
from graph_ingest.common.core import generate_uuid_from_doi, find_json_files
from graph_ingest.common.config_reader import load_config, AppConfig

class DatasetIngestor:
    """
    Ingests dataset metadata into Neo4j by processing JSON files.
    
    This class loads configuration, sets up logging and a Neo4j driver,
    and then processes JSON files to add Dataset nodes with their properties.
    """
    def __init__(self) -> None:
        """
        Initialize the DatasetIngestor by loading configuration, creating the log directory,
        setting up the logger, and initializing the Neo4j driver.
        """
        self.config: AppConfig = load_config()
        self.log_directory: str = self.config.paths.log_directory
        os.makedirs(self.log_directory, exist_ok=True)
        self.logger: logging.Logger = setup_logger(
            __name__, "neo4j_dataset_index_logs.log", level=logging.DEBUG, file_level=logging.WARNING
        )
        self.driver = get_driver()

    def set_uniqueness_constraint(self) -> None:
        constraints_query: List[str] = [
            "CREATE CONSTRAINT dataset_globalid IF NOT EXISTS FOR (d:Dataset) REQUIRE d.globalId IS UNIQUE"
        ]
        with self.driver.session() as session:
            for query in constraints_query:
                try:
                    session.run(query)
                    self.logger.info(f"Constraint successfully created: {query}")
                except Exception as e:
                    self.logger.error(f"Failed to create constraint: {query}. Error: {e}")

    def extract_daac(self, data: Dict[str, Any]) -> str:
        if "DataCenters" in data:
            for center in data["DataCenters"]:
                if "Roles" in center and "ARCHIVER" in center["Roles"]:
                    return center.get("ShortName", "N/A")
        return "N/A"

    def extract_temporal_extent(self, data: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        temporal_extents = data.get("TemporalExtents", [])
        if temporal_extents:
            range_datetimes = temporal_extents[0].get("RangeDateTimes", [])
            if range_datetimes:
                beginning = range_datetimes[0].get("BeginningDateTime", "")
                ending = range_datetimes[0].get("EndingDateTime", "")
                return beginning, ending
        return None, None

    def extract_frequency(self, data: Dict[str, Any]) -> str:
        return data.get("Frequency", "Unknown")

    def add_datasets(self, tx: Any, datasets: List[Dict[str, Any]]) -> None:
        query = (
            "MERGE (d:Dataset { globalId: $globalId }) "
            "SET d.doi = $doi, d.shortName = $shortName, d.longName = $longName, "
            "d.daac = $daac, d.abstract = $abstract, d.cmrId = $cmrId, "
            "d.temporalExtentStart = $temporalExtentStart, d.temporalExtentEnd = $temporalExtentEnd, "
            "d.temporalFrequency = $temporalFrequency "
        )
        for dataset in datasets:
            tx.run(query, **dataset)

    def process_files(self, batch_size: int = 100) -> Tuple[int, int]:
        self.logger.info("Starting dataset node creation process...")
        start_dir: str = self.config.paths.dataset_metadata_directory
        json_files = list(find_json_files(start_dir))
        created_count = 0
        skipped_count = 0

        with self.driver.session() as session:
            batch: List[Dict[str, Any]] = []
            for json_file in tqdm(json_files, desc="Processing files", unit="file"):
                with open(json_file, "r") as file:
                    data = json.load(file)
                    doi = data.get("DOI", {}).get("DOI", "")
                    cmr_id = data.get("CMR_ID", "")
                    if doi and cmr_id:
                        dataset_id = generate_uuid_from_doi(doi)
                        short_name = data.get("ShortName", "N/A")
                        long_name = data.get("EntryTitle", "N/A")
                        daac = self.extract_daac(data)
                        abstract = data.get("Abstract", "N/A").replace("\n", "")
                        temporal_start, temporal_end = self.extract_temporal_extent(data)
                        frequency = self.extract_frequency(data)
                        batch.append({
                            "globalId": dataset_id,
                            "doi": doi,
                            "shortName": short_name,
                            "longName": long_name,
                            "daac": daac,
                            "abstract": abstract,
                            "cmrId": cmr_id,
                            "temporalExtentStart": temporal_start,
                            "temporalExtentEnd": temporal_end,
                            "temporalFrequency": frequency,
                        })
                        if len(batch) >= batch_size:
                            session.execute_write(self.add_datasets, batch)
                            created_count += len(batch)
                            batch = []
                    else:
                        skipped_count += 1
            if batch:
                session.execute_write(self.add_datasets, batch)
                created_count += len(batch)

        self.logger.info(f"Dataset node creation process completed. Created: {created_count}, Skipped: {skipped_count}")
        return created_count, skipped_count


def main() -> None:
    ingestor = DatasetIngestor()
    ingestor.set_uniqueness_constraint()
    created, skipped = ingestor.process_files()
    print(f"Datasets created: {created}")
    print(f"Datasets skipped: {skipped}")


if __name__ == "__main__":
    main()
