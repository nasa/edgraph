import os
import csv
import uuid
import logging
import pandas as pd
from typing import List, Tuple, Optional
from tqdm import tqdm

from graph_ingest.common.config_reader import load_config, AppConfig
from graph_ingest.common.logger_setup import setup_logger
from graph_ingest.common.dbconfig import get_driver


class GCMDScienceKeywordIngestor:
    """
    Ingests GCMD Science Keywords into Neo4j by processing a CSV file.
    """

    def __init__(self) -> None:
        """
        Initializes the ingestor by loading configuration, setting up logging,
        and establishing database connection parameters.
        """
        self.config: AppConfig = load_config()
        self.log_directory = self.config.paths.log_directory
        os.makedirs(self.log_directory, exist_ok=True)

        self.logger = setup_logger(
            __name__, "neo4j_science_keywords.log", level=logging.DEBUG, file_level=logging.INFO
        )

        self.driver = get_driver()

    def generate_global_id_from_string(self, input_string: str) -> Optional[str]:
        if not input_string or not isinstance(input_string, str):
            self.logger.error(f"Invalid string for globalId generation: {input_string}")
            return None
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, input_string))

    def set_sciencekeyword_uniqueness_constraint(self) -> None:
        query = "CREATE CONSTRAINT FOR (sk:ScienceKeyword) REQUIRE sk.globalId IS UNIQUE"
        with self.driver.session() as session:
            try:
                session.run(query)
                self.logger.info("Uniqueness constraint on ScienceKeyword.globalId set successfully.")
            except Exception as e:
                self.logger.error(f"Failed to create uniqueness constraint: {str(e)}")

    def create_node(self, tx, name: str, global_id: str) -> None:
        query = "MERGE (n:ScienceKeyword {globalId: $globalId}) ON CREATE SET n.name = $name"
        tx.run(query, name=name, globalId=global_id)

    def create_relationship(self, tx, global_id_from: str, global_id_to: str) -> None:
        query = """
        MATCH (a:ScienceKeyword {globalId: $global_id_from}), (b:ScienceKeyword {globalId: $global_id_to})
        MERGE (a)-[:HAS_SUBCATEGORY]->(b)
        """
        tx.run(query, global_id_from=global_id_from, global_id_to=global_id_to)

    def process_csv(self, file_path: str, batch_size: int = 1000) -> None:
        try:
            operations: List[Tuple[str, str, str]] = []
            with open(file_path, "r", newline="", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in tqdm(reader, desc="Processing CSV Rows", unit="rows"):
                    prev_global_id = None
                    for label in ["Topic", "Term", "Variable_Level_1", "Variable_Level_2", "Variable_Level_3", "Detailed_Variable"]:
                        value = row.get(label, "").strip()
                        if value:
                            current_global_id = self.generate_global_id_from_string(value)
                            if current_global_id:
                                operations.append(("node", value, current_global_id))
                                if prev_global_id:
                                    operations.append(("rel", prev_global_id, current_global_id))
                                prev_global_id = current_global_id
                            else:
                                self.logger.warning(f"Failed to generate globalId for: {value}")

                    if len(operations) >= batch_size:
                        self.execute_batch(operations)
                        operations.clear()

                if operations:
                    self.execute_batch(operations)

            self.logger.info("Processing of GCMD Science Keywords CSV completed.")
        except Exception as e:
            self.logger.error(f"Failed to process CSV file: {file_path}. Error: {e}")

    def execute_batch(self, operations: List[Tuple[str, str, str]]) -> None:
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                for op in tqdm(operations, desc="Executing Batch", unit="ops", leave=False):
                    if op[0] == "node":
                        self.create_node(tx, op[1], op[2])
                    elif op[0] == "rel":
                        self.create_relationship(tx, op[1], op[2])
                tx.commit()

    def run(self) -> None:
        """
        Runs the full ingestion process.
        """
        self.set_sciencekeyword_uniqueness_constraint()
        file_path = self.config.paths.gcmd_sciencekeyword_directory
        self.process_csv(file_path, batch_size=1000)
        self.logger.info("Science keyword node creation process completed.")


def main() -> None:
    ingestor = GCMDScienceKeywordIngestor()
    ingestor.run()


if __name__ == "__main__":
    main()
