import json
import os
import uuid
import logging
from typing import Any, Dict, Optional, List
from tqdm import tqdm

from neo4j import Driver
from graph_ingest.common.config_reader import load_config, AppConfig
from graph_ingest.common.dbconfig import get_driver
from graph_ingest.common.logger_setup import setup_logger


class PublicationIngestor:
    """
    Ingests Publication nodes into Neo4j by processing a JSON file containing publication records.
    """

    def __init__(self, neo4j_driver: Optional[Driver] = None) -> None:
        """
        Initialize the PublicationIngestor by loading configuration, setting up the log directory,
        configuring the logger, and initializing the Neo4j driver.

        Args:
            neo4j_driver (Optional[Driver]): Optional Neo4j driver instance. If not provided, a new one will be created.
        """
        self.config: AppConfig = load_config()
        self.log_directory: str = self.config.paths.log_directory
        os.makedirs(self.log_directory, exist_ok=True)
        self.logger: logging.Logger = setup_logger(
            __name__, "publications.log", level=logging.DEBUG, file_level=logging.INFO
        )
        self.driver = neo4j_driver if neo4j_driver is not None else get_driver()

    def set_publication_uniqueness_constraint(self) -> None:
        """
        Sets a uniqueness constraint on the 'globalId' property of Publication nodes in Neo4j.
        """
        query: str = "CREATE CONSTRAINT IF NOT EXISTS FOR (pub:Publication) REQUIRE pub.globalId IS UNIQUE"
        with self.driver.session() as session:
            try:
                session.run(query)
                self.logger.info("Uniqueness constraint on Publication.globalId set successfully.")
            except Exception as e:
                self.logger.error(f"Failed to create uniqueness constraint: {e}")

    def generate_uuid_from_doi(self, doi: str) -> Optional[str]:
        """
        Generates a UUID from the given DOI string.

        Args:
            doi (str): The DOI string from which to generate the UUID.

        Returns:
            Optional[str]: The generated UUID as a string, or None if DOI is invalid.
        """
        if not doi or not isinstance(doi, str):
            self.logger.error(f"Invalid DOI for UUID generation: {doi}")
            return None
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, doi))

    def create_publication_node(self, tx: Any, publication_data: Dict[str, Any]) -> None:
        """
        Creates or updates a Publication node in Neo4j with the given publication data.

        Args:
            tx (Any): The Neo4j transaction object.
            publication_data (Dict[str, Any]): The publication data to insert or update.
        """
        query: str = """
        MERGE (pub:Publication {globalId: $globalId})
        ON CREATE SET 
            pub.doi = $doi,
            pub.title = $title,
            pub.year = $year,
            pub.abstract = $abstract,
            pub.authors = $authors
        ON MATCH SET
            pub.doi = coalesce(pub.doi, $doi),
            pub.title = coalesce(pub.title, $title),
            pub.year = coalesce(pub.year, $year),
            pub.abstract = coalesce(pub.abstract, $abstract),
            pub.authors = coalesce(pub.authors, $authors)
        """
        tx.run(query, **publication_data)

    def process_publications(self, batch_size: int = 100) -> None:
        """
        Processes publications from the metadata JSON file and ingests them into Neo4j in batches.

        Args:
            batch_size (int): The number of publications to process per batch.
        """
        file_path: str = self.config.paths.publications_metadata_directory
        try:
            with open(file_path, "r") as file:
                publications_data: List[Dict[str, Any]] = json.load(file)
        except Exception as e:
            self.logger.error(f"Failed to load JSON file: {file_path}. Error: {e}")
            return

        total_publications = len(publications_data)
        self.logger.info(f"Total publications to process: {total_publications}")
        created: int = 0
        
        # Process in batches
        for i in range(0, total_publications, batch_size):
            batch = publications_data[i:i+batch_size]
            batch_number = i//batch_size + 1
            total_batches = (total_publications+batch_size-1)//batch_size
            self.logger.info(f"Processing batch {batch_number}/{total_batches}, size: {len(batch)}")
            
            with self.driver.session() as session:
                for publication in tqdm(batch, desc=f"Batch {batch_number}/{total_batches}", unit="publication"):
                    doi: Optional[str] = publication.get("DOI")
                    if not doi:
                        self.logger.warning(f"Skipping publication with missing DOI: {publication}")
                        continue
                    global_id: Optional[str] = self.generate_uuid_from_doi(doi)
                    if global_id is None:
                        continue
                    publication_data: Dict[str, Any] = {
                        "globalId": global_id,
                        "doi": doi,
                        "title": publication.get("Title", ""),
                        "year": publication.get("Year", ""),
                        "abstract": "" if publication.get("Abstract", "").strip() in ["", "No abstract available"]
                                else publication.get("Abstract", ""),
                        "authors": ", ".join(publication.get("Authors", [])),
                    }
                    try:
                        session.execute_write(self.create_publication_node, publication_data)
                        created += 1
                    except Exception as e:
                        self.logger.error(f"Failed to insert publication: {publication_data}. Error: {e}")
            
            # Log progress after each batch
            self.logger.info(f"Progress: {min(i+batch_size, total_publications)}/{total_publications} publications processed ({created} created)")

        self.logger.info(f"Total publications created: {created}")

    def run(self) -> None:
        """
        Executes the publication ingestion process, including setting constraints and processing publications.
        """
        self.set_publication_uniqueness_constraint()
        self.process_publications()


def main() -> None:
    """
    Entry point for running the PublicationIngestor.
    """
    ingestor = PublicationIngestor()
    ingestor.run()
    print("Publication node creation process completed.")


if __name__ == "__main__":
    main()
