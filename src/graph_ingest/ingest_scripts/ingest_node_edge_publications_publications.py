import os
import json
import uuid
import logging
from typing import Any, Dict, List, Optional, Tuple
from tqdm import tqdm
from multiprocessing import cpu_count, Pool
from functools import partial

from neo4j import Driver, GraphDatabase
from graph_ingest.common.config_reader import load_config, AppConfig
from graph_ingest.common.logger_setup import setup_logger
from graph_ingest.common.dbconfig import get_driver


class PublicationsOfPublicationsIngestor:
    """
    Ingests publication-of-publications data into Neo4j by processing a JSON file with multiprocessing and batching.
    """

    def __init__(self, neo4j_driver: Optional[Driver] = None) -> None:
        """
        Initialize the PublicationsOfPublicationsIngestor by loading configuration,
        setting up the log directory and logger, and capturing database connection parameters.
        """
        self.config: AppConfig = load_config()
        self.log_directory: str = self.config.paths.log_directory
        os.makedirs(self.log_directory, exist_ok=True)

        self.logger: logging.Logger = setup_logger(
            __name__, "publications_of_publications.log", level=logging.DEBUG, file_level=logging.INFO
        )

        # Use provided driver or get a new one (just like PublicationIngestor)
        self.driver = neo4j_driver if neo4j_driver is not None else get_driver()

        # Get Neo4j connection parameters from environment variables
        # These will be passed to worker processes
        self.neo4j_uri = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
        self.neo4j_user = os.environ.get("NEO4J_USER", "neo4j")
        self.neo4j_password = os.environ.get("NEO4J_PASSWORD", "password")

    def generate_uuid_from_doi(self, doi: str) -> Optional[str]:
        if not doi or not isinstance(doi, str):
            self.logger.error(f"Invalid DOI for UUID generation: {doi}")
            return None
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, doi))

    @staticmethod
    def publication_exists(tx: Any, globalId: str) -> bool:
        query: str = "MATCH (pub:Publication {globalId: $globalId}) RETURN pub"
        result = tx.run(query, globalId=globalId)
        return result.single() is not None

    @staticmethod
    def create_publication_node(tx: Any, publication: Dict[str, Any]) -> None:
        publication_data = {
            "globalId": publication["globalId"],
            "doi": publication["doi"],
            "title": publication.get("title", ""),
            "year": publication.get("year", ""),
            "abstract": publication.get("abstract", ""),
            "authors": ", ".join(publication.get("authors", [])) if isinstance(publication.get("authors"), list) else publication.get("authors", "")
        }
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
        try:
            tx.run(query, **publication_data)
        except Exception as e:
            # Just log the error but continue processing
            print(f"Error creating node: {e}")

    @staticmethod
    def create_cites_relationship(tx: Any, citing_globalId: str, cited_globalId: str) -> None:

        """
        Create a CITES relationship between two Publication nodes in the Neo4j database.

        Args:
            tx: An active Neo4j transaction object.
            citing_globalId: The globalId of the citing Publication node (the publication that cites another).
            cited_globalId: The globalId of the cited Publication node (the publication being cited).

        Returns:
            None. The function executes a Cypher query to merge the CITES relationship in the database.
        """
        
        query: str = """
        MATCH (citing:Publication {globalId: $citing_globalId})
        MATCH (cited:Publication {globalId: $cited_globalId})
        MERGE (cited)-[:CITES]->(citing)
        """
        tx.run(query, citing_globalId=citing_globalId, cited_globalId=cited_globalId)

    def chunk_data(self, data: Dict[str, List[Dict[str, Any]]], chunk_size: int) -> List[List[Tuple[str, List[Dict[str, Any]]]]]:
        """
        Split the data into chunks for parallel processing.

        Args:
        data: A dictionary where each key is a citing DOI (str), and each value is a list of cited publication metadata dictionaries.
        chunk_size: The maximum number of (citing DOI, publications) pairs to include in each chunk.

        Returns:
            A list of chunks, where each chunk is a list of (citing DOI, publications) tuples. Each chunk will have at most `chunk_size` items.
        """
        
        items = list(data.items())
        chunks = []
        for i in range(0, len(items), chunk_size):
            chunks.append(items[i:i + chunk_size])
        return chunks

    def process_nodes_batch(self, batch: List[Tuple[str, List[Dict[str, Any]]]], 
                            neo4j_uri: str, neo4j_user: str, neo4j_password: str) -> Dict[str, int]:
        """
        Process a batch of publications to create nodes (for worker processes).
        
        Args:
            batch: List of (citing_doi, publications) tuples to process
            neo4j_uri: Neo4j URI
            neo4j_user: Neo4j username
            neo4j_password: Neo4j password
            
        Returns:
            Statistics dictionary
        """
        local_stats = {"publication_nodes_created": 0, "existing_publication_nodes": 0}
        
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        
        try:
            with driver.session() as session:
                for citing_doi, publications in batch:
                    # Create the citing publication node if it doesn't exist
                    citing_globalId = str(uuid.uuid5(uuid.NAMESPACE_DNS, citing_doi))
                    if not session.execute_read(self.publication_exists, citing_globalId):
                        # We don't have metadata for the citing publication
                        # Just create with DOI and globalId
                        session.execute_write(
                            self.create_publication_node, 
                            {"doi": citing_doi, "globalId": citing_globalId}
                        )
                        local_stats["publication_nodes_created"] += 1
                    
                    # For each cited publication in this entry
                    for publication in publications:
                        cited_doi = publication.get("doi")
                        if cited_doi:
                            cited_globalId = str(uuid.uuid5(uuid.NAMESPACE_DNS, cited_doi))
                            if not session.execute_read(self.publication_exists, cited_globalId):
                                # Create the cited publication with available metadata
                                publication_data = {
                                    "doi": cited_doi,
                                    "title": publication.get("title", ""),
                                    "year": publication.get("year", ""),
                                    "abstract": publication.get("abstract", ""),
                                    "authors": publication.get("authors", []),
                                    "globalId": cited_globalId
                                }
                                session.execute_write(
                                    self.create_publication_node, publication_data
                                )
                                local_stats["publication_nodes_created"] += 1
                            else:
                                local_stats["existing_publication_nodes"] += 1
        finally:
            driver.close()
        
        return local_stats

    def process_relationships_batch(self, batch: List[Tuple[str, List[Dict[str, Any]]]], 
                                   neo4j_uri: str, neo4j_user: str, neo4j_password: str) -> Dict[str, int]:
        """
        Process a batch of publications to create relationships (for worker processes).
        
        Args:
            batch: List of (citing_doi, publications) tuples to process
            neo4j_uri: Neo4j URI
            neo4j_user: Neo4j username
            neo4j_password: Neo4j password
            
        Returns:
            Statistics dictionary
        """
        local_stats = {"cites_relationships_created": 0, "cited_publications_not_found": 0}
        
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        
        try:
            with driver.session() as session:
                for citing_doi, publications in batch:
                    citing_globalId = str(uuid.uuid5(uuid.NAMESPACE_DNS, citing_doi))
                    
                    for publication in publications:
                        cited_doi = publication.get("doi")
                        if cited_doi:
                            cited_globalId = str(uuid.uuid5(uuid.NAMESPACE_DNS, cited_doi))
                            
                            # Verify both publications exist
                            cited_exists = session.execute_read(
                                self.publication_exists, cited_globalId
                            )
                            citing_exists = session.execute_read(
                                self.publication_exists, citing_globalId
                            )
                            
                            if cited_exists and citing_exists:
                                session.execute_write(
                                    self.create_cites_relationship,
                                    citing_globalId, cited_globalId
                                )
                                local_stats["cites_relationships_created"] += 1
                            else:
                                local_stats["cited_publications_not_found"] += 1
        finally:
            driver.close()
        
        return local_stats

    def process_publications_parallel(self, max_workers: Optional[int] = None, batch_size: int = 100) -> None:
        """
        Process publications in parallel using multiprocessing.
        
        Args:
            max_workers: Maximum number of worker processes (defaults to CPU count)
            batch_size: Size of each batch to process
        """
        # Calculate number of workers
        num_workers = max_workers if max_workers is not None else max(1, cpu_count() // 2)
        
        # Load all the data
        input_file_path: str = self.config.paths.pubs_of_pubs
        with open(input_file_path, "r") as f:
            data: Dict[str, List[Dict[str, Any]]] = json.load(f)
        
        total_items = len(data)
        self.logger.info(f"Total citing publications to process: {total_items}")
        print(f"Total citing publications to process: {total_items}")
        
        # Prepare chunks for processing
        chunks = self.chunk_data(data, batch_size)
        total_chunks = len(chunks)
        
        # Prepare statistics counters
        stats = {
            "publication_nodes_created": 0,
            "existing_publication_nodes": 0,
            "cites_relationships_created": 0,
            "cited_publications_not_found": 0
        }
        
        # Process nodes in parallel
        print(f"Starting parallel publication node creation with {num_workers} workers")
        self.logger.info(f"Starting node creation across {total_chunks} batches using {num_workers} processes")
        
        # Create a partial function with Neo4j connection parameters
        process_nodes_func = partial(
            self.process_nodes_batch,
            neo4j_uri=self.neo4j_uri,
            neo4j_user=self.neo4j_user, 
            neo4j_password=self.neo4j_password
        )
        
        try:
            # Use Pool for multiprocessing
            with Pool(processes=num_workers) as pool:
                results = []
                for i, result in enumerate(tqdm(
                    pool.imap_unordered(process_nodes_func, chunks),
                    total=total_chunks,
                    desc="Creating Publication Nodes"
                )):
                    stats["publication_nodes_created"] += result["publication_nodes_created"]
                    stats["existing_publication_nodes"] += result["existing_publication_nodes"]
                    # Log progress every few batches
                    if (i + 1) % 5 == 0 or i == total_chunks - 1:
                        self.logger.info(f"Node creation progress: {i+1}/{total_chunks} batches, {stats['publication_nodes_created']} nodes created")
            
            # Process relationships in parallel
            print("Publication nodes creation complete. Starting CITES relationship creation.")
            self.logger.info("Node creation complete. Starting relationship creation.")
            
            # Create a partial function with Neo4j connection parameters
            process_rels_func = partial(
                self.process_relationships_batch,
                neo4j_uri=self.neo4j_uri,
                neo4j_user=self.neo4j_user, 
                neo4j_password=self.neo4j_password
            )
            
            # Use Pool for multiprocessing
            with Pool(processes=num_workers) as pool:
                results = []
                for i, result in enumerate(tqdm(
                    pool.imap_unordered(process_rels_func, chunks),
                    total=total_chunks,
                    desc="Creating CITES Relationships"
                )):
                    stats["cites_relationships_created"] += result["cites_relationships_created"]
                    stats["cited_publications_not_found"] += result["cited_publications_not_found"]
                    # Log progress every few batches
                    if (i + 1) % 5 == 0 or i == total_chunks - 1:
                        self.logger.info(f"Relationship creation progress: {i+1}/{total_chunks} batches, {stats['cites_relationships_created']} relationships created")
        
        except Exception as e:
            self.logger.error(f"Error in parallel processing: {e}")
            print(f"Error in parallel processing: {e}")
            # Fall back to single-process approach
            self.logger.info("Falling back to single process mode")
            print("Falling back to single process mode")
            
            # Reset stats
            stats = {
                "publication_nodes_created": 0,
                "existing_publication_nodes": 0,
                "cites_relationships_created": 0,
                "cited_publications_not_found": 0
            }
            
            # Process in batches but without multiprocessing
            for i, chunk in enumerate(chunks):
                print(f"Processing batch {i+1}/{total_chunks}")
                # Process nodes first
                node_result = self.process_nodes_batch(
                    chunk, self.neo4j_uri, self.neo4j_user, self.neo4j_password
                )
                stats["publication_nodes_created"] += node_result["publication_nodes_created"]
                stats["existing_publication_nodes"] += node_result["existing_publication_nodes"]
                
                # Then relationships
                rel_result = self.process_relationships_batch(
                    chunk, self.neo4j_uri, self.neo4j_user, self.neo4j_password
                )
                stats["cites_relationships_created"] += rel_result["cites_relationships_created"]
                stats["cited_publications_not_found"] += rel_result["cited_publications_not_found"]
        
        # Log final statistics
        print(f"Final stats: {stats}")
        self.logger.info(f"Total publications created: {stats['publication_nodes_created']}")
        self.logger.info(f"Total existing publications: {stats['existing_publication_nodes']}")
        self.logger.info(f"Total CITES relationships created: {stats['cites_relationships_created']}")
        self.logger.info(f"Cited publications not found: {stats['cited_publications_not_found']}")

    def run(self, max_workers: Optional[int] = None, batch_size: int = 100) -> None:
        """
        Run the ingestor with multiprocessing.
        
        Args:
            max_workers: Maximum number of worker processes (defaults to CPU count)
            batch_size: Size of each batch processed by a single worker
        """
        self.process_publications_parallel(max_workers, batch_size)


def main() -> None:
    # By default, use a reasonable number of workers and batch size
    # In Docker, you might want to limit workers and use smaller batches
    workers = 4  # Fixed number that should work well in most Docker environments
    batch_size = 50  # Smaller batches for better stability
    
    ingestor = PublicationsOfPublicationsIngestor()
    ingestor.run(max_workers=workers, batch_size=batch_size)
    print("Publication-of-Publications relationship creation completed.")


if __name__ == "__main__":
    main()