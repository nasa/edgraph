import logging
from typing import Any
from neo4j import GraphDatabase

from graph_ingest.common.config_reader import load_config, AppConfig  # Updated import
from graph_ingest.common.logger_setup import setup_logger
from graph_ingest.common.dbconfig import get_driver


class PageRankProcessor:
    """
    Runs PageRank computation in Neo4j, handling graph projection, execution, and cleanup.
    """

    def __init__(self) -> None:
        """
        Initialize the PageRankProcessor by loading configuration, setting up logging,
        and initializing the Neo4j database connection.
        """
        # Load configuration
        self.config: AppConfig = load_config()  # Updated type hint and assignment

        # Setup logging
        self.logger: logging.Logger = setup_logger(
            __name__, "pagerank_publication_dataset.log", level=logging.DEBUG, file_level=logging.INFO
        )

        # Initialize Neo4j driver
        self.driver = get_driver()

    def _drop_existing_projection(self, tx: Any) -> None:
        query = "CALL gds.graph.exists('publicationDatasetGraph') YIELD exists RETURN exists"
        result = tx.run(query).single()
        if result and result["exists"]:
            drop_query = "CALL gds.graph.drop('publicationDatasetGraph') YIELD graphName"
            tx.run(drop_query)
            self.logger.info("Existing graph projection 'publicationDatasetGraph' dropped.")
        else:
            self.logger.info("No existing graph projection found to drop.")

    def _create_graph_projection(self, tx: Any) -> None:
        query = """
        CALL gds.graph.project(
            'publicationDatasetGraph',
            ['Publication', 'Dataset'],   // Only include 'Publication' and 'Dataset' nodes
            '*'                           // Include all relationships
        )
        YIELD graphName, nodeCount, relationshipCount
        """
        result = tx.run(query).single()
        if result:
            self.logger.info(
                f"Graph projection created: {result['graphName']} "
                f"with {result['nodeCount']} nodes and {result['relationshipCount']} relationships."
            )
        else:
            self.logger.error("Failed to create graph projection.")

    def _run_pagerank(self, tx: Any) -> None:
        query = """
        CALL gds.pageRank.write('publicationDatasetGraph', {
            writeProperty: 'pagerank_publication_dataset'  // Use a more descriptive property name
        })
        YIELD nodePropertiesWritten
        """
        result = tx.run(query).single()
        if result:
            self.logger.info(f"PageRank scores written to {result['nodePropertiesWritten']} nodes.")
        else:
            self.logger.error("Failed to run PageRank or write properties.")

    def _drop_graph_projection(self, tx: Any) -> None:
        query = "CALL gds.graph.drop('publicationDatasetGraph') YIELD graphName"
        result = tx.run(query).single()
        if result:
            self.logger.info(f"Graph projection '{result['graphName']}' dropped.")
        else:
            self.logger.error("Failed to drop the graph projection.")

    def _get_pagerank_stats(self, tx: Any) -> None:
        query = """
        MATCH (n)
        WHERE n.pagerank_publication_dataset IS NOT NULL
        RETURN count(n) AS pagerank_nodes
        """
        result = tx.run(query).single()
        if result:
            count = result["pagerank_nodes"]
            self.logger.info(f"Total nodes with pagerank_publication_dataset property: {count}")
            print(f"Total nodes with pagerank_publication_dataset property: {count}")
        else:
            self.logger.error("Failed to retrieve PageRank stats.")
            print("Failed to retrieve PageRank stats.")

    def run_pagerank(self) -> None:
        with self.driver.session() as session:
            self.logger.info("Dropping existing graph projection (if any)...")
            session.execute_write(self._drop_existing_projection)

            self.logger.info("Creating graph projection for Publications and Datasets...")
            session.execute_write(self._create_graph_projection)

            self.logger.info("Running PageRank on Publications and Datasets...")
            session.execute_write(self._run_pagerank)

            self.logger.info("Fetching PageRank stats...")
            session.execute_read(self._get_pagerank_stats)

            self.logger.info("Dropping graph projection after PageRank computation...")
            session.execute_write(self._drop_graph_projection)

    def close(self) -> None:
        self.driver.close()
        self.logger.info("Neo4j connection closed.")


def main() -> None:
    processor = PageRankProcessor()
    processor.run_pagerank()
    processor.close()
    print("PageRank computation completed.")


if __name__ == "__main__":
    main()
