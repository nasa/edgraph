import logging
from typing import Any
from neo4j import GraphDatabase

from graph_ingest.common.config_reader import load_config, AppConfig  # Updated import
from graph_ingest.common.logger_setup import setup_logger
from graph_ingest.common.dbconfig import get_driver


class FastRPProcessor:
    """
    Runs Fast Random Projection (FastRP) embeddings in Neo4j, handling graph projection,
    embedding computation, and cleanup.
    """

    def __init__(self) -> None:
        """
        Initialize the FastRPProcessor by loading configuration, setting up logging,
        and initializing the Neo4j database connection.
        """
        # Load configuration
        self.config: AppConfig = load_config()  # Updated type hint and assignment

        # Setup logging
        self.logger: logging.Logger = setup_logger(
            __name__, "fastrp_embeddings.log", level=logging.DEBUG, file_level=logging.INFO
        )

        # Initialize Neo4j driver
        self.driver = get_driver()

    def _drop_existing_projection(self, tx: Any) -> None:
        query = "CALL gds.graph.exists('graphEmbedding') YIELD exists RETURN exists"
        result = tx.run(query).single()
        if result and result["exists"]:
            drop_query = "CALL gds.graph.drop('graphEmbedding') YIELD graphName"
            tx.run(drop_query)
            self.logger.info("Existing graph projection 'graphEmbedding' dropped.")
        else:
            self.logger.info("No existing graph projection found to drop.")

    def _create_graph_projection(self, tx: Any) -> None:
        query = """
        CALL gds.graph.project(
            'graphEmbedding',
            '*',   // Include all node labels
            '*'    // Include all relationship types
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

    def _run_fastrp(self, tx: Any) -> None:
        query = """
        CALL gds.fastRP.write('graphEmbedding', {
            embeddingDimension: 512,
            iterationWeights: [0.8, 1.0, 1.0, 1.0],
            nodeSelfInfluence: 1.0,
            writeProperty: 'fastrp_embedding_with_labels'
        })
        YIELD nodePropertiesWritten
        """
        result = tx.run(query).single()
        if result:
            self.logger.info(f"FastRP embeddings written to {result['nodePropertiesWritten']} nodes.")
        else:
            self.logger.error("Failed to run FastRP or write embeddings.")

    def _drop_graph_projection(self, tx: Any) -> None:
        query = "CALL gds.graph.drop('graphEmbedding') YIELD graphName"
        result = tx.run(query).single()
        if result:
            self.logger.info(f"Graph projection '{result['graphName']}' dropped.")
        else:
            self.logger.error("Failed to drop the graph projection.")

    def _get_embedding_stats(self, tx: Any) -> None:
        query = """
        MATCH (n)
        WHERE n.fastrp_embedding_with_labels IS NOT NULL
        RETURN count(n) AS embedding_nodes
        """
        result = tx.run(query).single()
        if result:
            count = result["embedding_nodes"]
            self.logger.info(f"Total nodes with FastRP embeddings: {count}")
            print(f"Total nodes with FastRP embeddings: {count}")
        else:
            self.logger.error("Failed to retrieve FastRP embedding stats.")
            print("Failed to retrieve FastRP embedding stats.")

    def run_fastrp_embeddings(self) -> None:
        with self.driver.session() as session:
            self.logger.info("Dropping existing graph projection (if any)...")
            session.execute_write(self._drop_existing_projection)

            self.logger.info("Creating graph projection...")
            session.execute_write(self._create_graph_projection)

            self.logger.info("Running FastRP embeddings...")
            session.execute_write(self._run_fastrp)

            self.logger.info("Fetching FastRP embedding stats...")
            session.execute_read(self._get_embedding_stats)

            self.logger.info("Dropping graph projection after use...")
            session.execute_write(self._drop_graph_projection)

    def close(self) -> None:
        self.driver.close()
        self.logger.info("Neo4j connection closed.")


def main() -> None:
    processor = FastRPProcessor()
    processor.run_fastrp_embeddings()
    processor.close()
    print("FastRP embedding generation completed.")


if __name__ == "__main__":
    main()
