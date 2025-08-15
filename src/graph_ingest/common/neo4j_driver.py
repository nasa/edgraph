"""Neo4j driver utility functions."""

from neo4j import GraphDatabase
from graph_ingest.common.config_reader import AppConfig

def get_driver(config: AppConfig = None):
    """Get a Neo4j driver instance.
    
    Args:
        config (AppConfig, optional): Application configuration. Defaults to None.
        
    Returns:
        neo4j.Driver: A Neo4j driver instance.
    """
    if config is None:
        from graph_ingest.common.config_reader import load_config
        config = load_config()
        
    return GraphDatabase.driver(
        config.database.uri,
        auth=(config.database.user, config.database.password)
    ) 