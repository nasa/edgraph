import os
from neo4j import GraphDatabase
from neo4j import Driver

def get_driver() -> Driver:
    """
    Initialize and return a Neo4j driver using environment variables.
    """
    uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "test")
    return GraphDatabase.driver(uri, auth=(user, password))
