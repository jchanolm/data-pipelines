from datetime import datetime
import time
from neo4j import BoltDriver, GraphDatabase, Neo4jDriver
import os
import logging
from neo4j.data import Record


class Cypher:
    def __init__(self, database=None) -> None:
        self.database = database
        if "NEO_DB" in os.environ:
            self.database = os.environ["NEO_DB"]
        self.unique_id = datetime.timestamp(datetime.now())
        self.CREATED_ID = f"created:{self.unique_id}"
        self.UPDATED_ID = f"updated:{self.unique_id}"

        self.create_constraints()
        self.create_indexes()

    def get_driver(self, 
                   uri: str, 
                   username: str, 
                   password: str) -> Neo4jDriver | BoltDriver | None:
        neo4j_driver = GraphDatabase.driver(uri, auth=(username, password))
        return neo4j_driver

    def get_drivers(self) -> list[Neo4jDriver]:
        uris = [uri.strip() for uri in os.environ["NEO4J_URI"].split(',')]
        usernames = [uri.strip() for uri in os.environ["NEO4J_USER"].split(',')]
        passwords = [uri.strip() for uri in os.environ["NEO4J_PASS"].split(',')]
        assert len(uris) == len(usernames) == len(passwords), "The variables NEO_URI, NEO_PASSWORD and NEO_USERNAME must have the same length"
        neo4j_drivers = []
        for uri, username, password in zip(uris, usernames, passwords):
            neo4j_driver = self.get_driver(uri, username, password)
            neo4j_drivers.append(neo4j_driver)
        return neo4j_drivers

    def create_constraints(self):
        logging.warning("This function should be implemented in the children class.")

    def create_indexes(self):
        logging.warning("This function should be implemented in the children class.")

    def run_query(self, 
                  neo4j_driver: Neo4jDriver, 
                  query: str, 
                  parameters: dict|None = None, 
                  counter: int = 0):
        """Run a query using the passed driver. Injects the parameter dict to the query."""
        time.sleep(counter * 10)
        assert neo4j_driver is not None, "Driver not initialized!"
        
        session = None
        response = None
        try:
            session = neo4j_driver.session(database=self.database) if self.database is not None else neo4j_driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            logging.error(f"An error occured for neo4j instance {neo4j_driver}")
            logging.error(f"Query failed: {e}")
            if counter > 10:
                raise e
            return self.run_query(neo4j_driver, query, parameters=parameters, counter=counter+1)
        finally:
            if session is not None:
                session.close()
        neo4j_driver.close()
        return response

    def query(self, 
              query: str, 
              parameters: dict|None = None, 
              last_response_only: bool = True) -> list[Record]:
        """
        Wrapper function that will query all instances of neo4J set in the NEO_URI env var.
        Returns the result from the RETURN statement.
        """
        neo4j_drivers = self.get_drivers()
        responses = []
        for neo4j_driver in neo4j_drivers:
            response = self.run_query(neo4j_driver, query, parameters)
            responses.append(response)
        if last_response_only:
            return responses[-1]
        return responses

    def sanitize_text(self, text: str|None) -> str:
        """
        Helper function to sanitize text before injecting it into a Neo4J query. 
        Useful for ingesting body of text from any source.
        """
        if text:
            return text.rstrip().replace('\r', '').replace('\\', '').replace('"', '').replace("'", "").replace("`", "").replace("\n", "")
        else:
            return ""
