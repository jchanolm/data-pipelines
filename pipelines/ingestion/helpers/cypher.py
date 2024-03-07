import os 
import logging 
import time 
import sys 
import dotenv 
import pandas as pd 

from datetime import datetime 
import tqdm
from neo4j import BoltDriver, GraphDatabase, Neo4jDriver, Record 
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI = os.environ['NEO4J_URI']
NEO4J_PASS = os.environ['NEO4J_PASS']
NEO4J_USER = 'neo4j' ## change if needed

class Cypher: 
    def __init__(self, database=None) -> None: 
        self.database = database
        if "NEO_DB" in os.environ: 
            self.database = os.environ['NEO_DB']
        self.unique_id = datetime.timestamp(datetime.now())
        self.CREATED_ID = f"created:{self.unique_id}"
        self.UPDATED_ID = f"updated:{self.unique_id}"

        # self.create_constraints()
        # self.create_indexes()


    def get_driver(self,
                   uri: NEO4J_URI, 
                   username: NEO4J_USER,
                   password: NEO4J_PASS) -> Neo4jDriver | BoltDriver | None: 
        neo4j_driver = GraphDatabase.driver(uri, auth=(username, password))
        return neo4j_driver
    
    # def create_constraints(seelf):
    #     logging.warning("This function shyould be implemented in sub class")
        
    # def create_indexes(self): 
    #     logging.warning("This function should be implemented in the sub class")

    def run_query(
            self,
            neo4j_driver: Neo4jDriver, 
            query: str, 
            parameters: dict|None = None, 
            response_format: str = 'df',
            counter: int = 0):
        """Run query with driver, pass parameters into query. Allows response in 'df' or 'json' format with 'df' as default."""
        time.sleep(counter * 10)
        assert neo4j_driver is not None, "Driver not initialized!"

        session = None 
        response = None 
        try: 
            session = neo4j_driver.session(database=self.database) if self.database is not None else neo4j_driver.session()
            result = session.run(query, parameters)
            if response_format == 'json':
                response = [record.data() for record in result]
            else:  # default to 'df'
                response = pd.DataFrame([record.values() for record in result], columns=result.keys())
        except Exception as e:
            logging.error(f"Query failed: {e}")
            if counter > 10:
                raise e 
            return self.run_query(neo4j_driver, query, parameters=parameters, response_format=response_format, counter=counter+1)
        finally:
            if session is not None:
                session.close()
            neo4j_driver.close()
            return response
        
    from tqdm import tqdm

    def query(self,
              query: str,
              parameters: dict|None,
              response_format: str = 'df') -> pd.DataFrame | list:
        """
        Queries neo4j, returns result in specified format ('df' or 'json'), and displays the number of records returned.
        """
        neo4j_driver = self.get_driver()
        response = self.run_query(neo4j_driver, query, parameters, response_format=response_format)
        
        # Display the number of records returned with tqdm
        if response_format == 'json':
            record_count = len(response)
        else:  # 'df'
            record_count = response.shape[0]
        
        for _ in tqdm(range(record_count), desc="Processing records"):
            pass

        return response
            