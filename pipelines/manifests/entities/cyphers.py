from tqdm import tqdm
from ...helpers import Cypher, Indexes
from ...helpers import S3Utils
from ...helpers import get_query_logging, count_query_logging
import pandas as pd

class EntityCyphers(Cypher):
    def __init__(self) -> None:
        super().__init__()


    @count_query_logging
    def create_ents_and_cats(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MERGE (ent:Entity {{name: rows.name }})
            ON CREATE SET
                ent.oaId = apoc.create.uuid(),
                ent.createdDt = timestamp(),
                ent.lastUpdateDt = timestamp(),
                ent.twitter = rows.twitter_handle,
                ent.categories = rows.category_array
            ON MATCH SET
                ent.lastUpdateDt = timestamp(),
                ent.twitter = rows.twitter_handle,
                ent.categories = rows.category_array
            RETURN COUNT(ent)"""
            count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def create_ents_and_ids(self, urls):
        count = 0
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MATCH (ent:Entity {{twitter: rows.twitter_handle}})
            SET ent.website = rows.website
            SET ent.description = rows.description
            SET ent.blog = rows.blog
            SET ent.mirror = rows.mirror 
            SET ent.farcaster = rows.farcaster 
            SET ent.github = rows.github
            SET ent.dune = rows.dune 
            SET ent.telegram = rows.telegram
            SET ent.discord = rows.discord
            SET ent.documentation = rows.documentation
            SET ent.snapshot = rows.snapshot 
            SET ent.tally = rows.tally
            SET ent.forum = rows.forum
            SET ent.instagram = rows.instagram
            RETURN COUNT(ent)
            """
            count += self.query(query)[0].value()
        return count 

    
