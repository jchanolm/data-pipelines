from tqdm import tqdm
from ...helpers import Cypher, Indexes
from ...helpers import S3Utils
from ...helpers import get_query_logging, count_query_logging
import pandas as pd

class RssTwitterCyphers(Cypher):
    def __init__(self) -> None:
        super().__init__()


    @count_query_logging
    def set_feed_metadata(self, urls):
        count = 0
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            WITH rows, split(rows.source_url, '/')[2] as twitterHandle
            MERGE (twitter:Twitter {{handle: twitterHandle}})
            MERGE (entity:Entity {{twitter: twitterHandle}})
            ON CREATE SET
                twitter.rssUrl = rows.rssUrl,
                twitter.profileImageUrl = rows.icon
            ON MATCH SET
                twitter.xmlUrl = rows.xmlUrl,
                twitter.rssUrl = rows.icon
            WITH entity, twitter
            MERGE (entity)-[r:ACCOUNT]->(twitter)
            RETURN COUNT(twitter)
            """
            print(query)
            count += self.query(query)[0].value()
        return count 
    