from tqdm import tqdm
from ...helpers import Cypher, Indexes
from ...helpers import S3Utils
from ...helpers import get_query_logging, count_query_logging
import pandas as pd

class RssTwitterCyphers(Cypher):
    def __init__(self) -> None:
        super().__init__()


    @count_query_logging
    def set_xml_urls(self, urls):
        """
        Sets `.xmlUrl` for all Twitter accounts retrieved from Feedparser.
        Creates :Twitter:Account node if not present in the graph.
        """
        count = 0
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{urls}' AS rows
            MERGE (twitter:Twitter {{handle: rows.twitterHandle}})
            ON CREATE SET
                twitter.xmlUrl = rows.xmlUrl
            ON MATCH SET
                twitter.xmlUrl = rows.xmlUrl
            RETURN COUNT(twitter)
            """
            count += self.query(query)[0].value()
        return count 
    