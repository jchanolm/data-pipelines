from ...helpers import Cypher
from ...helpers import Constraints
from ...helpers import Indexes
from ...helpers import count_query_logging

import tqdm
import time 

class TwitterEnsCyphers(Cypher):
    def __init__(self):
        super().__init__()

    # def create_constraints(self):
    #     constraints = Constraints()
    #     constraints.wallets()
    #     constraints.aliases()
    #     constraints.ens()
    #     constraints.twitter()

    # def create_indexes(self):
    #     indexes = Indexes()
    #     indexes.wallets()
    #     indexes.aliases()
    #     indexes.ens()
    #     indexes.twitter()

    @count_query_logging
    def create_or_merge_twitter(self, urls):
        count = 0
        for url in tqdm.tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS twitter
                    WITH twitter
                    WHERE twitter.handle IS NOT NULL
                    WITH twitter
                    MERGE (t:Twitter:Account {{handle: toLower(twitter.handle)}})
                    SET t.uuid = apoc.create.uuid()
                    RETURN COUNT(t)    
            """
            print(query)
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_twitter_alias(self, urls):
        count = 0
        for url in tqdm.tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS alias
                    WITH alias
                    WHERE alias.ens IS NOT NULL
                    MERGE (a:Alias:Ens {{name: toLower(alias.ens)}})
                    RETURN COUNT(a)
                    """
            print(query)
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_twitter_wallets(self, urls):
        count = 0
        for url in tqdm.tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                    WITH wallets
                    WHERE wallets.address IS NOT NULL
                    MERGE(wallet:Wallet:Account {{address: toLower(wallets.address)}})
                    RETURN COUNT(wallet)
            """
            count += self.query(query)[0].value()
            time.sleep(1)
            print(query)
        return count

    @count_query_logging
    def link_twitter_alias(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS rows
                    WITH rows 
                    WHERE rows.handle IS NOT NULL
                    WITH rows
                    MATCH (a:Alias:Ens {{name: toLower(rows.ens)}})
                    MATCH (t:Twitter:Account {{handle: toLower(rows.handle)}})
                    MERGE (t)-[r:ALIAS]->(a)
                    return count(r)
            """
            print(query)
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_wallet_alias(self, urls):
        count = 0
        for url in tqdm.tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS alias
                    WITH alias
                    WHERE alias.ens IS NOT NULL
                    MATCH (a:Alias {{name: toLower(alias.ens)}}), 
                        (w:Wallet {{address: toLower(alias.address)}})
                    MERGE (w)-[r:ALIAS]->(a)
                    return count(r)
                    """
            print(query)
            count += self.query(query)[0].value()
        return count
