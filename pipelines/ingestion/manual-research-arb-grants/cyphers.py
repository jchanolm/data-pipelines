from ...helpers import Cypher
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging

class ManualCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()



    @count_query_logging
    def create_grantees_direct(self, urls):
        count = 0 
        for url in urls:
            ### probably should do wallet/twitter as UUID later
            query = """
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (grantee:Grantee:Entity:Arbitrum {{name: rows.name}}}) 
            ON CREATE SET
                grantee.name = rows.name
            ON MATCH SET
                grantee:Arbitrum
            RETURN 
                COUNT(grantee)
            """
            count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def create_grantees_ecosystem(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (grantee:Grantee:Entity:Arbitrum {{name: rows.name}}}) 
            ON CREATE SET
                grantee.name = rows.name
            ON MATCH SET
                grantee:Arbitrum
            RETURN 
                COUNT(grantee)
            """
            count += self.query(query)[0].value()
        return count 
        
    @count_query_logging
    def create_twitter_accounts(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as twitters
            MERGE (twitter:Twitter:Account {{handle: rows.twitterHandle}})
            ON CREATE SET 
                twitter.name = rows.name
            ON MATCH SET
                twitter.name = rows.name
            RETURN 
                COUNT(twitter)                        
             """
            count += self.query(query)[0].value()
        return count 

    @count_query_logging
    def connect_twitter_accounts(self, urls):
        count = 0
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MATCH (twitter:Twitter:Account {{handle: rows.twitterHandle}})
            MATCH (grantee:Grantee:Entity {{name: rows.name}})
            MERGE (grantee)-[r:ACCOUNT]->(twitter)
            RETURN COUNT(DISTINCT(twitter))
            """
            count += self.query(query)[0].value()
        return count 
        
    @count_query_logging
    def create_websites(self, urls):
        count = 0 
        for url in urls: 
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MERGE (website:Website:Account {{url: rows.websiteUrl}})
            WITH website, rows
            MATCH (entity:Entity:Grantee {{name: rows.name}})
            MERGE (entity)-[r:ACCOUNT]->(website)
            RETURN COUNT(website)
            """
            count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def create_githubs(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MERGE (github:Github:Account {{handle: rows.githubHandle}})
            WITH github, rows
            MATCH (entity:Entity:Grantee {{name: rows.name}})
            MERGE (entity)-[r:ACCOUNT]->(github)
            RETURN COUNT(github)
            """
            count += self.query(query)[0].value()
        return count 
    
    @count_query_logging    
    def create_dunes(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (dune:Dune:Account {{handle: rows.duneHandle}})
            WITH dune, rows
            MATCH (entity:Entity:Grantee {{name: rows.name}})
            MERGE (entity)-[r:ACCOUNT]->(dune)
            RETURN COUNT(dune)
            """
            count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def create_wallets(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (wallet:Account:Wallet {{address: tolower(rows.fundingWalletAddress)}})
            ON CREATE SET
                wallet.createdDt = '{self.asOf}',
                wallet.lastUpdateDt = '{self.asOf}',
            ON MATCH SET
                wallet.lastUpdateDt = '{self.asOf}' 
            WITH wallet, rows
            MATCH (entity:Entity:Grantee {{name: rows.name}})
            MERGE (entity)-[r:ACCOUNT]->(wallet)
            RETURN COUNT(wallet)"""
            count += self.query(query)[0].value()
        return count 
    
    @count_query_logging    
    def create_grantees_ecosystem(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MERGE (grantee:Entity:Grantee:Arbitrum {{name: rows.name}})
            ON CREATE SET 
                grantee.createdDt = '{self.asOf}',
                grantee.lastUpdateDt = '{self.asOf}'
            ON MATCH SET
                grantee:Arbitrum,
                grantee.createdDt = '{self.asOf}',
                grantee.lastUpdateDt = '{self.asOf}'
            RETURN 
                COUNT(grantee) 
            """
            count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def create_grantees_ecosystem_websites(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (website:Account:Website {{name: rows.websiteUrl}})
            ON CREATE SET
                website.createdDt = '{self.asOf}',
                website.lastUpdateDt = '{self.asOf}'
            ON MATCH SET
                website.createdDt = '{self.asOf}',
                website.lastUpdateDt = '{self.asOf}'
            WITH website, rows
            MATCH (entity:Entity:Grantee {{name: rows.name}})
            MERGE (entity)-[r:ACCOUNT]->(website)
            RETURN COUNT(website)
            """
            count += self.query(query)[0].value()
        return count 

    @count_query_logging
    def create_grantees_ecosystem_twitter(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (twitter:Account:Twitter {{url: rows.twitterHandle}})
            WITH twitter, rows 
            MATCH (entity:Entity:Grantee {{name: rows.name}})
            MERGE (entity)-[r:ACCOUNT]->(twitter)
            ON CREATE SET
                r.createdDt = '{self.asOf}',
                r.updatedDt = '{self.asOf}',
                r.source = rows.source
            ON MATCH SET
                r.createdDt = '{self.asOf}',
                r.lastUpdateDt = '{self.asOf}',
                r.source = rows.source
            RETURN COUNT(twitter)
            """
            count += self.query(query)[0].value()
    

    @count_query_logging
    def create_grantees_ecosystem_github(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (github:Account:Github {{url: rows.githubHandle}})
            WITH github, rows
            MATCH (entity:Entity:Grantee {{name: rows.name}})
            MERGE (entity)-[r:ACCOUNT]->(github)
            ON CREATE SET
                r.createdDt = '{self.asOf}', 
                r.updatedDt = '{self.asOf}'
            ON MATCH SET
                r.createdDt = '{self.asOf}',
                r.updatedDt = '{self.asOf}' 
            RETURN COUNT(github)
            """ 
            count += self.query(query)[0].value()
        return count 

@@@ GRANTS

    @count_query_logging
    def connect_funding_wallet_grant(self, urls):
        count = 0 
        query = """
        MATCH (grant:GrantInitiative:Grant)-[r1]-(grantee:Entity:Grantee)-[r:ACCOUNT]->(wallet:Wallet)
        MERGE (grant)-[r:FUNDED]->(wallet:Wallet)
        SET r1.source = grant.submissionUrl
        SET r1.createdDt = '{self.asOf}'
        SET r1.lastUpdatedDt = '{self.asOf}'
        RETURN COUNT(grant)
        """
        count += self.query(query)[0].value()
        return count 

        
        !!!asOf!!!
        !!!createdDt!!!
        !!!updatedDt!!!