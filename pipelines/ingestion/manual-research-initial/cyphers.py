from ...helpers import Cypher
from ...helpers import Utils
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
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (grantee:Grantee:Entity {{name: rows.name}}) 
            ON CREATE SET
                grantee.createdDt = '{self.CREATED_ID}'
            ON MATCH SET
                grantee.updatedDt = '{self.CREATED_ID}'
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
            MERGE (grantee:Grantee:Entity {{name: rows.name}}) 
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
            LOADS CSV WITH HEADERS FROM f'{url}' as urls as rows
            MERGE (twitter:Account:Twitter {{handle: rows.twitterHandle}})
            ON CREATE SET
                twitter.createdDt = '{self.CREATED_ID}',
                twitter.updatedDt = '{self.CREATED_ID}'
            ON MATCH SET
                twitter.updatedDt = '{self.CREATED_ID}'
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
                wallet.createdDt = '{self.CREATED_ID}',
                wallet.lastUpdateDt = '{self.CREATED_ID}',
            ON MATCH SET
                wallet.lastUpdateDt = '{self.CREATED_ID}' 
            WITH wallet, rows
            MATCH (entity:Entity:Grantee {{name: rows.name}})
            MERGE (entity)-[r:ACCOUNT]->(wallet)
            RETURN COUNT(wallet)"""
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
                website.createdDt = '{self.CREATED_ID}',
                website.lastUpdateDt = '{self.CREATED_ID}'
            ON MATCH SET
                website.createdDt = '{self.CREATED_ID}',
                website.lastUpdateDt = '{self.CREATED_ID}'
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
                r.createdDt = '{self.CREATED_ID}',
                r.updatedDt = '{self.CREATED_ID}',
                r.source = rows.source
            ON MATCH SET
                r.updatedDt = '{self.CREATED_ID}',
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
                r.createdDt = '{self.CREATED_ID}', 
                r.updatedDt = '{self.CREATED_ID}'
            ON MATCH SET
                r.updatedDt = '{self.CREATED_ID}' 
            RETURN COUNT(github)
            """ 
            count += self.query(query)[0].value()
        return count 


    @count_query_logging
    def create_grants(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (grant:GrantInitiative:Grant {{name: rows.name}})
            ON CREATE SET 
                grant.grantPlatform = rows.grantPlatform,
                grant.tokenAddress = rows.tokenAddress,
                grant.fundingAuthorizationVote = rows.fundingAuthorizationVote,
                grant.createdDt = '{self.CREATED_ID}' ,
                grant.updatedDt = '{self.CREATED_ID}'
            ON MATCH SET
                grant.updatedDt = '{self.CREATED_ID}'
            RETURN COUNT(grant)
            """
            count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def connect_grants(self, urls): 
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MATCH (grant:GrantInitiative:Grant {{name: rows.grantInitiative}})
            MATCH (grantee:Entity:Grantee {{name: rows.name}})
            WITH grantee, grant, rows
            MERGE (grantee)-[r:GRANTEE]->(grant)
            ON CREATE SET
                r.amount = rows.amount,
                r.percVoteWon = rows.percVoteWon,
                r.grantSubmissionUrl = rows.grantSubmissionUrl,
                r.grantApprovalSource = rows.grantApprovalSource,
                r.source = rows.grantApprovalSource,
                r.createdDt = '{self.CREATED_ID}',
                r.updatedDt = '{self.CREATED_ID}',
            ON MATCH SET
                r.updatedDt = '{self.CREATED_ID}'
            RETURN COUNT(r)
            """
            count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def create_orgs(self):
        count = 0
        daos =  ['Arbitrum Foundation', 'Uniswap', 'Allo Protocol', 'Gitcoin']
        companies = ['Questbook', 'Tally', 'Buidlbox']
        for i in daos:
            query = f"""merge (dao:Entity:Dao {{name: '{i}'}}) return count(*)"""
            count += self.query(query)[0].value()
        for i in companies:
            query = f"""merge (company:Entity:Company {{name: '{i}}}) return count(*)"""
            count += self.query(query)[0].value()
        return count 
    
    ## need formal process for this
    def conenct_funders_grants(self):
        count = 0 
        query = f"""
        MATCH (entity:Entity)
        MATCH (grant:GrantInitiative)
        WHERE tolower(grant.name contains entity.name
        MERGE (entity)-[r:FUNDED]->(grant)
        ON CREATE SET
            r.source = rows.grantApprovalSource,
            r.createdDt = '{self.CREATED_ID}',
            r.updatedDt = '{self.CREATED_ID}'
        ON MATCH SET
            r.updatedDt = '{self.CREATED_ID}' 
        RETURN COUNT(*)    
        """
        count += self.query(query)[0].value()
        return count 
    
    def connect_grants_platforms(self):
        platforms = ['Gitcoin', 'Buidlbox' , 'Questbook', 'Allo']
        for platform in platforms:
            query = f"""
            MERGE (entity:Entity {{name: '{platform}'}})
            ON CREATE SET
                entity:GrantsPlatform, 
                entity.createdDt = '{self.CREATED_ID}', 
                entity.updatedDt = '{self.CREATED_ID}'
            ON MATCH SET
                entity:GrantsPlatform, 
                entity.updatedDt = '{self.CREATED_ID}' 
            """
            count += self.query(query)[0].value()

            connect_query = """
            MATCH (entity:Entity:GrantsPlatform)
            MATCH (grant:GrantsInitiative)
            WHERE tolower(grant.grantsPlatform) CONTAINS entity.name
            WITH entity, grant
            MERGE (grant)-[r:GRANTS_PLATFORM]->(entity)
            RETURN COUNT(entity)
            """
            count += self.query(connect_query)[0].value()
        return count 
    
    @count_query_logging
    def connect_funding_wallet_grant(self, urls):
        count = 0 
        query = f"""
        MATCH (grant:GrantInitiative:Grant)-[r1]-(grantee:Entity:Grantee)-[r:ACCOUNT]->(wallet:Wallet)
        MERGE (grant)-[r:FUNDED]->(wallet:Wallet)
        SET r1.source = grant.submissionUrl
        SET r1.createdDt = '{self.CREATED_ID}'
        SET r1.lastUpdatedDt = '{self.CREATED_ID}'
        RETURN COUNT(grant)
        """
        count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def connect_approvals_snapshot(self):
        count = 0
        query = """
        MATCH (entity:Entity)-[r:GRANTEE]-(grant:Initiative)
        MATCH (prop:Proposal)
        WHERE r.grantApprovalSource = prop.url 
        MERGE (prop)-[r:FUNDED]->(entity)
        MERGE (prop)-[r1:IMPLEMENTED]->(grant)
        RETURN COUNT(DISTINCT(prop))
        """
        count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def connect_submission_grants(self):
        count = 0 
        query = """
        MATCH (forum:Discourse:Forum:Post)
        MATCH (entity:Entity:Grantee)-[r:GRANTEE]-(grant:GrantInitiative)
        WHERE forum.url = r.grantSubmissionUrl 
        MERGE (entity)-[r:SUBMITTED]->(forum)
        MERGE (forum)-[r1:SUBMISSION]->(grant)
        RETURN COUNT(*)
        """
        count += self.query(query)[0].value()
        return count 
    
    





        
