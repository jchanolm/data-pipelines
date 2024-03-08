from ...helpers import Cypher
from ...helpers import Utils
import datetime  
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging

class ManualCyphers(Cypher):

    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()
        self.asOf = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M")

    @count_query_logging
    def create_grantees_direct(self, urls):
        count = 0
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (grantee:Grantee:Entity {{name: rows.name}})
            ON CREATE SET
                grantee.createdDt = '{self.asOf}'
            ON MATCH SET
                grantee.updatedDt = '{self.asOf}'
            RETURN 
                COUNT(grantee)"""
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
               grantee.createdDt = '{self.asOf}'
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
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (twitter:Account:Twitter {{handle: rows.twitterHandle}})
            ON CREATE SET
                twitter.createdDt = '{self.asOf}',
                twitter.updatedDt = '{self.asOf}'
            ON MATCH SET
                twitter.updatedDt = '{self.asOf}'
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
            MATCH (grantee:Grantee {{name: rows.name}})
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
                wallet.lastUpdateDt = '{self.asOf}'
            ON MATCH SET
                wallet.lastUpdateDt = '{self.asOf}' 
            WITH wallet, rows
            MATCH (entity:Entity:Grantee {{name: rows.name}})
            MERGE (entity)-[r:ACCOUNT]->(wallet)
            RETURN COUNT(wallet)
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
                r.updatedDt = '{self.asOf}',
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
                r.updatedDt = '{self.asOf}' 
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
            WITH rows WHERE rows.grantInitiative IS NOT NULL
            MERGE (grant:GrantInitiative:Grant {{name: rows.grantInitiative}})
            ON CREATE SET 
                grant.grantPlatform = rows.grantPlatform,
                grant.tokenAddress = rows.tokenAddress,
                grant.fundingAuthorizationVote = rows.fundingAuthorizationVote,
                grant.createdDt = '{self.asOf}',
                grant.updatedDt = '{self.asOf}'
            ON MATCH SET
                grant.updatedDt = '{self.asOf}'
            RETURN COUNT(grant)
            """
            print(query)
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
            MERGE (grantee)<-[r:GRANTEE]-(grant)
            ON CREATE SET
                r.amount = rows.amount,
                r.percVoteWon = rows.percVoteWon,
                r.grantSubmissionUrl = rows.grantSubmissionUrl,
                r.grantApprovalSource = rows.grantApprovalSource,
                r.source = rows.grantApprovalSource,
                r.createdDt = '{self.asOf}',
                r.updatedDt = '{self.asOf}'
            ON MATCH SET
                r.updatedDt = '{self.asOf}'
            RETURN COUNT(r)
            """
            print(query)
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_orgs(self):
        count = 0
        daos = ['Arbitrum Foundation', 'Uniswap', 'Allo Protocol', 'Gitcoin']
        companies = ['Questbook', 'Tally', 'Buidlbox']
        for i in daos:
            query = f"MERGE (dao:Entity:Dao {{name: '{i}'}}) RETURN count(*)"
            count += self.query(query)[0].value()
        for i in companies:
            query = f"MERGE (company:Entity:Company {{name: '{i}'}}) RETURN count(*)"
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def connect_funders_grants(self):
        count = 0
        query = f"""
        MATCH (entity:Entity)
        MATCH (grant:GrantInitiative)
        WHERE toLower(grant.name) CONTAINS toLower(entity.name)
        MERGE (entity)-[r:FUNDED]->(grant)
        ON CREATE SET
            r.source = grant.grantApprovalSource,
            r.createdDt = '{self.asOf}',
            r.updatedDt = '{self.asOf}'
        ON MATCH SET
            r.updatedDt = '{self.asOf}' 
        RETURN COUNT(*)
        """
        count += self.query(query)[0].value()
        return count

    # @count_query_logging
    # def connect_grants_platforms(self):
    #     count = 0
    #     platforms = ['Gitcoin', 'Buidlbox', 'Questbook', 'Allo']
    #     for platform in platforms:
    #         connect_query = f"""
    #         MATCH (entity:Entity {{name: '{platform}'}})
    #         MATCH (grant:GrantsInitiative)
    #         WHERE toLower(grant.grantsPlatform) CONTAINS toLower(entity.name)
    #         WITH entity, grant
    #         MERGE (grant)-[r:GRANTS_PLATFORM]->(entity)
    #         RETURN COUNT(entity)
    #         """
    #         count += self.query(connect_query)[0].value()
    #     return count

    @count_query_logging
    def connect_funding_wallet_grant(self):
        count = 0
        query = f"""
        MATCH (grant:GrantInitiative)-[r1]-(grantee:Entity)-[r2:ACCOUNT]->(wallet:Wallet)
        MERGE (grant)-[r3:FUNDED]->(wallet)
        ON CREATE SET r3.source = r1.grantSubmissionUrl
        RETURN COUNT(DISTINCT(wallet))"""
        count += self.query(query)[0].value()
        return count 

    @count_query_logging
    def connect_approvals_snapshot(self):
        count = 0
        query = """
        MATCH (entity:Entity)-[r:GRANTEE]-(grant:GrantInitiative)
        MATCH (prop:Proposal)
        WHERE r.grantApprovalSource contains prop.url   
        MERGE (prop)-[r1:FUNDED]->(entity)
        MERGE (prop)-[r2:IMPLEMENTED]->(grant)
        RETURN COUNT(DISTINCT(prop))
        """
        count += self.query(query)[0].value()
        return count

    # @count_query_logging
    # def connect_submission_grants(self):
    #     count = 0
    #     query = """
    #     MATCH (grant:GrantInitiative)-[r:GRANTEE]-(entity:Entity)
    #     MATCH (post:Post)
    #     WHERE post.url CONTAINS last(split(toString(r.grantSubmissionUrl), '/'))   
    #     MERGE (entity)-[:SUBMITTED]->(post)
    #     MERGE (post)-[:SUBMISSION]->(grant)
    #     RETURN COUNT(post)
    #     """
    #     count += self.query(query)[0].value()
    #     return count
