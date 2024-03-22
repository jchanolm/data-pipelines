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
            MERGE (grantee:Entity {{name: rows.name}})
            RETURN COUNT(grantee)"""
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_grantees_ecosystem(self, urls):
        count = 0
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (grantee:Entity {{name: rows.name}}) 
            RETURN COUNT(grantee)
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
            RETURN COUNT(twitter)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def connect_twitter_accounts(self, urls):
        count = 0
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MATCH (twitter:Account {{handle: rows.twitterHandle}})
            MATCH (grantee:Entity {{name: rows.name}})
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
            MATCH (entity:Entity {{name: rows.name}})
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
            MATCH (entity:Entity {{name: rows.name}})
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
            MATCH (entity:Entity {{name: rows.name}})
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
            WITH wallet, rows
            MATCH (entity:Entity {{name: rows.name}})
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
            WITH website, rows
            MATCH (entity:Entity {{name: rows.name}})
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
            MATCH (entity:Entity {{name: rows.name}})
            MERGE (entity)-[r:ACCOUNT]->(twitter)
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
            MATCH (entity:Entity {{name: rows.name}})
            MERGE (entity)-[r:ACCOUNT]->(github)
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
            WITH rows WHERE rows.grantInitiative is not Null
            MERGE (grant:Grants:GrantInitiative {{name: rows.grantInitiative}})
            SET grant.token = rows.grantToken
            SET grant.platform = rows.grantPlatform
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
            MATCH (grant:GrantInitiative {{name: rows.grantInitiative}})
            MATCH (grantee:Entity {{name: rows.name}})
            WITH grantee, grant, rows
            CREATE (grantee)<-[r:GRANTEE]-(grant)
            SET r.amount = rows.amount
            SET r.percVoteWon = rows.percVoteWon
            SET r.grantSubmissionUrl = rows.grantSubmissionUrl
            SET r.grantApprovalUrl = rows.grantApprovalSource
            RETURN COUNT(r)
            """
            print(query)
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_orgs(self):
        count = 0
        daos = ['Arbitrum Foundation', 'Uniswap', 'Allo Protocol', 'Gitcoin']
        entities = ['Questbook', 'Tally', 'Buidlbox', 'Giveth', 'Glo Dollar']
        for i in daos:
            query = f"MERGE (dao:Entity:Dao {{name: '{i}'}}) RETURN count(*)"
            count += self.query(query)[0].value()
        for i in entities:
            query = f"MERGE (company:Entity {{name: '{i}'}}) RETURN count(*)"
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def connect_funders_grants(self):
        count = 0
        query = f"""
        MATCH (entity:Entity)
        MATCH (grant:Grants:GrantInitiative)
        WHERE toLower(grant.name) CONTAINS toLower(entity.name)
        MERGE (entity)-[r:FUNDED]->(grant)
        RETURN COUNT(*)
        """
        count += self.query(query)[0].value()
        return count

    @count_query_logging
    def connect_grants_platforms(self):
        count = 0
        query = """
        MATCH (entity:Entity)
        MATCH (entityOther:Entity)
        WHERE entity.grantPlatform = entityOther.name 
        MERGE (entity)-[r:USED_PLATFORM]->(entityOther)
        RETURN COUNT(r)
        """
        count += self.query(query)[0].value()

    @count_query_logging
    def connect_funding_wallet_grant(self):
        count = 0
        query = f"""
        MATCH (grant:Grants:GrantInitiative)-[r1]-(grantee:Entity)-[r2:ACCOUNT]->(wallet:Wallet)
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
        WHERE r.grantApprovalUrl contains prop.url   
        MERGE (prop)-[r1:FUNDED]->(entity)
        MERGE (prop)-[r2:IMPLEMENTED]->(grant)
        RETURN COUNT(DISTINCT(prop))
        """
        count += self.query(query)[0].value()
        return count

