from ...helpers import Cypher
from ...helpers import Utils
import datetime  
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging
import datetime 

class TallyCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()
        self.asOf = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M")

    @count_query_logging
    def create_proposals(self, urls):
        count = 0 
        for url in urls: 
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MERGE (proposal:Proposal:Tally {{id: rows.id}})
            ON CREATE SET 
                proposal.title = rows.title,
                proposal.description = rows.description,
                proposal.eta = rows.eta,
                proposal.createdDt = '{self.asOf}',
                proposal.updatedDt = '{self.asOf}'
            ON MATCH SET
                proposal.updatedDt = '{self.asOf}'
            RETURN COUNT (*)
            """
            count += self.query(query)[0].value()
        return count 
    
    
    @count_query_logging
    def create_connect_wallets_ents_identifiers(self, urls):
        count = 0 
        # for url in urls: 
        #     create_ents = f"""
        #     LOAD CSV WITH HEADERS FROM '{url}' as rows
        #     WITH rows WHERE rows.name IS NOT NULL
        #     MERGE (ent:Entity {{name: rows.name}})
        #     ON CREATE SET
        #         ent.createdDt = '{self.asOf}', 
        #         ent.updatedDt = '{self.asOf}'
        #     ON MATCH SET
        #         ent.updatedDt = '{self.asOf}'
        #     RETURN COUNT(ent)"""
        #     count += self.query(create_ents)[0].value()
        

        # for url in urls:
        #     query = f"""
        #     LOAD CSV WITH HEADERS FROM '{url}' as rows
        #     MERGE (wallet:Wallet {{address: rows.address}})
        #     ON CREATE SET
        #         wallet.createdDt = '{self.asOf}',
        #         wallet.updatedDt = '{self.asOf}'
        #     ON MATCH SET
        #         wallet.updatedDt = '{self.asOf}' 
        #     RETURN COUNT(wallet) as wallets_created"""
        #     count += self.query(query)[0].value()   

        for url in urls:
            query =f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MATCH (wallet:Wallet {{address: rows.address }})
            MATCH (ent:Entity {{name: rows.name}})
            WITH ent, wallet
            MERGE (ent)-[r:WALLET]->(wallet)
            ON CREATE SET
                r.createdDt = '{self.asOf}',
                r.updatedDt = '{self.asOf}'
            ON MATCH SET
                r.updatedDt = '{self.asOf}'
            RETURN COUNT(r)
            """
            count += self.query(query)[0].value()
    
    ## twitter
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            WITH rows WHERE (rows.twitter IS NOT NULL AND rows.name IS NOT NULL) 
            MERGE (twitter:Account:Twitter {{handle: rows.twitter}})
            ON CREATE SET 
                twitter.createdDt ='{self.asOf}',
                twitter.updatedDt = '{self.asOf}
            ON MATCH SET
                twitter.updatedDt = '{self.asOf}'
            WITH twitter
            MATCH (ent:Entity {{name: rows.name}})
            MERGE (ent-[r:ACCOUNT]->(twitter)
            RETURN COUNT(DISTINCT(twitter)) 
            """
            count += self.query(query)[0].value()

        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            WITH rows WHERE (rows.twitter IS NOT NULL AND rows.name IS NULL) 
            MERGE (twitter:Account:Twitter {{handle: rows.twitter}})
            ON CREATE SET 
                twitter.createdDt ='{self.asOf}',
                twitter.updatedDt = '{self.asOf}
            ON MATCH SET
                twitter.updatedDt = '{self.asOf}'
            WITH twitter
            MATCH (wallet:Wallet {{address: rows.address}})
            MERGE (wallet-[r:ACCOUNT]->(twitter)
            RETURN COUNT(DISTINCT(twitter)) 
            """
            count += self.query(query)[0].value()

        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            WITH rows WHERE (rows.ens IS NOT NULL AND rows.name IS NOT NULL) 
            MERGE (alias:Alia:Ens {{name: rows.ens}})
            ON CREATE SET 
                alias.createdDt ='{self.asOf}',
                alias.updatedDt = '{self.asOf}
            ON MATCH SET
                alias.updatedDt = '{self.asOf}'
            WITH alias
            MATCH (ent:Entity {{name: rows.name}})
            MERGE (wallet-[r:ALIAS]->(alias)
            RETURN COUNT(DISTINCT(alias)) 
            """
            count += self.query(query)[0].value()

        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            WITH rows WHERE (rows.ens IS NOT NULL AND rows.name IS NULL) 
            MERGE (alias:Alias:Ens {{name: rows.ens}})
            ON CREATE SET 
                alias.createdDt ='{self.asOf}',
                alias.updatedDt = '{self.asOf}
            ON MATCH SET
                alias.updatedDt = '{self.asOf}'
            WITH alias
            MATCH (wallet:Wallet {{address: rows.address}})
            MERGE (wallet-[r:ALIAS]->(alias)
            RETURN COUNT(DISTINCT(alias)) 
            """
            count += self.query(query)[0].value()
        return count 

    @count_query_logging
    def connect_votes(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MATCH (wallet:Wallet {{address: rows.voter}})
            MATCH (proposal:Proposal:Tally {{id: rows.proposalId}})
            MERGE (wallet)-[r:VOTED]->(proposal)
            SET r.choice = rows.support
            SET r.weight = rows.weight
            RETURN COUNT(DISTINCT(r))
            """
            count += self.query(query)[0].value()
        return count 
            