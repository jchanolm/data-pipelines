from ...helpers import Cypher
from ...helpers import count_query_logging


class AlloCyphers(Cypher):
    def __init__(self):
        super().__init__()

    @count_query_logging
    def create_protocols(self, urls):
        count = 0 
        for url in urls:
            ## i have no idea what the id is 
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            WITH rows, split(rows.id, "-")[0] as protocolId
            MERGE (allo:Allo:Protocol {{id: tolower(protocolId)}})
            SET allo.protocol = rows.protocol
            SET allo.pointer = rows.pointer
            RETURN COUNT(allo)"""
            print(query)
            count += self.query(query)[0].value()
        return count 


    @count_query_logging
    def create_rounds(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (round:Allo:Round:GrantInitiative {{id: tolower(rows.id)}})
            SET round.protocol = rows.protocol 
            SET round.matchAmount = rows.matchAmount 
            SET round.applicationsStartTime = rows.applicationsStartTime
            SET round.applicationsEndTime = rows.applicationsEndTime 
            SET round.roundFeeAddress = rows.roundFeeAddress 
            SET round.projectId = tolower(rows.projectId)
            SET round.roundFeePercentage = rows.roundFeePercentage
            RETURN COUNT(round)
            """
            count += self.query(query)[0].value()
            print(query)
        return count 
    
    @count_query_logging
    def connect_rounds_protocols(self, urls):
        count = 0 
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS rows
                WITH rows, replace(rows.protocolIdRaw, "applicationsMetaPtr-", "") as protocolId
                MATCH (protocol:Protocol {{id: tolower(protocolId)}})
                MATCH (round:Round {{id: tolower(rows.id)}})
                MERGE (protocol)-[r:ROUND]->(round)
                RETURN COUNT(round)            """
            print(query)
            count += self.query(query)[0].value()
        return count 

    @count_query_logging
    def create_connect_round_tokens(self, urls):
        count = 0 
        for url in urls:
            create_token_query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (token:Token {{address: tolower(rows.token)}})
            RETURN COUNT(token)
            """
            count += self.query(create_token_query)[0].value()

            connect_token_round_query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MATCH (token:Token {{address: tolower(rows.token)}})
            MATCH (round:Round {{id: rows.id}})
            MERGE (round)-[r:TOKEN]->(token)
            SET round.tokenAddress = tolower(rows.token)
            RETURN COUNT(round)
            """
            count += self.query(connect_token_round_query)[0].value()
            print(connect_token_round_query)
        return count 

    @count_query_logging
    def create_connect_round_fee_addresses(self, urls):
        count = 0 
        for url in urls:
            create = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (wallet:Account:Wallet {{address: tolower(rows.roundFeeAddress)}})
            RETURN COUNT(wallet)"""
            count += self.query(create)[0].value()

            connect = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MATCH (wallet:Wallet {{address: tolower(rows.roundFeeAddress)}})
            MATCH (round:Round {{id: rows.id}})
            MERGE (round)-[r:ACCOUNT]->(wallet)
            RETURN COUNT(DISTINCT(round))
            """

            count += self.query(connect)[0].value()
        return count 

    
    @count_query_logging
    def create_round_accounts(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MERGE (wallet:Account:Wallet {{address: rows.accountAddress}})
            RETURN COUNT(wallet)
            """
            count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def connect_round_accounts(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MATCH (wallet:Wallet {{address: rows.accountAddress}})
            MATCH (round:Round {{id: rows.roundId}})
            MERGE (wallet)-[r:OPERATES]->(round)
            SET r.role = rows.roleId
            RETURN COUNT(wallet)
            """
            count += self.query(query)[0].value()
        return count 

    @count_query_logging
    def create_connect_projects(self, urls):
        count = 0 
        for url in urls:
            create = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            WITH rows
            WHERE rows.projectId IS NOT NULL
            MERGE (allo:Allo:Project:GrantInitiative {{id: tolower(rows.projectId)}})
            RETURN COUNT(allo)
            """
            count += self.query(create)[0].value()
            print(create)
            connect = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS rows
            MATCH (project:Project {{id: tolower(rows.projectId)}})
            MATCH (round:Round {{id: tolower(rows.roundId)}})
            MERGE (round)-[r:PROJECT]->(project)
            RETURN COUNT(project)
            """
            print(connect)
            count += self.query(connect)[0].value()
        return count 
    
    @count_query_logging
    def create_grantees(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (wallet:Account:Wallet {{address: tolower(rows.grantee)}})
            RETURN COUNT(wallet)
            """
            count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def create_connect_payouts(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            WITH rows
            WHERE rows.projectId IS NOT NULL
            MATCH (grantee: Wallet {{address: tolower(rows.grantee)}})
            MATCH (project:Project {{id: tolower(rows.projectId)}})-[]-(round:Round)
            MERGE (round)-[r:GRANTEE]->(grantee)
            MERGE (project)-[r1:PAYOUT]->(grantee)
            SET r1.amount = r1.amount + rows.amount
            RETURN COUNT(grantee)
            """
            print(query)
            count += self.query(query)[0].value()
        return count 


