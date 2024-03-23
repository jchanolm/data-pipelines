from tqdm import tqdm
from ...helpers import Cypher, Constraints, Indexes, Queries, count_query_logging


class ENSCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    # def create_constraints(self):
    #     constraints = Constraints()
    #     constraints.ens()

    # def create_indexes(self):
    #     indexes = Indexes()
    #     indexes.ens()

    @count_query_logging
    def create_or_merge_ens_domains(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as domains
                        MERGE (ens:Alias:Ens {{name: toLower(domains.name)}})
                        ON CREATE SET   ens.uuid = apoc.create.uuid(),
                                        ens.labelName = domains.labelName,
                                        ens.createdAt = datetime({{epochSeconds: toInteger(domains.createdAt)}}),
                                        ens.owner = domains.owner,
                                        ens.resolvedAddress = domains.resolvedAddress,
                                        ens.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                        ens.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        ON MATCH SET    ens.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        RETURN COUNT(DISTINCT(ens))
                        """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_ens_resolved_addresses(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as domains
                        MATCH (ens:Alias:Ens {{name: toLower(domains.name)}})
                        MATCH (wallet:Wallet {{address: toLower(domains.resolvedAddress)}})
                        MERGE (wallet)-[r:ALIAS]->(ens)
                        RETURN COUNT(r)
                """
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def link_ens_owners(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as domains
                        MATCH (ens:Alias:Ens {{name: toLower(domains.name)}})
                        MATCH (wallet:Wallet {{address: toLower(domains.owner)}})
                        MERGE (wallet)-[r:OWNS]->(ens)
                        RETURN COUNT(r)
                """
            count += self.query(query)[0].value()
        return count
    
