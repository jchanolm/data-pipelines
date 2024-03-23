from ...helpers import Cypher
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging


class MultisigCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    # def create_constraints(self):
    #     constraints = Constraints()
    #     constraints.wallets()

    # def create_indexes(self):
    #     indexes = Indexes()
    #     indexes.wallets()
    #     query = "CREATE INDEX multisigs IF NOT EXISTS FOR (n:MultiSig) ON (n.address)"
    #     self.query(query)

    @count_query_logging
    def create_or_merge_multisig_wallets(self, urls):
        count = self.queries.create_wallets(urls)
        return count

    @count_query_logging
    def add_multisig_labels_data(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                    MATCH (w:Wallet {{address: wallets.multisig}})
                    SET w:MultiSig
                    SET w.network = wallets.network
                    SET w.threshold = toInteger(wallets.threshold)
                    return count(w)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_multisig_signer(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                    MATCH (m:Wallet {{address: wallets.multisig}}), (s:Wallet {{address: wallets.ownerAddress}})
                    MERGE (s)-[r:SIGNER]->(m)
                    return count(r)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_multisig_creators(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                    MATCH (m:Wallet {{address: wallets.multisig}}), (s:Wallet {{address: wallets.creator}})
                    MERGE (s)-[r:CREATOR]->(m)
                    return count(r)
            """
            count += self.query(query)[0].value()
        return count
