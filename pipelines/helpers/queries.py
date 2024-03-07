import time
from tqdm import tqdm
from .cypher import Cypher
from .decorators import count_query_logging
# This file is for universal queries only, any queries that generate new nodes or edges must be in its own cyphers.py file in the service folder


class Queries(Cypher):
    """This class holds queries for general nodes such as Wallet or Twitter"""

    def __init__(self, database=None):
        super().__init__(database)

    def create_constraints(self):
        pass

    def create_indexes(self):
        pass

    @count_query_logging
    def create_wallets(self, urls):
        """csv is: address"""
        count = 0
        for url in tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                    MERGE(wallet:Wallet:Account {{address: toLower(wallets.address)}})
                    ON CREATE set wallet.uuid = apoc.create.uuid(),
                        wallet.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.ingestedBy = "{self.UPDATED_ID}"
                    return count(wallet)
            """
            count += self.query(query)[0].value()
            time.sleep(1)
        return count

    @count_query_logging
    def create_or_merge_twitter(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS twitter
                    MERGE (t:Twitter:Account {{handle: toLower(twitter.handle)}})
                    ON CREATE set t.uuid = apoc.create.uuid(),
                        t.profileUrl = twitter.profileUrl,
                        t.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        t.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        t.ingestedBy = "{self.CREATED_ID}",
                        t:Account
                    ON MATCH set t.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        t.ingestedBy = "{self.UPDATED_ID}"
                    return count(t)    
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_emails(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS emails
                MERGE (email:Email:Account {{email: emails.email}})
                ON CREATE SET   link.uuid = apoc.create.uuid(),
                                email.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                email.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                email.ingestedBy = "{self.CREATED_ID}"
                ON MATCH SET    email.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                email.ingestedBy = "{self.UPDATED_ID}"
                RETURN count(email)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_ens_alias(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS alias
                    MERGE (a:Alias:Ens {{name: toLower(alias.name)}})
                    ON CREATE set a.uuid = apoc.create.uuid(),
                        a.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        a.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(a)
                    """

            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_ens_nft(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS ens
                    MERGE (e:Ens:Nft {{editionId: ens.tokenId}})
                    ON CREATE set e.uuid = apoc.create.uuid(),
                        e.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        e.contractAddress = ens.contractAddress
                    return count(e)
                    """

            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_transaction(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS tx
                    MERGE (t:Transaction {{txHash: toLower(tx.txHash)}})
                    ON CREATE set t.uuid = apoc.create.uuid(),
                        t.date = datetime(apoc.date.toISO8601(toInteger(tx.date), 's')),
                        t:Event
                    return count(t)
                    """

            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_wallet_alias(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS alias
                    MATCH (a:Alias {{name: toLower(alias.name)}}), 
                        (w:Wallet {{address: toLower(alias.address)}})
                    MERGE (w)-[r:HAS_ALIAS]->(a)
                    return count(r)
                    """

            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_wallet_transaction_ens(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS tx
                    MATCH (t:Transaction {{txHash: toLower(tx.txHash)}}), 
                        (e:Ens {{editionId: tx.tokenId}})
                    MERGE (w)-[r:RECEIVED]->(t)
                    return count(r)
                    """

            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_ens_transaction(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS tx
                    MATCH (t:Transaction {{txHash: toLower(tx.txHash)}}), 
                        (e:Ens {{editionId: tx.tokenId}})
                    MERGE (e)-[r:TRANSFERRED]->(t)
                    return count(t)
                    """

            count += self.query(query)[0].value()

    @count_query_logging
    def link_ens_alias(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS ens
                    MATCH (e:Ens {{editionId: ens.tokenId}}), 
                        (a:Alias {{name: toLower(ens.name)}})
                    MERGE (e)-[r:HAS_NAME]->(a)
                    return count(r)
                    """

            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_partitions(self, urls, label):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS partitions
                MERGE(partition:Partition:{label} {{partitionTarget: partitions.partitionTarget, partition: partitions.partition}})
                ON CREATE set partition.uuid = apoc.create.uuid(),
                    partition.asOf = partitions.asOf,
                    partition.method = partitions.method,
                    partition.partition = partitions.partition,
                    partition.partitionTarget = partitions.partitionTarget,
                    partition.createdDt = datetime(
                        apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    partition.lastUpdateDt = datetime(
                        apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    partition.ingestedBy = "{self.CREATED_ID}"
                ON MATCH set partition.partition = partitions.partition,
                    partition.asOf = partitions.asOf,
                    partition.method = partitions.method,
                    partition.partitionTarget = partitions.partitionTarget,
                    partition.lastUpdateDt = datetime(
                        apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    partition.ingestedBy = "{self.UPDATED_ID}"
                return count(partition)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_partitions(self, urls, partitionTarget, targetField, label):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS partitions
                MATCH (target:{partitionTarget} {{ {targetField}: partitions.targetField }}), (partition:Partition:{label} {{partitionTarget: "{partitionTarget}", partition: partitions.partition }})
                WITH target, partition, partitions
                MERGE (target)-[link:HAS_PARTITION]->(partition)
                ON CREATE set link.asOf = partitions.asOf,
                    link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    link.ingestedBy = "{self.CREATED_ID}"
                ON MATCH set link.asOf = partitions.asOf,
                    link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    link.ingestedBy = "{self.UPDATED_ID}"
                RETURN count(link)
            """
            print(query)
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_tokens(self, urls, token_type, chain_id = 1):
        "CSV Must have the columns: [contractAddress, symbol, decimal]"
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                MERGE(token:Token {{address: toLower(tokens.contractAddress)}})
                ON CREATE set token.uuid = apoc.create.uuid(),
                    token.chainId = {chain_id},
                    token.symbol = tokens.id, 
                    token.decimal = tokens.id, 
                    token.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    token.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    token.ingestedBy = "{self.CREATED_ID}",
                    token:{token_type}
                ON MATCH set token.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    token.ingestedBy = "{self.UPDATED_ID}",
                    token:{token_type}
                return count(token)
            """
            count += self.query(query)[0].value()
        return count
