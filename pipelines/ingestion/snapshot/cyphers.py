from ...helpers import Cypher
from ...helpers import Constraints
from ...helpers import Indexes
from ...helpers import Queries
from ...helpers import count_query_logging
import logging
import sys


class SnapshotCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    # def create_constraints(self):
    #     constraints = Constraints()
    #     constraints.spaces()
    #     constraints.proposals()
    #     constraints.wallets()
    #     constraints.tokens()
    #     constraints.aliases()
    #     constraints.ens()
    #     constraints.twitter()
    #     constraints.transactions()

    # def create_indexes(self):
    #     indexes = Indexes()
    #     indexes.wallets()
    #     indexes.proposals()
    #     indexes.spaces()
    #     indexes.tokens()
    #     indexes.aliases()
    #     indexes.ens()
    #     indexes.twitter()
    #     indexes.transactions()

    @count_query_logging
    def create_or_merge_spaces(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS spaces
                    MERGE(s:EntitySnapshotSpace:Snapshot:Space:Entity {{snapshotId: spaces.snapshotId}})
                    ON CREATE set s.uuid = apoc.create.uuid(),
                        s.name = spaces.name, 
                        s.chainId = toInteger(spaces.chainId), 
                        s.onlyMembers = spaces.onlyMembers, 
                        s.symbol = spaces.symbol,
                        s.twitter = spaces.twitter,
                        s.profileUrl = spaces.profileUrl,
                        s.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        s.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set s.name = spaces.name,
                        s.onlyMembers = spaces.onlyMembers,
                        s.twitter = spaces.twitter,
                        s.symbol = spaces.symbol,
                        s.profileUrl = spaces.profileUrl,
                        s.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(s)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_proposals(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                    MERGE(p:Snapshot:Proposal:ProposalSnapshot {{snapshotId: proposals.snapshotId}})
                    ON CREATE set p.uuid = apoc.create.uuid(),
                        p.snapshotId = proposals.snapshotId,
                        p.ipfsCID = proposals.ipfsCId,
                        p.title = proposals.title,
                        p.text = proposals.text,
                        p.choices = proposals.choices,
                        p.type = proposals.type,
                        p.author = proposals.author,
                        p.state = proposals.state,
                        p.link = proposals.link,
                        p.startDt = datetime(apoc.date.toISO8601(toInteger(proposals.startDt), 's')),
                        p.endDt = datetime(apoc.date.toISO8601(toInteger(proposals.endDt), 's')),
                        p.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        p.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set p.title = proposals.title,
                        p.text = proposals.text,
                        p.choices = proposals.choices,
                        p.type = proposals.type,
                        p.state = proposals.state,
                        p.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(p)
            """
            print(query)
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_space_twitter(self, urls):
        count = self.queries.create_or_merge_twitter(urls)
        return count

    @count_query_logging
    def create_or_merge_space_tokens(self, urls):
        count = self.queries.create_or_merge_tokens(urls, "ERC20")
        return count

    @count_query_logging
    def create_or_merge_space_ens(self, urls):
        count = 0
        count += self.queries.create_or_merge_ens_alias(urls)
        count += self.queries.create_wallets(urls)
        count += self.queries.create_or_merge_ens_nft(urls)
        # count += self.queries.create_or_merge_transaction(urls)
        return count

    @count_query_logging
    def link_space_ens(self, urls):
        count = 0
        count += self.queries.link_wallet_alias(urls)
        # count += self.queries.link_wallet_transaction_ens(urls)
        # count += self.queries.link_ens_transaction(urls)
        count += self.queries.link_ens_alias(urls)
        return count

    @count_query_logging
    def create_or_merge_members(self, urls):
        count = self.queries.create_wallets(urls)
        return count

    @count_query_logging
    def create_or_merge_authors(self, urls):
        count = self.queries.create_wallets(urls)
        return count

    @count_query_logging
    def create_or_merge_voters(self, urls):
        count = self.queries.create_wallets(urls)
        return count

    @count_query_logging
    def link_proposal_spaces(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' as proposals
                    MATCH (p:Proposal {{snapshotId: proposals.snapshotId}}), (s:Space {{snapshotId: proposals.spaceId}})
                    MERGE (s)-[d:HAS_PROPOSAL]->(p)
                    return count(d)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_proposal_authors(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' as proposals
                    MATCH (p:Proposal {{snapshotId: proposals.snapshotId}}), (w:Wallet {{address: proposals.address}})
                    MERGE (p)<-[d:AUTHOR]-(w)
                    return count(d)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_strategies(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS strategy
                    MERGE(s:Snapshot:Strategy {{id: strategy.id}})
                    ON CREATE set s = strategy
                    ON MATCH set s = strategy
                    return count(s)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_member_spaces(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' as members
                    MATCH (w:Wallet {{address: toLower(members.address)}}), (s:Space {{snapshotId: members.space}})
                    MERGE (w)-[r:CONTRIBUTOR]->(s)
                    ON CREATE SET r.type = 'member',
                        r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH SET r.type = 'member'
                    return count(r)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_admin_spaces(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' as admins
                    MATCH (w:Wallet {{address: toLower(admins.address)}}), (s:Space {{snapshotId: admins.space}})
                    MERGE (w)-[r:CONTRIBUTOR]->(s)
                    ON CREATE SET r.type = 'admin',
                        r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH SET r.type = 'admin'
                    return count(r)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_space_alias(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' as ens
                    MATCH (s:Space {{snapshotId: ens.name}}),
                        (a:Alias {{name: toLower(ens.name)}})
                    MERGE (s)-[n:HAS_ALIAS]->(a)
                    return count(n)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_space_twitter(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' as twitter
                    WITH twitter WHERE twitter.twitterHandle IS NOT NULL
                    MATCH (s:Space {{snapshotId: twitter.snapshotId}})
                    MATCH (t:Twitter:Account {{handle: toLower(twitter.twitterHandle)}})
                    MERGE (s)-[r:HAS_ACCOUNT]->(t)
                    return count(r) 
            """
            print(query)
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_votes(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS votes
                    MATCH (w:Wallet {{address: votes.voter}}), (p:Proposal {{snapshotId: votes.proposalId}}) 
                    WITH w, p, datetime(apoc.date.toISO8601(toInteger(votes.votedAt), 's')) AS vDt, votes.choice as choice
                    MERGE (w)-[v:VOTED]->(p)
                    ON CREATE set v.votedDt = vDt,
                        v.choice = choice
                    ON MATCH set v.votedDt = vDt,
                        v.choice = choice
                    return count(v)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_strategies(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' as strats
                    MATCH (t:Token {{address: strats.token}}), (s:Space {{snapshotId: strats.space}})
                    MERGE (s)-[n:HAS_STRATEGY]->(t)
                    return count(n)
            """
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def create_connect_githubs(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MATCH (space:Snapshot:Space {{snapshotId: rows.snapshotId}})
            MERGE (github:Account:Github {{handle: rows.github}})
            WITH space, github
            MERGE (space)-[r:ACCOUNT]->(github
            """
            count += self.query(query)[0].value()
        return count 
