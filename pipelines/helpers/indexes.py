from .cypher import Cypher


class Indexes(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    def create_constraints(self) -> None:
        pass

    def create_indexes(self) -> None:
        pass

    # def contracts(self) -> None:
    #     query = """CREATE INDEX UniqueAddress IF NOT EXISTS FOR (n:Contract) ON (n.address)"""
    #     self.query(query)

    def proposals(self) -> None:
        query = """CREATE INDEX UniquePropID IF NOT EXISTS FOR (n:Proposal) ON (n.snapshotId)"""
        self.query(query)

    # def spaces(self) -> None:
    #     query = """CREATE INDEX UniqueSpaceID IF NOT EXISTS FOR (n:Space) ON (n.snapshotId)"""
    #     self.query(query)

    def wallets(self) -> None:
        query = """CREATE INDEX UniqueWalletAddress IF NOT EXISTS FOR (n:Wallet) ON (n.address)"""
        self.query(query)

    def accounts(self) -> None:
        query = "CREATE INDEX AccountHandles IF NOT EXISTS FOR (n:Account) ON (n.handle)"
        self.query(query)

    def tokens(self) -> None:
        query = """CREATE INDEX UniqueTokenAddress IF NOT EXISTS FOR (d:Token) ON (d.address)"""
        self.query(query)

    # def ens(self) -> None:
    #     query = "CREATE INDEX ENSName IF NOT EXISTS FOR (n:Ens) ON (n.name)"
    #     self.query(query)

    # def transactions(self) -> None:
    #     query = """CREATE INDEX UniqueTransaction IF NOT EXISTS FOR (n:Transaction) ON (n.txHash)"""
    #     self.query(query)

    # def aliases(self) -> None:
    #     query = """CREATE INDEX UniqueAlias IF NOT EXISTS FOR (n:Alias) ON (n.name)"""
    #     self.query(query)

    # def articles(self) -> None:
    #     query = """CREATE INDEX UniqueArticleID IF NOT EXISTS FOR (n:Mirror) ON (n.uri)"""
    #     self.query(query)

    def twitter(self) -> None:
        query = """CREATE INDEX UniqueTwitterID IF NOT EXISTS FOR (n:Twitter) ON (n.handle)"""
        self.query(query)

    # def gitcoin_grants(self) -> None:
    #     query = """CREATE INDEX UniqueGrantID IF NOT EXISTS FOR (n:GitcoinGrant) ON (n.id)"""
    #     self.query(query)

    # def gitcoin_users(self) -> None:
    #     query = """CREATE INDEX UniqueUserHandle IF NOT EXISTS FOR (n:GitcoinUser) ON (n.handle)"""
    #     self.query(query)

    # def gitcoin_bounties(self) -> None:
    #     query = """CREATE INDEX UniqueBountyID IF NOT EXISTS FOR (n:GitcoinBounty) ON (n.id)"""
    #     self.query(query)

    # def mirror_articles(self) -> None:
    #     query = """CREATE INDEX UniqueArticleID IF NOT EXISTS FOR (a:Mirror) ON a.originalContentDigest"""
    #     self.query(query)

    # def daohaus_dao(self) -> None:
    #     query = """CREATE INDEX UniqueDaoID IF NOT EXISTS FOR (a:Dao) ON a.daohausId"""
    #     self.query(query)

    # def daohaus_proposal(self) -> None:
    #     query = """CREATE INDEX UniqueProposalID IF NOT EXISTS FOR (a:Proposal) ON a.proposalId"""
    #     self.query(query)

    # def website(self) -> None:
    #     query = """CREATE INDEX UniqueWebsiteID IF NOT EXISTS FOR (a:Website) ON a.url"""
    #     self.query(query)

    # def email(self) -> None:
    #     query = "CREATE INDEX Emails IF NOT EXISTS FOR (e:Email) ON (e.email)"
    #     self.query(query)
        
    # def wicIndexes(self) -> None:
    #     query = """CREATE FULLTEXT INDEX wicArticles IF NOT EXISTS FOR (a:Article) ON EACH [a.text, a.title]"""
    #     self.query(query)
    #     query = """CREATE FULLTEXT INDEX wicBios IF NOT EXISTS FOR (a:Twitter|Github|Dune) ON EACH [a.bio]"""
    #     self.query(query)
    #     query = """CREATE FULLTEXT INDEX wicGrants IF NOT EXISTS FOR (a:Grant) ON EACH [a.text, a.title]"""
    #     self.query(query)
    #     query = """CREATE FULLTEXT INDEX wicProposals IF NOT EXISTS FOR (a:Proposal) ON EACH [a.text, a.title]"""
    #     self.query(query)
    #     query = """CREATE FULLTEXT INDEX wicTwitter IF NOT EXISTS FOR (a:Twitter) ON EACH [a.bio]"""

    # def sound(self):
    #     query = "CREATE INDEX Sound IF NOT EXISTS FOR (e:Sound) ON (e.handle)"
    #     self.query(query)

    # def telegram(self):
    #     query = "CREATE INDEX Telegram IF NOT EXISTS FOR (e:Telegram) ON (e.handle)"
    #     self.query(query)

    # def dune(self):
    #     query = "CREATE INDEX Dune IF NOT EXISTS FOR (e:Dune) ON (e.handle)"
    #     self.query(query)

    def walletsBools(self):
        query = "CREATE INDEX walletBooleans IF NOT EXISTS FOR (w:Wallet) ON (w.notifySelected)"
        self.query(query)

