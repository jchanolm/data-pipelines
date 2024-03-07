from .cypher import Cypher


class Constraints(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    def create_constraints(self) -> None:
        pass

    def create_indexes(self) -> None:
        pass

    def twitter(self) -> None:
        query = """CREATE CONSTRAINT UniqueHandle IF NOT EXISTS FOR (d:Twitter) REQUIRE d.handle IS UNIQUE"""
        self.query(query)

    def wallets(self) -> None:
        query = """CREATE CONSTRAINT UniqueAddress IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE"""
        self.query(query)

    def tokens(self) -> None:
        query = """CREATE CONSTRAINT UniqueTokenAddress IF NOT EXISTS FOR (d:Token) REQUIRE d.address IS UNIQUE"""
        self.query(query)

    def proposals(self) -> None:
        query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:Proposal) REQUIRE d.snapshotId IS UNIQUE"""
        self.query(query)


    def website(self) -> None:
        query = """CREATE CONSTRAINT UniqueWebsite IF NOT EXISTS FOR (a:Website) REQUIRE a.url IS UNIQUE"""
        self.query(query)
