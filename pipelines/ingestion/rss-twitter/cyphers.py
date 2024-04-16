from ...helpers import Cypher
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging


class RssTwitCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()
