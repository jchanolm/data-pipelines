from ..helpers import Ingestor
from ...helpers import Utils
import datetime  
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging
import datetime 
import logging 

class SnapshotIngestor(Ingestor):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()
        self.asOf = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M")


    def process_spaces_metadata(self, urls):

    
    def process_space_members(self, urls):

    def process_proposals(self, urls):

    def process_votes(self, urls):
    

    def run():
        logging.info("Run complete!")

if __name__ == '__main__':
    ingestor = SnapshotIngestor()
    ingestor.run()

