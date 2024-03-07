from pipelines.ingestion.helpers.ingestor import Ingestor
from .cyphers import ManualCyphers

import json
import pandas as pd
import datetime


class ManualIngestor(Ingestor):
    def __init__(self):        
        self.cyphers = ManualIngestor()
        super().__init__(bucket_name="arbitrum-discourse")
        




    def run(self):
        self.process_create_grants()
        self.process_create_grantees()
        self.process_create_accounts()
        self.process_connect_grantees_accounts()
        self.process_connect_grants_grantees()
        self.match_forum_posts()
        self.match_snapshot_proposals()


if __name__ == '__main__':
    ingestor = ManualIngestor()
    ingestor.run()
