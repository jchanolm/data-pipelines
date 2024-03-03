import logging 
import pandas as pd
import requests as r 
import json
from datetime import datetime 
from ...helpers import Requests
from ..helpers import Extractor 
from ...helpers import S3Utils

class ArbForumExtractor(Extractor):
    def __init__(self, bucket_name="arb-data"):
        super().__init__(bucket_name)
        ## data

        ##
        self.s3 = S3Utils(bucket_name=bucket_name)
        self.metadata['timestamp'] = self.asOf

    def run(self):
        logging.info("Dumping community portal data....")
        self.get_db_dump()
        logging.info("Saving community portal data....")
        self.save_data()
        logging.info("Saving metadata....")
        self.save_metadata()
    

if __name__ == "__main__":
    extractor = ArbForumExtractor()
    print("Hello world")
    extractor.run()
    logging.info("Run complete. Good job! :party-parrot:")

