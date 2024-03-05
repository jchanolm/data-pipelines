import sys
import json 
from datetime import datetime
import time
import logging
import os


class Scraper():
    def __init__(self, metadata_filename="scraper_metadata.json") -> None:
        self.runtime = datetime.now()
        self.metadata_filename = metadata_filename
        self.data = {}
        self.metadata = {
            'runtime': self.runtime
        }


    def save_data_local(self, chunk_prefix: str = "") -> None: 
        """
        Saves data to data/data.json.
        You can specify a file prefix with chunk_prefix to avoid name collision
        """
        logging.info("Saving results to data/")
        if chunk_prefix:
            filename = f"{chunk_prefix}-data.json" if chunk_prefix else "data.json"
            with open(f"data/{filename}", "w") as f:
                json.dump(self.data, f)



    # def save_data_s3(self, chunk_prefix: str = "") -> None:
        ### add function to save to s3 for deployed pipelines

    def save_metadata(self):
        """
        Save metadata for future runs, i.e. storing the date of the last retrieved object 
        to start the next run with objects following that date.
        """
        with open(self.metadata_filename, 'w') as f:
            json.dump(self.metadata, f, ensure_ascii=False, indent=4)
        

    def run(self):
        raise NotImplementedError("ERROR: the run function has not been implemented!")
