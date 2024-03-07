import sys
from datetime import datetime
import logging
import os
from ...helpers import Base

# This class is the base class for all scrapers.
# Every scraper must inherit this class and define its own run function
# The base class takes care of loading the metadata and checking that data was not already ingested for today
# The data and metadata are not saved automatically at the end of the execution to allow for user defined save points.
# Use the save_metadata and save_data functions to automatically save to S3
# During the run function, save the data into the instance.data field.
# The data field is a dictionary, define each root key as you would a mongoDB index.

class Scraper(Base):
    def __init__(self, bucket_name, load_data=False, chain="ethereum"):
        Base.__init__(self, bucket_name=bucket_name, metadata_filename="scraper_metadata.json", load_data=load_data, chain=chain)
        self.runtime = datetime.now()

    def run(self):
        "Main function to be called. Every scrapper must implement its own run function !"
        raise NotImplementedError("ERROR: the run function has not been implemented!")
