import sys
from datetime import datetime
import logging
import os
from ...helpers import Base


class Scraper(Base):
    def __init__(self, bucket_name, load_data=False, chain="ethereum"):
        Base.__init__(self, bucket_name=bucket_name, metadata_filename="scraper_metadata.json", load_data=load_data, chain=chain)
        self.runtime = datetime.now()

    def run(self):
        "Main function to be called. Every scrapper must implement its own run function !"
        raise NotImplementedError("ERROR: the run function has not been implemented!")


