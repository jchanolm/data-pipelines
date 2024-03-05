import sys
import json 
from datetime import datetime
import time
import logging
import os

import os
import json
from datetime import datetime

class Scraper():

    def __init__(self, module_name, metadata_filename="scraper_metadata.json"):
        self.runtime = datetime.now().strftime('%Y-%m-%d')
        self.module_name = module_name # must be defined in subclasses
        self.data = {}
        self.metadata = {'runtime': self.runtime}
        self.data_filename = f"pipelines/scraping/{self.module_name}/data/data_{self.runtime}.json"
        self.metadata_filename = f"pipelines/scraping/{self.module_name}/data/{metadata_filename}"


    def save_data_local(self):
        os.makedirs(os.path.dirname(self.data_filename), exist_ok=True)
        with open(self.data_filename, "w") as f:
            json.dump(self.data, f)

    def save_metadata_local(self):
        os.makedirs(os.path.dirname(self.metadata_filename), exist_ok=True)
        with open(self.metadata_filename, 'w') as f:
            json.dump(self.metadata, f, ensure_ascii=False, indent=4)