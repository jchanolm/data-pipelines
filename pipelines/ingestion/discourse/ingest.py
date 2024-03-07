from pipelines.ingestion.helpers.ingestor import Ingestor
from .cyphers import DiscourseCyphers

import json
import pandas as pd
import datetime


class DiscourseIngestor(Ingestor):
    def __init__(self):        
        self.cyphers = DiscourseCyphers()
        super().__init__(bucket_name="arbitrum-discourse")
        

    def process_create_posts(self):
        posts_df = pd.DataFrame(self.scraper_data['posts'])
        urls = self.save_df_as_csv(posts_df, f"ingestor_posts_{self.asOf}")
        self.cyphers.create_posts(urls)


    def run(self):
        self.process_create_posts()

if __name__ == '__main__':
    ingestor = DiscourseIngestor()
    ingestor.run()
