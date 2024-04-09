import os 
import re
import time
import json 
import feedparser
import pandas as pd
import requests as r 
from datetime import datetime 


class RssScraper(Scraper):
    def __init__(self, bucket_name="rss-twitter", load_data=False):
        super().__init__(bucket_name=bucket_name, load_data=load_data)
        self.rss_base_url = "https://api.rss.app"

    def create_feed(self, url):
        payload={
            "url": url
        }
        headers = {
        'Authorization': 'Bearer c_rtuymP25PLbcg9:s_CrIPmhZvTPHi8Ge1JPcIWr',
        'Content-Type': 'application/json'
        }

        response = r.post(url, headers=headers, json=payload)
        return response
print(response.text)

    def retrieve_feed_urls(self):
    # return feed_urls 

    def create_tmp_data(self):

