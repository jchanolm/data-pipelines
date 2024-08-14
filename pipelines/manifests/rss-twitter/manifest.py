from datetime import datetime, timezone
import logging
import os
import time
import json
from tqdm import tqdm
from .cyphers import RssTwitterCyphers
from ..helpers.manifest import Manifest 
import pandas as pd
import requests as r

class EntityManifest(Manifest):
    def __init__(self):
        self.cyphers = RssTwitterCyphers()
        super().__init__("manifest-rss-twitter")

    def get_feeds(self):
        limit = 100
        offset = 0
        feeds_list = []
        while True:
            url = f"https://api.rss.app/v1/feeds?limit={limit}&offset={offset}"
            payload={}
            headers = {
                'Authorization': 'Bearer c_rtuymP25PLbcg9:s_CrIPmhZvTPHi8Ge1JPcIWr'
            }
            response = r.request("GET", url, headers=headers, data=payload)
            time.sleep(2.5)
            feeds = response.json()
            print(feeds)
            if not feeds.get('data'):
                break
            feeds_list.extend(feeds['data'])
            offset += limit
        return feeds_list
    
    def set_feed_url_metadata(self, feeds):
        feeds_df = pd.DataFrame(feeds)
        feeds_urls = self.save_df_as_csv(feeds_df, f"feeds_urls_{self.asOf}.csv")
        self.cyphers.set_feed_metadata(feeds_urls)

    
    def create_feed_for_twitter_handle(self, url):
            payload={
                "url": url
            }
            headers = {
            'Authorization': 'Bearer c_rtuymP25PLbcg9:s_CrIPmhZvTPHi8Ge1JPcIWr',
            'Content-Type': 'application/json'
            }
            try: 
                response = r.post(url, headers=headers, json=payload)
                if response.status_code != 200:
                    with Exception as e:
                        logging.error(f"Feed creation API call returned error: {e}...")
                else:
                    return response
            except Exception as e:
                logging.error(f"Exception creating feed: {e}")
            return response
        



# retrieve xmls, set on nodes


    def run(self):
        rss_feeds = self.get_feeds()
        self.set_feed_url_metadata(rss_feeds)


if __name__ == '__main__':
    M = EntityManifest()
    M.run()
