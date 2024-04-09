from datetime import datetime, timezone
import logging
import os
import time
from tqdm import tqdm
from .cyphers import RssTwitterCyphers
from ..helpers.manifest import Manifest 
import pandas as pd
import requests as r

class EntityManifest(Manifest):
    def __init__(self):
        self.cyphers = RssTwitterCyphers()
        super().__init__("manifest-rss-twitter")

    def get_feeds_with_offset(self):
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
            feeds = response.json()
            print(feeds)
            if feeds['total'] == 0:
                break
            feeds_list.extend(feeds['data'])
            offset += limit
        return feeds_list
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

