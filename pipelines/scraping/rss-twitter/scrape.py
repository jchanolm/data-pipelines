import os 
import re
import time
import json 
import logging 
import pandas as pd
import feedparser
import requests as r 
from datetime import datetime 

from ..helpers.scraper import Scraper


class RssScraper(Scraper):
    def __init__(self, bucket_name="rss-twitter", load_data=False):
        super().__init__(bucket_name=bucket_name, load_data=load_data)
        self.rss_base_url = "https://api.rss.app"



    def fetch_feed_entries(self, feed_url):
        if feed_url:
            try: 
                feed = feedparser.parse(feed_url)
                return feed
            except Exception as e:
                print(f"Error encountered: {e}")
        return None 

    def check_for_blockquote(self, entry_summary):
        return '<blockquote' in entry_summary

    def extract_block_quote_urls(self, entry_summary):
        url_pattern = r'<a href="([^"]+)">'
        urls = re.findall(url_pattern, entry_summary)
        return urls 
            
    ## collect all feeds from rss.app 
    def get_feeds(self):
        limit = 100
        offset = 0
        feeds_list = []
        while True:
            url = f"https://api.rss.app/v1/feeds?limit={limit}&offset={offset}"
            payload={}
            headers = {
                'Authorization': os.getenv("RSS_APP_BEARER")
            }
            response = r.request("GET", url, headers=headers, data=payload)
            time.sleep(2.5)
            feeds = response.json()
            if not feeds.get('data'):
                break
            feeds_list.extend(feeds['data'])
            offset += limit
        return feeds_list
    
    def retrieve_feed_entries(self, feed_xml_url):
        if not feed_xml_url:
            logging.error("No feed xml url passed.")
        else:
            feed_entries = self.fetch_feed_entries(feed_xml_url)
            time.sleep(.25)
        return feed_entries
    
    def retrieve_all_feed_entries(self, feed_urls):
        counter = 0 
        all_entries = []
        for feed in feed_urls:
            account = feed['source_url']
            rss_feed_url = feed['rss_feed_url']
            counter += 1
            logging.info(f"""Collecting feed for account: {account} the {counter}th feed out of {len(feed_urls)}...""")
            try:
                response = self.retrieve_feed_entries(rss_feed_url)
                feed_entries = response.get('entries') 
                all_entries.extend(feed_entries)
                if not feed_entries:
                    break 
                else:
                    all_entries.extend(feed_entries)
            except Exception as e:
                logging.info(f"Exception collecting feed entries: {e}")
        return all_entries



        # retrieve results for each feed
        # add to feeds list
        # filter feeds list for items before last_scraped
        # return feeds


    def run(self):
        all_feeds = self.get_feeds()
        all_feed_entries = self.retrieve_all_feed_entries(all_feeds)
        most_recent_published_date = max(entry['published_parsed'] for entry in all_feed_entries)

        self.data['data'] = all_feed_entries
        self.metadata['most_recent_published_date'] = most_recent_published_date
        df = pd.DataFrame(self.data['data'])
        df.to_csv('airport.gay')
        self.save_metadata()
        self.save_data()
        with open('test.json', 'w') as f:
            json.dump(self.data['data'], f, indent=4)
if __name__ == "__main__":
    S = RssScraper()
    S.run()
## this simply takes a list of xml urls, scrapes, and stores. then they go into a
## will do ingestor in batches for timeliness

