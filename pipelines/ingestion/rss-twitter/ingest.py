from ..helpers import Ingestor
from .cyphers import RssTwitCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging


class RssTwitterIngestor(Ingestor):
    def __init__(self):
        self.cyphers = RssTwitCyphers()
        super().__init__("rss-twitter")
        self.tweets = self.scraper_data['data']
        print(self.tweets)
        self.llm_model =## fjdkfkjdjfd  gpt-3.5-turbo-0125


    def is_rt(self):

        return 
    
    def get_update_title(self):

        update_title = None 

        return update_title 
    
    def get_retweet_info(self):
        rt_info = None 
        return rt_info

    def extract_links(self):
        links = None 
        return links

    def extract_tagged_ents(self):
        ents = None
        return ents


    def add_update_types(self):
        update_types = None 
        return update_types


    def add_is_actionable(self):

        is_actionable = None
        return is_actionable

    
    def pre_process_tweets(self):
        all_processed_tweets = []
        for tweet in self.tweets:
            try:
                text = self.sanitize_text(tweet.get('text'))
                tweet_dict = {
                    'text': tweet.get('title'),
                    'tweetId': tweet.get('id'),
                    'publishedDt': tweet.get('published_date_parsed'),
                    'publishedDtStr': tweet.get('published'),
                    'url': tweet.get('url'),
                    'text': text
                }
                all_processed_tweets.extend(tweet_dict)
            except Exception as e:
                logging.info(f"Error processing tweets: {e} ")
        return all_processed_tweets



        return tweets_df 
    
    def enrich_tweet(self, post, model):

        return enriched_tweet


    def ingest_processed_post(self, enriched_post):

        self.cyphers.create_connect_post()

    def ingest_all_processed_posts(self):
        all_posts = self.posts
        logging.info(f"Ingesting {len(all_posts)} posts.....")
        counter = 0 
        for post in all_posts:
            try:
                enriched_post = self.enrich_post(post)
                self.ingest_processed_post(enriched_post)
                counter += 1 
                logging.info(f"Ingested post {str(counter)} out of {len(all_posts)}..")
            except Exception as e:
                logging.info(f"Error processing or ingesting post: '{e}'")


    def run(self):
        self.save_metadata()


if __name__ == "__main__":
    ingestor = RssTwitterIngestor()
    ingestor.run()
