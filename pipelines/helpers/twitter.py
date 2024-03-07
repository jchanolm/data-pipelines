from datetime import datetime, timezone
import logging
import os
import time

import numpy as np
import requests
from .requests import Requests

DEBUG = os.environ.get("DEBUG", False)

class Twitter(Requests):
    def __init__(self) -> None:
        super().__init__()
        self.api_url = "https://api.twitter.com/2"
        self.tweets_api_url = self.api_url + "/tweets"
        self.twitter_api_tokens = [el.strip() for el in os.environ.get("TWITTER_BEARER_TOKEN", "").split(",")]
        self.rate_limited = {}
        for token in self.twitter_api_tokens:
            self.rate_limited[token] = False

    def is_rate_limited(self, response: requests.models.Response, token: str) -> bool:
        try:
            content = response.json()
        except:
            return True
        if "title" in content and content["title"] == "Too Many Requests":
            self.rate_limited[token] = True
            time_to_wait = int(response.headers["x-rate-limit-reset"]) - int(datetime.now(timezone.utc).timestamp())
            logging.warning(f"Rate limit exceeded. Waiting {time_to_wait} seconds.")
            time.sleep(time_to_wait)
            self.rate_limited[token] = False
            return True
        return False

    def get_headers(self) -> tuple[dict, str]:
        """
        Returns an authorization header chosing a random API token from the provided tokens. 
        Returns the token along with the headers.
        Checks that the token is not marked as rate limited."""
        token = np.random.choice(self.twitter_api_tokens)
        while self.rate_limited[token]:
            token = np.random.choice(self.twitter_api_tokens)
        twitter_headers = {
            "Authorization": f"Bearer {token}",
        }
        return twitter_headers, token

    def get_request(self, 
                    url: str, 
                    params: dict|None = None, 
                    counter: int = 0) -> dict|None:
        "Wrapper over the Request.get_request that adds a check for rate limit and sleeps the required amount."
        if counter > 10:
            return None
        headers, token = self.get_headers()
        response = super().get_request(url, params=params, headers=headers, json=False, decode=False, ignore_retries=True)
        if self.is_rate_limited(response, token) or response.status_code != 200:
            return self.get_request(url, params=params, counter=counter+1)
        return response.json()

    def search_tweet(self, 
                     query: str, 
                     user_info: bool = False, 
                     since_id: int|None = None, 
                     tweets: list = [], 
                     users: list = [], 
                     meta: dict = {"newest_id": 0, "oldest_id": np.inf}, 
                     max_results: int = 100, 
                     next_token: str|None = None) -> tuple[list, list, list]:
        """
        Search tweet API wrapper. This performs a search for tweets given a keyword query as defined by the Twitter Search API query syntax.
        Parameters:
          - query: Text search query.
          - user_info: Wether to return the name and username fields in the includes
          - since_id: If set to an ID, filter only tweet that happened after this ID
          - max_results: defaults to 100, can be changed to less than 100 (Twitter API limit)
        """
        
        if DEBUG and len(tweets) > 500:
            return tweets, users, meta
        params = {
            "query": query,
            "tweet.fields": "text,author_id,created_at,id,conversation_id",
            "max_results": max_results
        }
        if user_info: params["expansions"] = "author_id"
        if user_info: params["user.fields"] = "name,username"
        if since_id: params["since_id"] = since_id
        if next_token: params["next_token"] = next_token
        url = self.tweets_api_url + "/search/recent"
        result = self.get_request(url, params=params)
        if result and type(result) == dict:
            if "data" in result: 
                tweets.extend(result["data"])
            if "includes" in result and "users" in result["includes"]:
                users.extend(result["includes"]["users"])
            if "meta" in result:
                if "newest_id" in result["meta"]: meta["newest_id"] = max(meta["newest_id"], int(result["meta"]["newest_id"]))
                if "oldest_id" in result["meta"]: meta["oldest_id"] = min(meta["oldest_id"], int(result["meta"]["oldest_id"]))
                if "next_token" in result["meta"]:
                    return self.search_tweet(query, user_info=user_info, since_id=since_id, tweets=tweets, users=users, meta=meta, max_results=max_results, next_token=result["meta"]["next_token"])
        return tweets, users, meta

    def get_tweet_conversation(self, 
                              conversation_id: str, 
                              user_info: bool = False, 
                              since_id: int|None = None, 
                              max_results: int = 100) -> tuple[list, list, list]:
        """Convenience function over the search API to retrieve all tweets in a twitter thread using the conversation ID.
        Parameters:
          - conversation_id: The id of the conversation defined in the tweet json body from the search or tweet API
          - user_info: Wether to return the name and username in the includes for all users in tweets.
          - since_id: If set to an ID, filter only tweet that happened after this ID
          - max_results: defaults to 100, can be changed to less than 100 (Twitter API limit)
        """
        query = f"conversation_id:{conversation_id}"
        tweets, users, meta = self.search_tweet(query, user_info=user_info, since_id=since_id, max_results=max_results)
        return tweets, users, meta
