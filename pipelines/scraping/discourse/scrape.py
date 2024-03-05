from ..helpers import Scraper
import logging 
import time
import os 
import requests as requests

from dotenv import load_dotenv
load_dotenv()


class DiscourseScraper(Scraper):
    def __init__(self):
        super().__init__()  # This calls the __init__ method of the Scraper class
        self.base_url = "https://forum.arbitrum.foundation"


    def fetch_categories(self):
        base_url = self.base_url
        url = f"{self.base_url}/categories.json"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch categories from {base_url} with Status Code: {response.status_code}")
            logging.error(f"Failed to fetch categories from {base_url}")
        else:
            category_data = response.json()
            categories = category_data.get('category_list', {}).get('categories', [])
            return categories 
        

    def fetch_topics(self, category_slug):
        topics = []
        page = 0 
        while True:
            url = f"{self.base_url}/c/{category_slug}.json?page={page}"
            response = requests.get(url)
            if response.status_code != 200: 
                break 
            topics_data = response.json()
            fetched_topics = topics_data.get("topic_list", {}).get("topics", [])
            if not fetched_topics:
                break 
            topics.extend(fetched_topics)
            page += 1
        return topics 
    
    def fetch_topic_details(self, topic_id):
        url = f"{self.base_url}/t/{topic_id}.json"
        response = requests.get(url)
        if response.status_code != 200:
            return None 
        return response.json()

    def organize_post_data(self, topic_details, category_name):
            posts_data = []
            posts = topic_details.get("post_stream", {}).get("posts", [])
            for post in posts:
                post_data = {
                    "text": post.get("cooked", ""),
                    "title": topic_details.get("title", ""),
                    "author": post.get("username", ""),
                    "tags": topic_details.get("tags", []),
                    "category": category_name,  # Include category name
                    "topic_id": topic_details.get("id"),  # Include topic ID
                    "url": f"{self.base_url}/t/{topic_details.get('id')}/{post.get('post_number')}"
                }
                # Find responses to this post
                responses = [response for response in posts if response.get("reply_to_post_number") == post.get("post_number")]
                post_data["responses"] = [{
                    "text": response.get("cooked", ""),
                    "author": response.get("username", "")
                } for response in responses]
                posts_data.append(post_data)
            return posts_data

    def ingest_data(self):
        print("Fetching categories....")
        categories = self.fetch_categories()
        all_posts_data = []
        for category in categories:
            category_slug = category.get('slug')
            category_name = category.get('name')
            print(f"Fetching posts for {category_name}.....")
            topics = self.fetch_topics(category_slug)
            for topic in topics:
                topic_id = topic.get('id')
                topic_details = self.fetch_topic_details(topic_id)
                if topic_details:
                    posts_data = self.organize_post_data(topic_details, category_name)
                    all_posts_data.extend(posts_data)
        return all_posts_data
    
    def extract_posts(self, posts_data):
            posts_without_responses = []
            for post in posts_data:
                post_copy = post.copy()  # Make a copy to avoid modifying the original data
                post_copy.pop('responses', None)  # Remove responses
                posts_without_responses.append(post_copy)
                return posts_without_responses

    def extract_post_responses(posts_data):
        responses = []
        for post in posts_data:
            for response in post.get('responses', []):
                response_copy = response.copy()  # Make a copy to avoid modifying the original data
                # Add a reference to the parent post
                response_copy['parent_post_title'] = post['title']
                response_copy['parent_post_id'] = post['topic_id']
                responses.append(response_copy)
        return responses
    
    def get_last_post_number(self, posts_data):
        last_post_number = max([post.get('post_number', 0) for post in posts_data])
        return last_post_number


    def run(self):
        print(f"Fetching all posts from {self.base_url}...")
        posts = self.ingest_data()
        self.data['posts'] = self.extract_posts(posts)
        self.data['responses'] = self.extract_post_responses(posts)
        self.metadata['last_post_number'] = self.get_last_post_number(posts)

        self.save_data_local()
        self.save_metadata()


if __name__ == "__main__":
    S = DiscourseScraper()
    S.run()
