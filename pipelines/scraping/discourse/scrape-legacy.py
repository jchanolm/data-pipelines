from ..helpers import Scraper
import logging
import time
import os
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DiscourseScraper(Scraper):
    def __init__(self):
        super().__init__(module_name='discourse')
        self.base_url = "https://forum.arbitrum.foundation"
        logging.info("Initialized DiscourseScraper")

    def fetch_categories(self):
        url = f"{self.base_url}/categories.json"
        response = requests.get(url)
        if response.status_code != 200:
            logging.error(f"Failed to fetch categories from {self.base_url} with Status Code: {response.status_code}")
            return None
        category_data = response.json()
        return category_data.get('category_list', {}).get('categories', [])

    def fetch_topics(self, category_slug):
        topics = []
        page = 0
        while True:
            url = f"{self.base_url}/c/{category_slug}.json?page={page}"
            response = requests.get(url)
            if response.status_code != 200:
                logging.error(f"Failed to fetch topics for category {category_slug} with status code {response.status_code}")
                break
            topics_data = response.json()
            fetched_topics = topics_data.get("topic_list", {}).get("topics", [])
            if not fetched_topics:
                break
            topics.extend(fetched_topics)
            page += 1
        return topics

    def fetch_topic_posts(self, topic_id):
        url = f"{self.base_url}/t/{topic_id}.json"
        response = requests.get(url)
        time.sleep(.1)  # semi-respectful pause between requests
        if response.status_code != 200:
            logging.error(f"Failed to fetch posts for topic {topic_id} with status code {response.status_code}")
            return None
        return response.json()

    def organize_post_data(self, topic_posts, category_name):
        posts_data = []
        posts = topic_posts.get("post_stream", {}).get("posts", [])
        for post in posts:
            post_data = {
                "text": post.get("cooked", ""),
                "title": topic_posts.get("title", ""),
                "author": post.get("username", ""),
                "tags": topic_posts.get("tags", []),
                "category": category_name,
                "topic_id": topic_posts.get("id"),
                "url": f"{self.base_url}/t/{topic_posts.get('id')}/{post.get('post_number')}"
            }
            responses = [response for response in posts if response.get("reply_to_post_number") == post.get("post_number")]
            post_data["responses"] = [{
                "text": response.get("cooked", ""),
                "author": response.get("username", "")
            } for response in responses]
            posts_data.append(post_data)
        return posts_data

    def fetch_posts(self):
        logging.info("Fetching categories...")
        categories = self.fetch_categories()
        if not categories:
            logging.error("No categories fetched.")
            return []
        all_posts_data = []
        for category in categories:
            category_slug = category.get('slug')
            category_name = category.get('name')
            logging.info(f"Fetching posts for {category_name}...")
            topics = self.fetch_topics(category_slug)
            for topic in topics:
                topic_id = topic.get('id')
                topic_details = self.fetch_topic_posts(topic_id)
                if topic_details:
                    posts_data = self.organize_post_data(topic_details, category_name)
                    all_posts_data.extend(posts_data)
                    logging.info(f"Scraped {len(all_posts_data)} records...")
        return all_posts_data

    def run(self):
        logging.info(f"Fetching all posts from {self.base_url}...")
        posts = self.fetch_posts()
        if posts:
            self.data['posts'] = self.extract_posts(posts)
            self.data['responses'] = self.extract_post_responses(posts)
            self.metadata['last_post_number'] = self.get_last_post_number(posts)
            self.save_data_local()
            self.save_metadata_local()
        else:
            logging.info("No posts were fetched.")

if __name__ == "__main__":
    S = DiscourseScraper()
    S.run()
