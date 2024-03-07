from ..helpers import Scraper
import logging 
import time
import os 
import requests as requests

## add call to look up topics?
###, title

## category
###

class DiscourseScraper(Scraper):
    def __init__(self, bucket_name="arbitrum-discourse", load_data=False):
        super().__init__(bucket_name=bucket_name, load_data=load_data)
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
    
    def fetch_topic_posts(self, topic_id):
        url = f"{self.base_url}/t/{topic_id}.json"
        response = requests.get(url)
        time.sleep(.05) ## semi-respectful pause between requests
        if response.status_code != 200:
            return None 
        return response.json()

    def process_post_data(self, topic_posts, category_name):
            posts_data = []
            posts = topic_posts.get("post_stream", {}).get("posts", [])
            for post in posts:
                id = post.get('id', '')
                topicId = topic_posts.get('id')
                postNumber = post.get('post_number', '')
                postUuid = f"{id}-{topicId}-{postNumber}"
                actions_summary = post.get('actions_summary', [])
                likes = 0
                for action in actions_summary:
                    if action.get('id') == 2:  # Assuming '2' identifies a 'like' action
                        likes = action.get('count', 0)
                        # likes = next((action['count'] for action in actions_summary if action.get('id') == 2), 0)
                postData = {
                    "postUuid": postUuid,
                    "postId": post.get("id"),
                    "text": post.get("cooked", ""),
                    "authorId": post.get("user_id"),
                    "trustLevel": post.get("trust_level"),
                    "createdDtSource": post.get("created_at"),
                    "postNumber": post.get("post_number"),
                    "incomingLinkCount": post.get("incoming_link_count"),
                    "quoteCount": post.get("quote_count"),
                    "likes": likes, 
                    "readersCount": post.get("readers_count"),
                    "quoteCount": post.get("quote_count"),
                    "readsCount": post.get("reads"),
                    "replyToPostNumber": post.get("reply_to_post_number"),
                    "title": topic_posts.get("title", ""),
                    "author": post.get("display_username", ""),
                    "tags": topic_posts.get("tags", []),
                    "category": category_name,  
                    "topicId": topic_posts.get("topic_id"),  
                    "url": f"{self.base_url}/t/{topic_posts.get('id')}/{post.get('post_number')}"
                }
                posts_data.append(postData)
            return posts_data

    def fetch_posts(self):
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
                topic_details = self.fetch_topic_posts(topic_id)
                if topic_details:
                    posts_data = self.process_post_data(topic_details, category_name)
                    all_posts_data.extend(posts_data)
                    print(f"Scraped {len(all_posts_data)} records....")
        return all_posts_data
    
    
    def get_last_post_number(self, posts_data):
        last_post_number = max([post.get('post_number', 0) for post in posts_data])
        return last_post_number


    def run(self):
        print(f"Fetching all posts from {self.base_url}...")
        posts = self.fetch_posts()
        self.data['posts'] = posts
        self.metadata['last_post_number'] = self.get_last_post_number(posts)

        self.save_data()
        self.save_metadata()


if __name__ == "__main__":
    S = DiscourseScraper()
    S.run()
