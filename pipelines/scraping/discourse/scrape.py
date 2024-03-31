from ..helpers import Scraper
import logging 
import time
import os 
import json
import requests as requests


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
    
    
    def get_last_post_number(self, posts_data):
        last_post_number = max([post.get('post_number', 0) for post in posts_data])
        return last_post_number
    
    def fetch_latest_posts(self):
        posts = []
        last_post_id = 10000  # Initialize with None to start fetching from the latest
        attempt = 0
        max_attempts = 5  # Maximum number of retry attempts
        delay = 10  # Initial delay in seconds before retrying in case of rate limit or error
        post_count = 0  # Initialize post count

        while attempt < max_attempts:
            try:
                url = f"{self.base_url}/posts.json"
                if last_post_id:
                    url += f"?before={last_post_id}"

                response = requests.get(url)
                time.sleep(1)  # Short pause to be polite to the server
                logging.info(f"Attempt {attempt + 1} to fetch posts...")

                if response.status_code == 200:
                    latest_posts = response.json().get('latest_posts', [])
                    if not latest_posts:
                        logging.info("No more posts to fetch.")
                        break  # Exit the loop if no more posts are returned

                    posts.extend(latest_posts)
                    post_count += len(latest_posts)
                    logging.info(f"Captured {len(posts)} posts...")
                    last_post_id = latest_posts[-1].get('id')  # Update last_post_id for pagination
                    attempt = 0  # Reset attempt counter after a successful fetch

                    # Save posts in batches of 5000
                    if post_count >= 5000:
                        file_name = f"{post_count}.json"
                        with open(file_name, 'w') as file:
                            json.dump(posts, file)
                        logging.info(f"Saved {len(posts)} posts to {file_name}")
                        posts = []  # Reset posts list after saving
                        post_count = 0  # Reset post count after saving

                elif response.status_code == 429:
                    logging.info("Rate limit encountered. Waiting before retrying...")
                    time.sleep(delay)
                    delay *= 2  # Exponential back-off for retry delay
                    attempt += 1
                else:
                    logging.error(f"Error fetching posts: Status {response.status_code}. Will try again...")
                    attempt += 1

            except Exception as e:
                logging.error(f"An exception occurred: {str(e)}. Attempting to retry...")
                attempt += 1
                time.sleep(delay)
                delay *= 2  # Exponential back-off for retry delay

            if attempt >= max_attempts:
                logging.error("Maximum attempts reached. Some posts may not have been fetched.")
                if posts:  # Check if there are any posts left to save
                    file_name = f"{post_count}.json"
                    with open(file_name, 'w') as file:
                        json.dump(posts, file)
                    logging.info(f"Saved the remaining {len(posts)} posts to {file_name}")
                break

        return posts

    def process_post_data(self, topic_posts, category_name):
        posts_data = []
        if not isinstance(topic_posts, dict):
            logging.error("Expected topic_posts to be a dict, but got a list. Check the data source or calling function.")
            return posts_data  # Return an empty list or handle as needed

        # Continue with your logic assuming topic_posts is a dict
        if 'post_stream' in topic_posts and 'posts' in topic_posts['post_stream']:
            for post in topic_posts['post_stream']['posts']:
                id = post.get('id', '')
                topicId = topic_posts.get('id')
                postNumber = post.get('post_number', '')
                postUuid = f"{id}-{topicId}-{postNumber}"
                actions_summary = post.get('actions_summary', [])
                likes = 0
                for action in actions_summary:
                    if action.get('id') == 2:  # Assuming '2' identifies a 'like' action
                        likes = action.get('count', 0)
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
                self.data['posts'] = posts_data 


        return posts_data


    def run(self):
        print(f"Fetching all posts from {self.base_url}...")
        posts = self.fetch_latest_posts()
        self.data['posts'] = posts
        print(self.data['posts'].keys())

        self.save_data()
        self.save_metadata()


if __name__ == "__main__":
    S = DiscourseScraper()
    S.run()
