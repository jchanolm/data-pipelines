# Discourse Scraper README

## Overview
This script, `DiscourseScraper`, automates the extraction of posts and their responses from the Arbitrum Discourse forum. Designed for seamless integration and ease of use, it fetches categories, topics, and individual posts, organizing the data into a structured format.

## Data
- Datafile: https://arb-grants-data-arbitrum-discourse.s3.us-east-2.amazonaws.com/data_2024-3-7_.json

## Features
- **Category Fetching:** Retrieves all forum categories.
- **Topic Fetching:** Collects topics under each category.
- **Post Fetching:** Extracts posts from each topic, including post content, author, tags, and more.
- **Response Organization:** Identifies and collates responses to posts.

## Usage

### Fetching Posts and Responses
The main function to use is `run()`, which orchestrates the entire scraping process.

```python
if __name__ == "__main__":
    scraper = DiscourseScraper()
    scraper.run()
```

### Post Object

```
post = {
    'postUuid': topicId + postId + postNumber, 
    'postId': int, 
    'title': str, 
    "text": str, 
    "authorId": int,
    "author": str, 
    "trustLevel": str,
    postNumber: str, 
    "incomingLinkCount: int, 
    "quoteCount"; int, 
    "likes": int, 
    "readersCount": int, 
    "quoteCount: int, 
    "readsCount": int, 
    "replyToPostNumber": str[],
    "author": str, 
    "tags": str[],
    "category": str,
    "topicId": int, 
    "url": str
}
```

