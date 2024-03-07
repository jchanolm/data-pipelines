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

### Post

```
{
  "text": "Example post content.",
  "title": "Example Post Title",
  "author": "username",
  "tags": ["tag1", "tag2"],
  "category": "Example Category",
  "topic_id": 123,
  "url": "https://forum.arbitrum.foundation/t/123/1"
}```

### Response

```
{
  "text": "Example response content.",
  "author": "username",
  "parent_post_title": "Example Post Title",
  "parent_post_id": 123
}```
