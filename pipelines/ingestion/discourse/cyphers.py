from ...helpers import Cypher
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging

class DiscourseCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()

## add contraints


    ## do posts not have dates??
    @count_query_logging
    def create_posts(self, urls):
        count = 0 
        for url in urls:      
            query = f"""LOAD CSV WITH HEADERS FROM '{url}' AS posts
                        WITH posts WHERE posts.postUuid IS NOT NULL 
                        MERGE (post:Post:Discourse {{postUuid: posts.postUuid}})
                        ON CREATE SET
                            post:Ingest,
                            post.text = posts.text,
                            post.postNumber = tointeger(posts.postNumber),
                            post.title = posts.title, 
                            post.author = posts.author,
                            post.authorId = tointeger(posts.user_id),
                            post.tags = posts.tags, 
                            post.category = posts.category,
                            post.topicId = tointeger(posts.topic_id),
                            post.incomingLinkCount = tointeger(posts.incomingLinkCount),
                            post.readersCount = tointeger(posts.readersCount),
                            post.quoteCount = tointeger(posts.quoteCount), 
                            post.likes = tointeger(posts.likes), 
                            post.readsCount = tointeger(posts.readsCount), 
                            post.topicId = tointeger(posts.topicId),
                            post.url = posts.url,
                            post.trustLevel = post.trustLevel,
                            post.replyToPostNumber = tointeger(posts.replyToPostNumber),
                            post.publishedDt = posts.createdDtSource
                        ON MATCH SET
                            post:Ingest
                        RETURN COUNT(DISTINCT(post))"""
            count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def connect_posts_replies(self):
        count = 0 
        query = """
        MATCH (post:Discourse:Post)
        MATCH (otherpost:Discourse:Post)
        WHERE post.replyToPostNumber = otherpost.postNumber
        WITH post, otherpost
        MERGE (post)-[r:REPLY]->(otherpost)
        RETURN COUNT(DISTINCT(r))
        """
        count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def create_authors(self, urls):
        count = 0 
        for url in urls: 
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MERGE (user:Account:Discourse {{userId: rows.authorId}})
            ON CREATE SET
                user.username = rows.author
            ON MATCH SET
                user.username = rows.author
            RETURN COUNT(*)"""
            count += self.query(query)[0].value()
        return count 
    
    @count_query_logging 
    def connect_authors(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' as rows
            MATCH (user:Account:Discourse {{userId: rows.authorId}}) 
            MATCH (post:Post {{postUuid: rows.postUuid}})
            MERGE (user)-[r:PUBLISHED]->(post)
            RETURN COUNT(DISTINCT(r))
            """
            print(query)
            count += self.query(query)[0].value()
        return count 



    
def create_authors(self, urls):
    count = 0 
    for url in urls:
        query = f"""
        LOAD CSV WITH HEADERS FROM '{url}' AS users
        MERGE (user:Discourse:User:Ingest {{userId: users.userId}})
        ON CREATE SET
            user.userName = users.userName
        ON MATCH 
            SET user:Ingest
        RETURN COUNT(user)
        """
        count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def connect_authors(self):
        count = 0 
        query = """
        MATCH (author:Author)
        MATCH (post:Post)
        WHERE author.userId = post.authorId
        WITH author, post 
        MERGE (author)-[r:POSTED]->(post)
        RETURN COUNT(DISTINCT(r))
        """
        count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def create_categories(self, categories):
        count = 0 
        for category in categories:
            query = f"""
            merge (category:Discourse:Category {{name: '{category}'}})
        """
        count += self.query(query)[0].value()
        return count 
    
    @count_query_logging
    def connect_topics_posts(self):
        count = 0 
        query = """
        MATCH (topic:Discourse:Topic)
        MATCH (post:Discourse:Post)
        WHERE topic.name = post.topic
        MERGE (post)-[r:TOPIC]->(topic)
        RETURN COUNT(r)
        """
        count += self.query(query)[0].value()
        return count 

    def create_tags(self, tags):
        count = 0 
        for tag in tags:
            query = f"""merge (tag:Discourse:Tag {{name: '{tag}'}})"""
            count += self.query(query)[0].value()
        return count 

    def connect_tags_posts(self):
        count = 0 
        query = """
        MATCH (tag:Discourse:Tag)
        MATCH (post:Discourse:Post)
        WHERE tag.name = post.name 
        WITH tag, post
        MERGE (post)-[r:TAG]->(tag)
        RETURN COUNT(DISTINCT(r))
        """
        count += self.query(query)[0].value()
        return count 
    


## 
    # @count_query_logging
    # def create_categories(self):

    # @count_query_logging
    # def create_topics(self):

    # @count_query_logging
    # def connect_categories_topics(self):

    # @count_query_logging
    # def connect_posts_topics(self):

