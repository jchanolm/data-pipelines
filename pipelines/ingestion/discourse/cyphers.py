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
                        MERGE (post:Post:Discourse {{postUuid: posts.postUuid}})
                        ON CREATE SET
                            post:Ingest,
                            post.text = posts.text,
                            post.postNumber = posts.postNumber,
                            post.title = posts.title, 
                            post.author = posts.author,
                            post.authorId = posts.user_id,
                            post.tags = posts.tags, 
                            post.category = posts.category,
                            post.topicId = posts.topic_id,
                            post.incomingLinkCount = posts.incomingLinkCount,
                            post.readersCount = posts.readersCount,
                            post.quoteCount = posts.quoteCount, 
                            post.likes = posts.likes, 
                            post.readsCount = posts.readsCount, 
                            post.topicId = posts.topicId,
                            post.url = posts.url,
                            post.trustLevel = post.trustLevel,
                            post.replyToPostNumber = posts.replyToPostNumber,
                            post.createdDtSource = posts.createdDtSource
                        ON MATCH SET
                            post:Ingest
                        RETURN COUNT(DISTINCT(post))"""
            print(query)
            count += self.query(query)[0].value()
        return count 


    @count_query_logging
    def create_replies(self):
        count = 0 
        query = """
        MATCH (post:Discourse:Post)
        MATCH (otherPost:Discourse:Post)
        WHERE post.replyToPostNumber is NULL
        AND otherPost.replyToPostNumber = post.postNumber
        WITH post, otherPost
        MERGE (otherPost)-[r:REPLY]->(post)
        RETURN COUNT(DISTINCT(otherPost)) as repliesCreated
        """
        count += self.query(query)[0].value()
        return count 
    
def create_authors(self, urls):
    count = 0 
    for url in urls:
        query = """
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
    



    # @count_query_logging
    # def create_categories(self):

    # @count_query_logging
    # def create_topics(self):

    # @count_query_logging
    # def connect_categories_topics(self):

    # @count_query_logging
    # def connect_posts_topics(self):

