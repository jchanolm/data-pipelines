from tqdm import tqdm
from ...helpers import Cypher, Indexes
from ...helpers import get_query_logging, count_query_logging

class GithubCypher(Cypher):
    def __init__(self) -> None:
        super().__init__()
    
    def create_indexes(self):
        indexes = Indexes()
        indexes.accounts()
        query = "CREATE INDEX GithubRepository IF NOT EXISTS FOR (n:Repository) ON (n.full_name)"
        self.query(query)

    @get_query_logging
    def get_missing_github_accounts(self):
        query = f"""
            MATCH (github:Github:Account)
            WHERE (github.lastMetadataUpdateDt IS NULL OR github.lastMetadataUpdateDt < datetime() - duration({{days:30}}))
            AND NOT github:BadHandle
            RETURN github.handle as handle
        """
        handles = self.query(query)
        return handles

    @get_query_logging
    def get_known_github_accounts(self):
        query = f"""
            MATCH (github:Github:Account)
            WHERE (github.lastMetadataUpdateDt IS NOT NULL AND github.lastMetadataUpdateDt > datetime() - duration({{days:30}}))
            RETURN github.handle as handle
        """
        handles = [el["handle"] for el in self.query(query)]
        return handles

    @get_query_logging
    def get_known_github_repositories(self):
        query = f"""
            MATCH (github:Github:Repository)
            WHERE (github.lastMetadataUpdateDt IS NOT NULL AND github.lastMetadataUpdateDt > datetime() - duration({{days:30}}))
            RETURN github.full_name as full_name
        """
        full_names = [el["full_name"] for el in self.query(query)]
        return full_names

    @get_query_logging
    def get_github_repositories(self):
        query = f"""
            MATCH (github:Github:Repository)
            RETURN github.full_name as full_name
        """
        full_names = [el["full_name"] for el in self.query(query)]
        return full_names

    @count_query_logging
    def create_or_merge_users(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                WITH data
                WHERE data.login IS NOT NULL
                MERGE (user:Github:Account {{handle: toLower(data.login)}})
                ON CREATE SET   user.uuid = apoc.create.uuid(),
                                user.id = data.id,
                                user.avatar_url = data.avatar_url,
                                user.html_url = data.html_url,
                                user.name = data.name,
                                user.company = data.company,
                                user.blog = data.blog,
                                user.location = data.location,
                                user.email = data.email,
                                user.hireable = data.hireable,
                                user.bio = data.bio,
                                user.twitter_username = data.twitter_username,
                                user.public_repos = toInteger(data.public_repos),
                                user.public_gists = toInteger(data.public_gists),
                                user.followers = toInteger(data.followers),
                                user.following = toInteger(data.following),
                                user.created_at = datetime(data.created_at),
                                user.updated_at = datetime(data.updated_at),
                                user.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                user.lastMetadataUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                user.ingestedBy = "{self.CREATED_ID}"
                ON MATCH SET    user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                user.avatar_url = data.avatar_url,
                                user.html_url = data.html_url,
                                user.name = data.name,
                                user.company = data.company,
                                user.blog = data.blog,
                                user.location = data.location,
                                user.email = data.email,
                                user.hireable = data.hireable,
                                user.bio = data.bio,
                                user.twitter_username = data.twitter_username,
                                user.public_repos = toInteger(data.public_repos),
                                user.public_gists = toInteger(data.public_gists),
                                user.followers = toInteger(data.followers),
                                user.following = toInteger(data.following),
                                user.updated_at = datetime(data.updated_at),
                                user.lastMetadataUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                user.ingestedBy = "{self.UPDATED_ID}"
                RETURN count(user)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_repositories(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                WITH data
                WHERE data.full_name IS NOT NULL
                MERGE (repo:Github:Repository {{full_name: toLower(data.full_name)}})
                ON CREATE SET   repo.uuid = apoc.create.uuid(),
                                repo.id = data.id,
                                repo.name = data.name,
                                repo.private = data.private,
                                repo.owner = data.owner,
                                repo.html_url = data.html_url,
                                repo.description = data.description,
                                repo.fork = data.fork,
                                repo.created_at = datetime(data.created_at),
                                repo.updated_at = datetime(data.updated_at),
                                repo.pushed_at = datetime(data.pushed_at),
                                repo.homepage = data.homepage,
                                repo.size = data.size,
                                repo.stargazers_count = toInteger(data.stargazers_count),
                                repo.watchers_count = toInteger(data.watchers_count),
                                repo.language = data.language,
                                repo.languages = toList(data.languages),
                                repo.has_issues = data.has_issues,
                                repo.has_projects = data.has_projects,
                                repo.has_downloads = data.has_downloads,
                                repo.has_wiki = data.has_wiki,
                                repo.has_pages = data.has_pages,
                                repo.has_discussions = data.has_discussions,
                                repo.forks_count = toInteger(data.forks_count),
                                repo.mirror_url = data.mirror_url,
                                repo.archived = data.archived,
                                repo.disabled = data.disabled,
                                repo.open_issues_count = toInteger(data.open_issues_count),
                                repo.license = data.license,
                                repo.allow_forking = data.allow_forking,
                                repo.is_template = data.is_template,
                                repo.web_commit_signoff_required = data.web_commit_signoff_required,
                                repo.topics = data.topics,
                                repo.visibility = data.visibility,
                                repo.forks = toInteger(data.forks),
                                repo.open_issues = toInteger(data.open_issues),
                                repo.watchers = toInteger(data.watchers),
                                repo.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                repo.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                repo.ingestedBy = "{self.CREATED_ID}"
                ON MATCH SET    repo.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                repo.name = data.name,
                                repo.private = data.private,
                                repo.owner = data.owner,
                                repo.html_url = data.html_url,
                                repo.description = data.description,
                                repo.fork = data.fork,
                                repo.created_at = datetime(data.created_at),
                                repo.updated_at = datetime(data.updated_at),
                                repo.pushed_at = datetime(data.pushed_at),
                                repo.homepage = data.homepage,
                                repo.size = data.size,
                                repo.stargazers_count = toInteger(data.stargazers_count),
                                repo.watchers_count = toInteger(data.watchers_count),
                                repo.language = data.language,
                                repo.languages = toList(data.languages),
                                repo.has_issues = data.has_issues,
                                repo.has_projects = data.has_projects,
                                repo.has_downloads = data.has_downloads,
                                repo.has_wiki = data.has_wiki,
                                repo.has_pages = data.has_pages,
                                repo.has_discussions = data.has_discussions,
                                repo.forks_count = toInteger(data.forks_count),
                                repo.mirror_url = data.mirror_url,
                                repo.archived = data.archived,
                                repo.disabled = data.disabled,
                                repo.open_issues_count = toInteger(data.open_issues_count),
                                repo.license = data.license,
                                repo.allow_forking = data.allow_forking,
                                repo.is_template = data.is_template,
                                repo.web_commit_signoff_required = data.web_commit_signoff_required,
                                repo.topics = data.topics,
                                repo.visibility = data.visibility,
                                repo.forks = toInteger(data.forks),
                                repo.open_issues = toInteger(data.open_issues),
                                repo.watchers = toInteger(data.watchers),
                                repo.ingestedBy = "{self.UPDATED_ID}"
                RETURN count(repo)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging ## lazy, need to change full_name to name
    def set_name(self):
        count = self.query("""match (github:Github:Repository) set github.name = github.full_name return count(*))""")
        return count 
    

    @count_query_logging
    def add_repositories_languages(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MATCH (repo:Github:Repository {{full_name: toLower(data.full_name)}})
                SET repo.languages = data.languages
                RETURN count(repo)
            """

            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_followers(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                WITH data
                WHERE data.handle IS NOT NULL AND data.follower IS NOT NULL
                MATCH (handle:Github:Account {{handle: toLower(data.handle)}})
                MATCH (follower:Github:Account {{handle: toLower(data.follower)}})
                MERGE (follower)-[edge:FOLLOWS]->(handle)
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_owners(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                WITH data
                WHERE data.owner IS NOT NULL AND data.full_name IS NOT NULL
                MATCH (handle:Github:Account {{handle: toLower(data.owner)}})
                MATCH (repo:Github:Repository {{full_name: toLower(data.full_name)}})
                MERGE (handle)-[edge:OWNER]->(repo)
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_contributors(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                WITH data
                WHERE data.contributor IS NOT NULL AND data.full_name IS NOT NULL
                MATCH (handle:Github:Account {{handle: toLower(data.contributor)}})
                MATCH (repo:Github:Repository {{full_name: toLower(data.full_name)}})
                MERGE (handle)-[edge:CONTRIBUTOR]->(repo)
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_subscribers(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                WITH data
                WHERE data.subscriber IS NOT NULL AND data.full_name IS NOT NULL
                MATCH (handle:Github:Account {{handle: toLower(data.subscriber)}})
                MATCH (repo:Github:Repository {{full_name: toLower(data.full_name)}})
                MERGE (handle)-[edge:SUBSCRIBER]->(repo)
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_email(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                WITH data
                WHERE data.email IS NOT NULL
                MERGE (email:Email:Account {{email: toLower(data.email)}})    
                RETURN count(email)
            """
            count += self.query(query)[0].value()
        return count


    @count_query_logging
    def link_email(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                WITH data
                WHERE data.email IS NOT NULL AND data.handle IS NOT NULL
                MATCH (user:Github:Account {{handle: toLower(data.handle)}})    
                MATCH (email:Email:Account {{email: toLower(data.email)}})
                MERGE (user)-[edge:HAS_ACCOUNT]->(email)
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_twitter(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                WITH data
                WHERE data.twitter IS NOT NULL
                MERGE (twitter:Twitter:Account {{handle: toLower(data.twitter)}})
                RETURN count(twitter)
            """
        count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_twitter(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                WITH data
                WHERE data.handle IS NOT NULL AND data.twitter IS NOT NULL
                MATCH (user:Github:Account {{handle: toLower(data.handle)}})    
                MATCH (twitter:Twitter:Account {{handle: toLower(data.twitter)}})
                MERGE (user)-[edge:HAS_ACCOUNT]->(twitter)
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def flag_bad_handles(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                WITH data
                WHERE data.handle IS NOT NULL
                MATCH (user:Github:Account {{handle: toLower(data.handle)}})
                SET user:BadHandle    
                RETURN count(user)
            """
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def clean_handles(self):
        count = 0 
        query_remove_https = """
        match (g:Github) where g.handle contains "https://"
        set g.handle = replace(g.handle, 'https://', "")
        return count(g)
        """
        count += self.query(query_remove_https)[0].value()

        query_remove_github_domain = """
        MATCH (g:Github) SET g.handle = REPLACE(g.handle, "github.com/", "")
        RETURN COUNT(g)
        """
        count += self.query(query_remove_github_domain)[0].value()

        query_remove_slashes = """
        MATCH (g:Github)
        WHERE g.handle contains "/"
        WITH g, split(g.handle, '/')[0] as handle_fragment
        WITH g, replace(handle_fragment, "/", "") as cleaned_handle
        SET g.handle = cleaned_handle
        RETURN COUNT(DISTINCT(g))
        """
        count += self.query(query_remove_slashes)[0].value()
        
        return count 