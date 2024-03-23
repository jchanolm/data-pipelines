from datetime import datetime, timezone
import logging
import os
import time
from tqdm import tqdm
from .cyphers import GithubCypher
from ..helpers import Processor
import numpy as np
from requests.models import Response

DEBUG = os.environ.get("DEBUG", False)

class GithubProcessor(Processor):
    def __init__(self):
        self.github_tokens = [key.strip() for key in os.environ.get("GITHUB_API_KEY", "").split(",")]
        self.cyphers = GithubCypher()
        
        self.user_keys = ["id", "login", "avatar_url", "html_url", "name", "company", "blog", "location", 
                          "email", "hireable", "bio", "twitter_username", "public_repos", "public_gists", "followers", "following", 
                          "created_at", "updated_at"]
        self.contributor_keys = ["login", "contributions"]
        self.repository_keys = ["id","name","full_name","private","owner","html_url","description","fork","created_at","updated_at",
                                "pushed_at","homepage","size","stargazers_count","watchers_count","language", "languages", 
                                "has_issues","has_projects","has_downloads","has_wiki","has_pages","has_discussions","forks_count","mirror_url",
                                "archived","disabled","open_issues_count","license","allow_forking","is_template","web_commit_signoff_required","topics",
                                "visibility","forks","open_issues","watchers"]
        self.known_users = set()
        self.known_repos = set()
        
        self.chunk_size = 10
        super().__init__("github-processing")

    def init_data(self):
        self.data = {}
        self.data["users"] = {}
        self.data['bad_handles'] = []
        self.data["repositories"] = {}

    def get_headers(self):
        token = np.random.choice(self.github_tokens)
        github_headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28"
        }
        return github_headers

    def get_all_handles(self):
        handles = self.cyphers.get_missing_github_accounts()
        handles = [el["handle"] for el in handles]
        if DEBUG:
            handles = handles[:100]
        return handles
    
    def get_known_handles(self):
        handles = self.cyphers.get_known_github_accounts()
        return handles

    def get_known_repositories(self):
        full_names = self.cyphers.get_known_github_repositories()
        return full_names

    ##########################################
    #                  USERS                 #
    ##########################################

    def check_api_rate_limit(self, response):
        if not type(response) == Response:
            return True
        remaining = int(response.headers["X-RateLimit-Remaining"])
        if remaining <= 0:
            to_sleep = float(response.headers["X-RateLimit-Reset"]) - datetime.now(timezone.utc).timestamp()
            logging.info(f"API Rate limit exceeded sleeping for: {to_sleep}")
            time.sleep(int(to_sleep))
            return False
        return True

    def get_followers(self, handle, followers_data = [], page=1, counter=0):
        time.sleep(counter)
        if counter > 10  or page > 10:
            return followers_data
        url = f"https://api.github.com/users/{handle}/followers"
        params = {
            "per_page":100,
            "page":page
        }
        response = self.get_request(url, params=params, headers=self.get_headers(), decode=False, json=False, ignore_retries=True) 
        if response and not self.check_api_rate_limit(response):
            return self.get_followers(handle, followers_data=followers_data, page=page)
        
        if not response:
            return followers_data
        try:
            followers_raw_data = response.json()
        except:
            logging.error(f"Error getting followers: {response}")
            return self.get_followers(handle, counter=counter+1)
        if followers_raw_data and type(followers_raw_data) == list and len(followers_raw_data) > 0:
            follower_pbar = tqdm(followers_raw_data, desc="Getting followers information", position=2, leave=False)
            for follower in follower_pbar:
                if "login" in follower:
                    follower_pbar.set_description("Getting followers information: " +  follower["login"])
                    self.get_user_data(follower["login"], follow_through=False)
                    followers_data.append(follower["login"])
            return self.get_followers(handle, followers_data=followers_data, page=page+1)
        return followers_data

    def get_user_data(self, handle, follow_through=True, counter=0):
        time.sleep(counter)
        if counter > 10:
            return None
        if handle in self.data["users"] or handle in self.known_users:
            return None
        url = f"https://api.github.com/users/{handle}"
        response = self.get_request(url, headers=self.get_headers(), decode=False, json=False, ignore_retries=True)
        if response and not self.check_api_rate_limit(response):
            return self.get_user_data(handle, follow_through=follow_through)
        if response:
            try:
                user_raw_data = response.json()
            except:
                logging.error(f"Error getting user data: {response}")
                return self.get_user_data(handle, follow_through=follow_through, counter=counter+1)
            if user_raw_data and type(user_raw_data) == dict:
                user_data = {key: value for (key, value) in user_raw_data.items() if key in self.user_keys}
                if follow_through:
                    followers = self.get_followers(handle)
                    user_data["followers_handles"] = followers
                    repositories = self.get_user_repos(handle)
                    user_data["repositories"] = repositories
                self.data["users"][handle] = user_data
        else:
            self.data["bad_handles"].append(handle)

    def get_user_repos(self, handle, counter=0):
        time.sleep(counter)
        if counter > 10:
            return []
        url = f"https://api.github.com/users/{handle}/repos"
        response = self.get_request(url, headers=self.get_headers(), decode=False, json=False, ignore_retries=True)
        if response and not self.check_api_rate_limit(response):
            return self.get_user_repos(handle)
        repositories = []
        if not response:
            return repositories
        try:
            repos_raw_data = response.json()
        except:
            logging.error(f"Error getting user's repos: {response}")
            return self.get_user_repos(handle, counter=counter+1)
        if repos_raw_data and type(repos_raw_data) == list:
            repo_pbar = tqdm(repos_raw_data, position=1, desc="Getting repository information", leave=False)
            for repo in repo_pbar:
                if "full_name" in repo and repo["full_name"]:
                    repo_pbar.set_description(desc="Getting repository information: " + repo["full_name"])
                    self.get_repository_data(repo["full_name"])
            repositories = [repo["full_name"] for repo in repos_raw_data]
        return repositories

    ##########################################
    #                  REPOS                 #
    ##########################################

    def get_contributors(self, repository, contributors_data = [], page=1, counter=0):
        self.cyphers.clean_handles()
        time.sleep(counter)
        if counter > 10 or page > 10:
            return contributors_data
        url = f"https://api.github.com/repos/{repository}/contributors"
        params = {
            "per_page":100,
            "page":page
        }
        response = self.get_request(url, params=params, headers=self.get_headers(), decode=False, json=False, ignore_retries=True)
        if response and not self.check_api_rate_limit(response):
            return self.get_contributors(repository, contributors_data=contributors_data, page=page)
        if not response:
            return contributors_data
        try:
            contributors_raw_data = response.json()
        except:
            logging.error(f"Error getting contributors: {response}")
            return self.get_contributors(repository, contributors_data=contributors_data, page=page, counter=counter+1)
        if contributors_raw_data and type(contributors_raw_data) == list and len(contributors_raw_data)>0:
            cont_pbar = tqdm(contributors_raw_data, desc="Getting contributors information", position=2, leave=False)
            for contributor in cont_pbar:
                if "login" in contributor:
                    cont_pbar.set_description(desc="Getting contributors information: " + contributor["login"])
                    self.get_user_data(contributor["login"], follow_through=False)
                    contributor = {key: value for (key, value) in contributor.items() if key in self.contributor_keys}
                    contributors_data.append(contributor["login"])
            return self.get_contributors(repository, contributors_data=contributors_data, page=page+1, counter=0)
        return contributors_data

    def get_subscribers(self, repository, page=1, subscribers_data = [], counter=0):
        time.sleep(counter)
        if counter > 10 or page > 10:
            return subscribers_data
        url = f"https://api.github.com/repos/{repository}/subscribers"
        params = {
            "per_page":100,
            "page":page
        }
        try:
            response = self.get_request(url, params=params, headers=self.get_headers(), decode=False, json=False, ignore_retries=True)

    # Check if response is meant to be JSON (assuming json parameter affects how
            if response and not self.check_api_rate_limit(response):
                return self.get_subscribers(repository, subscribers_data=subscribers_data, page=page)
            if not response:
                return subscribers_data
            try:
                subscribers_raw_data = response.json()
            except:
                logging.error(f"Error getting subscribers: {response}")
                return self.get_subscribers(repository, subscribers_data=subscribers_data, page=page, counter=counter+1)
            if subscribers_raw_data and type(subscribers_raw_data) == list and len(subscribers_raw_data) > 0:
                sub_pbar = tqdm(subscribers_raw_data, desc="Getting subscribers information", position=2, leave=False)
                for subscriber in sub_pbar:
                    if "login" in subscriber:
                        sub_pbar.set_description(desc="Getting subscribers information: " + subscriber["login"])
                        self.get_user_data(subscriber["login"], follow_through=False)
                        subscribers_data.append(subscriber["login"])
                return self.get_subscribers(repository, subscribers_data=subscribers_data, page=page+1)
            return subscribers_data
        except Exception as e:
            logging.error(f"Error retrieving subscribers :( {e}")
            pass

    def get_repo_readme(self, repository):
        url = f"https://api.github.com/repos/{repository}/readme"
        readme_data = self.get_request(url, headers=self.get_headers(), decode=False, json=True, ignore_retries=True)
        if readme_data and type(readme_data) == dict and "download_url" in readme_data:
            readme_file = self.get_request(readme_data["download_url"], decode=True, ignore_retries=True)
            if readme_file:
                return readme_file
            else:
                return ""
        else:
            return ""
    def handle_repository(self, repository, repos_raw_data):
        repos_raw_data["languages"] = self.get_repository_languages(repository)
        if "owner" in repos_raw_data:
            if repos_raw_data["owner"]:
                repos_raw_data["owner"] = repos_raw_data["owner"].get("login", None)
            else:
                repos_raw_data["owner"] = None
        else:
            repos_raw_data["owner"] = None
        if "license" in repos_raw_data:
            if repos_raw_data["license"]:
                repos_raw_data["license"] = repos_raw_data["license"].get("name", None)
            else:
                repos_raw_data["license"] = None
        else:
            repos_raw_data["license"] = None
        if "parent" in repos_raw_data and repos_raw_data["parent"]["full_name"]:
            self.get_repository_data(repos_raw_data["parent"]["full_name"])
            repos_raw_data["parent"] = repos_raw_data["parent"]["full_name"]
        repos_data = {key: value for (key, value) in repos_raw_data.items() if key in self.repository_keys}
        contributors = self.get_contributors(repository)
        subscribers = self.get_subscribers(repository)
        readme = self.get_repo_readme(repository)
        repos_data["contributors_handles"] = contributors
        repos_data["subscribers_handles"] = subscribers 
        repos_data["readme"] = readme
        self.data["repositories"][repository] = repos_data

    def get_repository_data(self, repository, counter=0):
        time.sleep(counter)
        if counter > 10:
            return None
        if repository in self.data["repositories"] or repository in self.known_repos:
            return self.data["repositories"][repository]
        url = f"https://api.github.com/repos/{repository}"
        response = self.get_request(url, headers=self.get_headers(), decode=False, json=False, ignore_retries=True)
        if response and not self.check_api_rate_limit(response):
            return self.get_repository_data(repository)
        if response:
            try:
                repos_raw_data = response.json()
            except:
                logging.error(f"Error getting repository: {response}")
                return self.get_repository_data(repository, counter=counter+1)
            if repos_raw_data and type(repos_raw_data) == dict:
                self.handle_repository(repository, repos_raw_data)
    
    def get_repository_languages(self, repository):
        url = f"https://api.github.com/repos/{repository}/languages"
        response = self.get_request(url, headers=self.get_headers(), decode=False, json=False, ignore_retries=True)
        if response and not self.check_api_rate_limit(response):
            return self.get_repository_languages(repository)
        if response:
            languages = [key for key in response.json()]
        else:
            languages = []
        return languages

    def process_github_accounts(self, handles):
        self.parallel_process(self.get_user_data, handles, "Getting users accounts information")

    def github_repo_search(self, url, params, page=1, counter=0):
        time.sleep(counter)
        if counter > 10 or page > 10:
            return None
        params["page"] = page
        response = self.get_request(url, params, headers=self.get_headers(), decode=False, json=False)
        if response and not self.check_api_rate_limit(response):
            return self.github_repo_search(url, params, page=page, counter = counter+1)
        try:
            data = response.json()
            data = data["items"]
        except:
            logging.error(f"Error in search query: {response}")
            return self.github_repo_search(url, params, page=page, counter = counter+1)
        repo_pbar = tqdm(data, position=1, desc="Getting repository information", leave=False)
        for repo in repo_pbar:
            if repo and type(repo) == dict:
                repo_pbar.set_description(desc="Getting repository information: " + repo["full_name"])
                self.handle_repository(repo["full_name"], repo)
        return self.github_repo_search(url, params, page=page+1, counter = 0)

    def get_solidity_repos(self):
        sorting = ["stars", "forks", "help-wanted-issues", "updated"]
        url = "https://api.github.com/search/repositories"
        params = {
            "q":"language:solidity",
            "per_page":100,
        }
        for sort in sorting:
            logging.info(f"Getting top 1000 repos for: {sort}")
            params["sort"] = sort
            self.github_repo_search(url, params)

    def update_repositories_languages(self):
        repos = self.cyphers.get_github_repositories()
        repos = list(set(repos))
        if DEBUG:
            repos = repos[:10]
        data = self.parallel_process(self.get_repository_languages, repos, description="Getting repositories languages")
        results = []
        for repo, languages in zip(repos, data):
            results.append({
                "full_name": repo,
                "languages": languages
            })
        urls = self.save_json_as_csv(results,  f"processor_repos_language_{self.asOf}")
        self.cyphers.add_repositories_languages(urls)

    def ingest_github_data(self, recover=False):
        if recover:
            handles_urls = self.get_files_urls_from_s3( "processor_bad_handles_")
            users_urls = self.get_files_urls_from_s3( "processor_users_")
            emails_urls = self.get_files_urls_from_s3( "processor_emails_")
            twitters_urls = self.get_files_urls_from_s3( "processor_twitters_")
            repositories_urls = self.get_files_urls_from_s3( "processor_repositories_")
            followers_urls = self.get_files_urls_from_s3( "processor_followers_")
            contributors_urls = self.get_files_urls_from_s3( "processor_contributors_")
            subscribers_urls = self.get_files_urls_from_s3( "processor_subscribers_")
        else:
            bad_handles = [{"handle": handle} for handle in self.data["bad_handles"] if handle]
            handles_urls = self.save_json_as_csv(bad_handles,  f"processor_bad_handles_{self.asOf}")

            users = [self.data["users"][handle] for handle in self.data["users"]]
            users_urls = self.save_json_as_csv(users,  f"processor_users_{self.asOf}")

            emails = [{"handle": user["login"], "email": user["email"]} for user in users if "email" in user and user["email"]]
            emails_urls = self.save_json_as_csv(emails,  f"processor_emails_{self.asOf}")

            twitters = [{"handle": user["login"], "twitter":user["twitter_username"]} for user in users if "twitter_username" in user and user["twitter_username"]]
            twitters_urls = self.save_json_as_csv(twitters,  f"processor_twitters_{self.asOf}")

            repositories = [self.data["repositories"][handle] for handle in self.data["repositories"]]
            repositories_urls = self.save_json_as_csv(repositories,  f"processor_repositories_{self.asOf}")

            followers = []
            for user in users:
                if "followers_handles" in user and user["login"]:
                    for follower in user["followers_handles"]:
                        followers.append({
                            "handle": user["login"],
                            "follower": follower
                        })
            followers_urls = self.save_json_as_csv(followers,  f"processor_followers_{self.asOf}")
            
            contributors = []
            subscribers = []
            for repo in repositories:
                if "contributors_handles" in repo:
                    for contributor in repo["contributors_handles"]:
                        if contributor and repo["full_name"]:
                            contributors.append({
                                "full_name": repo["full_name"],
                                "contributor": contributor
                            })
                if "subscribers_handles" in repo:
                    for subscriber in repo["subscribers_handles"]:
                        if subscriber and repo["full_name"]:
                            subscribers.append({
                                "full_name": repo["full_name"],
                                "subscriber": subscriber
                            })
            contributors_urls = self.save_json_as_csv(contributors,  f"processor_contributors_{self.asOf}")
            subscribers_urls = self.save_json_as_csv(subscribers,  f"processor_subscribers_{self.asOf}")

        self.cyphers.flag_bad_handles(handles_urls)
        self.cyphers.create_or_merge_users(users_urls)
        self.cyphers.create_or_merge_repositories(repositories_urls)
        self.cyphers.link_followers(followers_urls)
        self.cyphers.link_owners(repositories_urls)
        self.cyphers.link_contributors(contributors_urls)
        self.cyphers.link_subscribers(subscribers_urls)
        self.cyphers.create_or_merge_email(emails_urls)
        self.cyphers.link_email(emails_urls)
        self.cyphers.create_or_merge_twitter(twitters_urls)
        self.cyphers.link_twitter(twitters_urls)

    def recover_ingests(self):
        logging.info("Starting the recovery process")
        self.ingest_github_data(recover=True)

    def run(self):
        if os.environ.get("UPDATE_REPO_LANG", False):
            self.update_repositories_languages()
            return None
        if os.environ.get("RECOVER", False):
            self.recover_ingests()
        else:
            logging.info("Initializing data...")
            self.init_data()
            logging.info("Capturing existing users...")
            self.known_users = set(self.get_known_handles())
            logging.info("Capturing known users...")
            self.known_repos = set(self.get_known_repositories())
            logging.info("Labeling solidity repos...")
            self.get_solidity_repos()
            logging.info("Ingesting into Neo4J...")
            self.ingest_github_data()
            handles = self.get_all_handles()
            for i in range(0, len(handles), self.chunk_size):
                logging.info(f"Getting data for {i}...")
                self.init_data()
                self.process_github_accounts(handles[i: i+self.chunk_size])
                self.ingest_github_data()

if __name__ == '__main__':
    P = GithubProcessor()
    P.run()
