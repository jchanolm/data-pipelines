from ..helpers import Scraper
import logging
import tqdm
import os
import requests
import json
DEBUG = os.environ.get("DEBUG", False)

class EthLeaderboardEnsScraper(Scraper):
    def __init__(self):
        super().__init__("twitter-ens")
        self.api_url = "https://ethleaderboard.xyz/api/frens?skip={}"
    def get_accounts(self):
        logging.info("Getting Twitter / ENS linkages...")
        accounts = []
        skip = 0
        more_results = True
        while more_results:
            query = self.api_url.format(int(skip))  # Ensuring 'skip' is formatted as an int
            print(query)
            logging.debug(f"Requesting: {query}")  # Log the request URL
            response = requests.get(query)
            if response.status_code != 200:
                logging.error(f"Failed to fetch data: {response.status_code}")
                break
            data = response.json()
            new_accounts = data.get("frens", [])
            if not new_accounts:
                more_results = False
                continue
            accounts.extend(new_accounts)
            skip += 100  # Increment by 100 to fetch the next set of results
            logging.info(f"Current Accounts: {len(accounts)}")
        logging.info(f"Total accounts acquired: {len(accounts)}")

        accounts = [{"ens": account["ens"], "handle": account["handle"]} for account in accounts]
        self.data["accounts"] = accounts
    def run(self):
        self.get_accounts()
        # self.save_metadata()
        self.save_data()

if __name__ == "__main__":
    scraper = EthLeaderboardEnsScraper()
    scraper.run()
