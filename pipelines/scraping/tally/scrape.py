# from ...helpers.selenium import Selenium, By
from ..helpers import Scraper
import logging 
import time
import json
import os 
import requests as requests

                                        


class TallyScraper(Scraper):
    def __init__(self, bucket_name="arbitrum-tally", load_data=False):
        super().__init__(bucket_name=bucket_name, load_data=load_data)

        self.arb_base_url = "https://tally.xyz/gov/arbitrum"
        self.arb_proposals_base_url = "https://tally.xyz/gov/arbitrum/proposals"
        self.base_api_endpoint = "https://api.tally.xyz/query"
        self.headers = {
            'Content-Type': 'application/json',
            'Api-Key': os.getenv('TALLY_API_KEY')
        }
        



    def send_tally_api_request(self, query, variables=None, retry_count=0):
        max_retries = 25  # Adjusted max_retries to allow for retries
        while retry_count <= max_retries:
            try:
                payload = {"query": query, "variables": variables}  # Include variables in the payload
                response = requests.post(self.base_api_endpoint, json=payload, headers=self.headers)
                response.raise_for_status()  # Raises exception for 4XX or 5XX errors
                data = response.json()
                if "errors" in data:
                    error_message = json.dumps(data['errors'], indent=4)  # Formatting the error for better readability
                    raise Exception(f"API response error: {error_message}")
                return data["data"]
            except requests.RequestException as e:
                logging.error(f"Request error: {e}, Response: {e.response.text}")
                time.sleep(2 ** retry_count)  # Implementing exponential backoff
                retry_count += 1
                if retry_count > max_retries:
                    logging.error("Max retries exceeded. Giving up.")
                    return None
            except Exception as e:
                logging.error(f"Unhandled exception: {e}")
                return None
            


    def arb_votes_voters(self):
        ## get proposal creators
        query = """
        query Proposals($chainId: ChainID!, $governorIds: Address!) {
          proposals(chainId: $chainId, governors: [$governorIds]) {
            id
            title
            description
            eta
            votes {
                voter {
                    address
                    ens
                    twitter
                    name
                    bio
                }
                id
                support
                weight 
            }
        }
    }"""
        
        variables = {"chainId": "eip155:42161", "governorIds": "0xf07DeD9dC292157749B6Fd268E37DF6EA38395B9"}
        logging.info("Collecting voters for proposals....")
        results = self.send_tally_api_request(query, variables=variables)
        self.data['data'] = results



    def get_delegates(self):
        query = """
        query Delegates($input: DelegatesInput!) {
        delegates(input: $input) {
            nodes {
            ... on Delegate {
                id
                account {
                id
                address
                }
                chainId
                delegatorsCount
                votesCount
                statement {
                statement
                discourseUsername
                }
                
            }
        }
            pageInfo {
            firstCursor
            lastCursor
            count
            }
        }
        }
    """
        all_delegates = []
        variables = {
            "input": {
                "filters": {
                    "governorId": "eip155:42161:0xf07DeD9dC292157749B6Fd268E37DF6EA38395B9"
                },
                "page": {
                    "limit": 100
                },
                "sort": {
                    "isDescending": True,
                    "sortBy": "DELEGATORS"
                }
            }
        }

        has_more_pages = True
        while has_more_pages:
            logging.info("Collecting delegates...")
            results = self.send_tally_api_request(query, variables=variables)
            time.sleep(.75)
            if not results or 'delegates' not in results or not results['delegates']['nodes']:
                has_more_pages = False
                continue
            # Filter delegates with more than one vote
            delegates_with_delegators = [delegate for delegate in results['delegates']['nodes'] if int(delegate.get('delegatorsCount', 0)) > 2]
            if not delegates_with_delegators and not delegates_with_delegators:
                has_more_pages = False
                continue
            all_delegates.extend(delegates_with_delegators)
            last_cursor = results['delegates']['pageInfo'].get('lastCursor')
            if last_cursor:
                variables['input']['page']['afterCursor'] = last_cursor
            else:
                has_more_pages = False
            logging.info(f"Retrieved {len(all_delegates)} delegates...")
        logging.info("Completed delegate collection")
        return all_delegates
    
    def fetch_delegators_for_delegate(self, delegate_address):
        query = """
        query Delegators($input: DelegationsInput!) {
            delegators(input: $input) {
                nodes {
                    ... on DelegationV2 {
                        id
                        blockNumber
                        blockTimestamp
                        chainId
                        delegator {
                            id
                            address
                        }
                        delegate {
                            id
                            account {
                                id
                                address
                            }
                        }
                        governor {
                            id
                        }
                        token {
                            id
                            symbol
                        }
                        votes
                    }
                }
                pageInfo {
                    firstCursor
                    lastCursor
                    count
                }
            }
        }
        """

        delegators = []
        afterCursor = None

        while True:
            variables = {
                "input": {
                    "filters": {
                        "address": delegate_address
                    },
                    "page": {
                        "limit": 100,
                        "afterCursor": afterCursor
                    },
                    "sort": {
                        "isDescending": True,
                        "sortBy": "VOTES"
                    }
                }
            }
            logging.info(f"Processing delegators for {delegate_address}...")
            try:
                results = self.send_tally_api_request(query, variables=variables)
                time.sleep(.75)
                if not results or 'delegators' not in results or not results['delegators']['nodes']:
                    break  # No more pages to process or error in response
                current_delegators = results['delegators']['nodes']
                logging.info(f"Collected {len(current_delegators)} delegators...")
                delegators.extend(current_delegators)
                logging.info(f"{len(delegators)} collected total")
                last_cursor = results['delegators']['pageInfo'].get('lastCursor')
                if not last_cursor:
                    break  # No more pages to process
                afterCursor = last_cursor
            except Exception as e:
                logging.info(f"Some error retrieving delegators: {e}")

        return delegators
    
    def fetch_all_delegators(self, delegate_addresses):
        count = 0 
        all_delegators = []
        logging.info(f"Fetching {len(delegate_addresses)} delegators...")
        logging.info("Collecting delegators...")
        for delegate_address in delegate_addresses:
            try:
                count += 1
                logging.info(f"Collecting delegators for {delegate_address}..., the {str(count)} delegator out of {len(delegate_addresses)}")
                delegators = self.fetch_delegators_for_delegate(delegate_address)
                count += 1
                all_delegators.extend(delegators)
                print(f"Processed {delegate_address}, found {len(delegators)} delegators.")
                print(f"Captured {len(all_delegators)} in total.")
            except Exception as e:
                logging.error(f"Ran ino an error pulling delegators: {e}")
                pass 

        return all_delegators

# Assuming `delegate_addresses` is a list of delegate addresses you want to process
# all_delegators = fetch_all_delegators(delegate_addresses)

    def run(self):
        # self.arb_votes_voters()
        # self.data['votes'] = self.arb_votes_voters()
        delegates = self.get_delegates()
        self.data['delegates'] = delegates 
        self.save_data()
        # self.data['delegates'] = delegates
        # addresses = [i.get('account').get('address') for i in delegates]
        # delegators = self.fetch_all_delegators(addresses)
        # self.data['delegators'] = delegators    
        # self.save_data()
        # self.save_metadata()

if __name__ == "__main__":
    S = TallyScraper()
    S.run()
