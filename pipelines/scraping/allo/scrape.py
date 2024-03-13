from ..helpers import Scraper
import logging 
import time
import os 
import json 
import requests as requests
import pandas as pd 

class AlloScraper(Scraper):
    def __init__(self, bucket_name="allo", load_data=False):
        super().__init__(bucket_name=bucket_name, load_data=load_data)
        self.allo_subgraph_url = """https://api.thegraph.com/subgraphs/name/gitcoinco/gitcoin-grants-arbitrum-one/"""
        self.headers = {
            'Content-Type': 'application/json'
        }


    def send_allo_api_request(self, query, variables=None, retry_count=0):
        max_retries = 0  # Adjusted max_retries to allow for retries
        while retry_count <= max_retries:
            try:
                payload = {"query": query, "variables": variables}  # Include variables in the payload
                response = requests.post(self.allo_subgraph_url, json=payload, headers=self.headers)
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
    def get_protocols(self):

        query = """
        {
            metaPtrs(first: 1000) {
            protocol
            id
            pointer
        }
    }
        """
        results = self.send_allo_api_request(query)
        self.data['protocols'] = results 

    def get_rounds(self):
        query = """
        {
            rounds {
                accounts {
                    id
                    address
                    role {
                        id
                        role
                    }
                }
                applicationMetaPtr {
                    id
                    protocol
                }
                applicationsStartTime
                applicationsEndTime
                createdAt
                id
                matchAmount
                roundEndTime
                roundFeeAddress
                roundFeePercentage
                token
                votingStrategy
                payoutStrategy {
                    id
                }
                program {
                    id
                }
            }
        }
        """
        results = self.send_allo_api_request(query)
        self.data['rounds'] = results 

    def get_programs(self):
        query = """
            {
            programs {
                accounts {
                address 
                id
                program {
                roles {
                    id 
                }
            }
        }
            }
        }
        """
        results = self.send_allo_api_request(query)
        self.data['programs'] = results 

    def get_payouts(self):
        query = """
        {
        payouts(first: 1000) {
            createdAt
            id
            amount
            grantee
            projectId
            protocolFee
            roundFee
            token
            txnHash
            vault
            payoutStrategy {
            id
            round {
                id
            }
            }
        }
    }
    """
        results = self.send_allo_api_request(query)
        self.data['payouts'] = results 
    def run(self):
        self.get_protocols()
        self.get_rounds()
        self.get_programs()
        self.get_payouts()
        self.save_metadata()
        self.save_data()

if __name__ == "__main__":
    S = AlloScraper()
    S.run()
