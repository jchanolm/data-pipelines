# from ...helpers.selenium import Selenium, By
from ..helpers import Scraper
import logging 
import time
import json
import os 
import requests as requests
from bs4 import BeautifulSoup

                                        


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
        max_retries = 0  # Adjusted max_retries to allow for retries
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
        logging.info("Querying proposals...")
        results = self.send_tally_api_request(query, variables=variables)
        self.data['data'] = results


    # def get_propsals(self, proposal_ids):

        ## go to url
        ### get title
        #### get description (whole)
        #####
    # def get_proposal_urls(self):
    #     self.selenium_driver.get(self.arb_proposals_base_url) 
    #     time.sleep(2.5)     
    #     proposal_links = self.selenium_driver.find_elements(By.CSS_SELECTOR, 'a.script.props.pageprob')
    #     print(proposal_links)
    #    # proposal_links = self.selenium_driver.find_elements(By.c,'//a[contains(@class, "chakra-link") and contains(@href, "proposal")]')      
       # proposal_hrefs = [link.get_attribute('href') for link in proposal_links]
      #  print(proposal_hrefs)
      #  proposal_links_titles = []

        # go to base url 



    #def close_selenium_driver(self):
      #  self.selenium_driver.close()


    # def orgs_members():
    #     ## creators too
        ## return XXX
    

    ## get whether prop passed -- interesting to judge voters baser on that, if ou constantly vote for shit fails
    ### or no, etc
    ### tag snapshot vote status as well
    ## also good for discourse collusion

### comments
### asSOCIATE wallets w/forum authors
### get links to proposals and snapshots

    # def scrape_home
    # ### safes 
    def run(self):
        self.arb_votes_voters()
        # self.data['votes'] = self.arb_votes_voters()
        self.save_data()
        self.save_metadata()
        #self.close_selenium_driver()
        # self.arb_votes_voters()
        # self.save_metadata()
        # self.save_data()

        # self.save_data()
        # self.save_metadata()


## postProcessor -- safes + signers


if __name__ == "__main__":
    S = TallyScraper()
    S.run()
