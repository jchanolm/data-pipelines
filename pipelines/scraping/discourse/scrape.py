import requests
import json
import logging
import time
from ..helpers import Scraper
from .helpers.queries import proposals_query

class SnapshotScraper(Scraper):
    def __init__(self):
        super().__init__(module_name='snapshot')
        self.snapshot_api_url = "https://hub.snapshot.org/graphql"
        self.arb_space_id = "arbitrumfoundation.eth"
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.proposals_query = f"""
        {{
            proposals(
                where: {{
                    space_in: ["{self.arb_space_id}"]
                }},
                first: 1000
            ) {{
                id
                title
                body
                choices
                start
                end
                state
                author
            }}
        }}
        """ 



    def send_api_request(self, query, retry_count=0):
        max_retries = 10
        while retry_count <= max_retries:
            try:
                response = requests.post(self.snapshot_api_url, json={"query": query})
                response.raise_for_status()  # Raises exception for 4XX or 5XX errors
                data = response.json()
                if "errors" in data:
                    raise Exception(f"API response error: {data['errors']}")
                return data["data"]
            except requests.RequestException as e:
                logging.error(f"Request error: {e}")
                retry_count += 1
                if retry_count > max_retries:
                    logging.error("Max retries exceeded. Giving up.")
                    return None
            except Exception as e:
                logging.error(f"Unhandled exception: {e}")
                return None

        def fetch_proposals(self):
            logging.info("Fetching proposals...")
            return self.send_api_request(self.proposals_query)

        def get_all_votes_for_proposals(self, proposals):
            votes_query_template = """
            {{
                votes (
                    first: {0},
                    skip: {1},
                    orderBy: "created",
                    orderDirection: desc,
                    where: {{
                        proposal_in: ["{2}"]
                    }}
                ) {{
                    id
                    ipfs
                    voter
                    created
                    choice
                    proposal {{
                        id
                        choices
                    }}
                    space {{
                        id
                        name
                    }}
                }}
            }}
            """
            all_votes = []
            for proposal in proposals:
                proposal_id = proposal["id"]
                proposal_title = proposal['title']
                offset = 0
                results = True
                while results:
                    query = votes_query_template.format(1000, offset, proposal_id)
                    response = self.send_api_request(query)
                    time.sleep(.5)
                    if response:
                        data = response["votes"]
                        votes_count = len(data)
                        if votes_count == 0:
                            logging.info(f"No more votes found for {proposal_title}. Total votes retrieved: {len(all_votes)}")
                            break
                        try:
                            if offset < 5000:
                                all_votes.extend(data)
                                logging.info(f"Retrieved {votes_count} votes for {proposal_title}. Total votes retrieved so far: {len(all_votes)}")
                                offset += 1000
                            else:
                                logging.info(f"Reached maximum offset limit for {proposal_title}. Total votes retrieved: {len(all_votes)}")
                                break
                        except Exception as e:
                            logging.error(f"Error fetching votes for {proposal_title} with offset {offset}: {str(e)}")
                            break
                    else:
                        logging.error(f"Failed to fetch additional votes for {proposal_title}.")
                        break
            return all_votes
        
    def refine_votes(self, votes):
        refined_votes = []
        for vote in votes:
            refined_vote = {
                'proposalId': vote['proposal']['id'],
                'voter': vote['voter'].lower(),
                'choice': vote['choice'],
                'id': vote['id']
            }
            refined_votes.append(refined_vote)
        return refined_votes
    

    def run(self):
        proposals_response = self.fetch_proposals()
        proposals = proposals_response.get('proposals', [])
        all_votes = self.get_all_votes_for_proposals(proposals)
        refined_votes = self.refine_votes(all_votes)
        self.data['proposals'] = proposals 
        self.data['votes'] = refined_votes
        if proposals:
            most_recent_proposal = max(proposals, key=lambda x: x['end'])
            self.metadata['last_proposal_end_dt'] = most_recent_proposal['end']
            self.metadata['last_proposal_id'] = most_recent_proposal['id']
            logging.info(f"Total votes fetched and refined: {len(refined_votes)}")
        self.save_data_local()
        self.save_metadata_local()

if __name__ == "__main__":
    scraper = SnapshotScraper()
    scraper.run()
