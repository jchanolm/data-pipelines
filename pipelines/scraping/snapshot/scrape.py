import requests
import json
import logging
import time
from ..helpers import Scraper
from .helpers.queries import proposals_query

class SnapshotScraper(Scraper):
    def __init__(self):
        super().__init__(bucket_name='snapshot')
        self.snapshot_api_url = "https://hub.snapshot.org/graphql"
        self.arb_space_id = "arbitrumfoundation.eth"
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.last_space_offset = 0


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
        self.space_limit = 1000

        self.spaces_query =  """

        {{
            spaces(
                first: {0},
                skip: {1},
                orderBy: "created",
                orderDirection: asc
            ) {{
            id
                name
                about
                avatar
                terms
                location
                website
                twitter
                github
                network
                symbol
                strategies {{
                name
                params
                }}
                admins
                members
                filters {{
                minScore
                onlyMembers
                }}
                plugins
            }}
        }}
        """



    def make_api_call(self, query, counter=0, content=None):
        time.sleep(counter)
        if counter > 10:
            logging.error(content)
            raise Exception("Something went wrong while getting the results from the API")
        content = self.post_request(self.snapshot_api_url, json={"query": query})
        if "504: Gateway time-out" in content:
            return self.make_api_call(query, counter=counter + 1, content=content)
        data = json.loads(content)
        if "data" not in data or "error" in data:
            return self.make_api_call(query, counter=counter + 1, content=content)
        return data["data"]

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
    
    def get_all_spaces(self):
        logging.info("Getting spaces...")
        raw_spaces = []
        offset = self.last_space_offset
        results = True
        while results:
            self.metadata["last_space_offset"] = offset
            if len(raw_spaces) % 1000 == 0:
                logging.info(f"Current Spaces: {len(raw_spaces)}")
            query = self.spaces_query.format(self.space_limit, offset)
            data = self.make_api_call(query)
            results = data.get("spaces")  # Use get to avoid KeyError if "spaces" is missing
            if results:  # Check if results is not None or empty
                raw_spaces.extend(results)
                offset += self.space_limit
            else:  # Break the loop if results is None or empty
                break
        logging.info(f"Total Spaces acquired: {len(raw_spaces)}")
        ens_list = self.parallel_process(self.get_ens_info, raw_spaces, description="Getting information about ENS holders")
        for i in range(len(raw_spaces)):
            if ens_list[i] is not None:
                raw_spaces[i]["ens"] = ens_list[i]

        final_spaces = [space for space in raw_spaces if space]
        self.data["spaces"] = raw_spaces
        logging.info(f"Final spaces count: {len(final_spaces)}")


    def run(self):
        # self.data['spaces'] = self.get_all_spaces()
        proposals_response = self.fetch_proposals()
        proposals = proposals_response.get('proposals', [])
        all_votes = self.get_all_votes_for_proposals(proposals)
        refined_votes = self.refine_votes(all_votes)
        # self.data['proposals'] = proposals 
        # self.data['votes'] = refined_votes
        # if proposals:
        #     most_recent_proposal = max(proposals, key=lambda x: x['end'])
        #     self.metadata['last_proposal_end_dt'] = most_recent_proposal['end']
        #     self.metadata['last_proposal_id'] = most_recent_proposal['id']
        #     logging.info(f"Total votes fetched and refined: {len(refined_votes)}")
        self.save_data()
        self.save_metadata()

if __name__ == "__main__":
    scraper = SnapshotScraper()
    scraper.run()