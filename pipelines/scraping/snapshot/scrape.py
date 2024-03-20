from ..helpers import Scraper
from .helpers.query_strings import spaces_query, proposals_query, votes_query, proposal_status_query
import json
import logging
import time

class SnapshotScraper(Scraper):
    def __init__(self, bucket_name="snapshot"):
        super().__init__(bucket_name)
        self.snapshot_url = "https://hub.snapshot.org/graphql"
        self.space_limit = 100
        self.proposal_limit = 500
        self.vote_limit = 1000

        self.last_space_offset = 0  # we collect all spaces

        self.last_proposal_offset = 0
        if "last_proposal_offset" in self.metadata:
            self.last_proposal_offset = self.metadata["last_proposal_offset"]

        self.open_proposals = set()
        if "open_proposals" in self.metadata:
            self.open_proposals = set(self.metadata["open_proposals"])

    def make_api_call(self, query, counter=0, content=None):
        time.sleep(counter)
        if counter > 10:
            logging.error(content)
            raise Exception("Something went wrong while getting the results from the API")
        content = self.post_request(self.snapshot_url, json={"query": query})
        if "504: Gateway time-out" in content:
            return self.make_api_call(query, counter=counter + 1, content=content)
        data = json.loads(content)
        if "data" not in data or "error" in data:
            return self.make_api_call(query, counter=counter + 1, content=content)
        return data["data"]

    def get_spaces(self):
        logging.info("Getting spaces...")
        raw_spaces = []
        offset = self.last_space_offset
        results = True
        while True:
            self.metadata["last_space_offset"] = offset
            if len(raw_spaces) % 1000 == 0:
                logging.info(f"Current Spaces: {len(raw_spaces)}")
            query = spaces_query.format(self.space_limit, offset)
            data = self.make_api_call(query)
            if not data["spaces"]:  # Check if "spaces" is empty to break the loop
                break
            results = data["spaces"]
            if results is not None:  # Ensure results is not None before extending raw_spaces
                raw_spaces.extend(results)
            offset += self.space_limit
        logging.info(f"Total Spaces acquired: {len(raw_spaces)}")
        ens_list = self.parallel_process(self.get_ens_info, raw_spaces, description="Getting information about ENS holders")
        for i in range(len(raw_spaces)):
            if ens_list[i] is not None:
                raw_spaces[i]["ens"] = ens_list[i]

        final_spaces = [space for space in raw_spaces if space]
        self.data["spaces"] = final_spaces
        logging.info(f"Final spaces count: {len(final_spaces)}")

    def get_proposals(self):
        logging.info("Getting proposals...")
        raw_proposals = []
        first = 1000  # Starting with the maximum allowed limit for "first"
        skip = 0
        more_data = True

        while more_data:
            self.metadata["last_proposal_offset"] = skip
            if len(raw_proposals) % 1000 == 0:
                logging.info(f"Current Proposals: {len(raw_proposals)}")
            query = proposals_query.format(first, skip)
            data = self.make_api_call(query)
            results = data["proposals"]
            if results:
                raw_proposals.extend(results)
                skip += len(results)  # Increment skip by the number of results fetched
            else:
                more_data = False  # Stop loop if no results are returned

            # Reset skip and adjust first if skip reaches or exceeds the limit
            if skip >= 5000:
                skip = 0  # Reset skip to start from the beginning for the next batch
                # Logic to adjust 'first' if needed, based on specific requirements

        proposals = [proposal for proposal in raw_proposals if proposal]
        logging.info(f"Total Proposals: {len(proposals)}")
        self.data["proposals"] = proposals


    def scrape_votes(self, proposal_ids):
        raw_votes = []
        offset = 0
        results = True
        while results:
            offset += self.vote_limit
            query = votes_query.format(self.vote_limit, offset, json.dumps(proposal_ids))
            data = self.make_api_call(query)
            results = data["votes"]
            if type(results) != list:
                logging.error("Something went wrong while getting the data that was not caught correctly!")
            if results:
                raw_votes.extend(results)
        return raw_votes

    def get_votes(self):
        logging.info("Getting votes...")
        proposal_ids = list(self.open_proposals.union(set([proposal["id"] for proposal in self.data["proposals"]])))
        
        proposals = [proposal_ids[i : i + 5] for i in range(0, len(proposal_ids), 5)]
        raw_votes = self.parallel_process(self.scrape_votes, proposals, description="Getting all the votes")
        votes = [vote for votes in raw_votes for vote in votes]
        logging.info(f"Total Votes: {len(votes)}")
        self.data["votes"] = votes

    def get_proposals_status(self):
        proposal_statuses = []
        for proposal_id in self.open_proposals:
            query = proposal_status_query.format(proposal_id)
            data = self.make_api_call(query)
            if len(data["proposals"]) == 0:
                logging.error(f"Something unexpected happened with proposal id: {proposal_id}")
            if data["proposals"]:
                proposal_statuses += data["proposals"]
        open_proposals = [proposal["id"] for proposal in proposal_statuses if proposal["state"] != "closed"]
        open_proposals += [proposal["id"] for proposal in self.data["proposals"] if proposal["state"] != "closed"]
        self.open_proposals = list(set(open_proposals))
        self.metadata["open_proposals"] = self.open_proposals

    def run(self):
        self.get_spaces()
        self.get_proposals()
        self.get_votes()
        self.get_proposals_status()

        self.save_metadata()
        self.save_data()


if __name__ == "__main__":

    scraper = SnapshotScraper()
    scraper.run()
