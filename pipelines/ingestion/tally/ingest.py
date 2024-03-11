from pipelines.ingestion.helpers.ingestor import Ingestor
from .cyphers import TallyCyphers

import json
import logging
import pandas as pd
import datetime





class TallyIngestor(Ingestor):
    def __init__(self):        
        self.cyphers = TallyCyphers()
        super().__init__(bucket_name="arbitrum-tally")
        self.asOf = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M")



    def proccess_proposals(self):
        proposal_metadata_list = []
        proposals = self.scraper_data['data']['proposals']
        for proposal in proposals:
             proposal_info = {}
             proposal_info = {
                  'id': proposal['id'],
                  'title': proposal['title'],
                  'description': proposal['description'],
                  'eta': proposal['eta'],
             }
             proposal_metadata_list.append(proposal_info)
        proposals_df = pd.DataFrame(proposal_metadata_list)
        urls = self.save_df_as_csv(proposals_df, f"data_proposal_metadata_tally_{self.asOf}.csv")
        self.cyphers.create_proposals(urls)


    def process_voters(self):
        voters_list = []
        logging.info("Extracting voters...")
        for proposal in self.scraper_data['data']['proposals']: 
            for vote in proposal.get('votes', []):
                voter_dict = {}
                indiv_voter = vote['voter']
                voter_dict['address'] = indiv_voter.get('address', '')
                voter_dict['ens'] = indiv_voter.get('ens', '')
                voter_dict['name'] = indiv_voter.get('name', '')
                voter_dict['bio'] = indiv_voter.get('bio', '')
                voters_list.append(voter_dict)
        voters_df = pd.DataFrame(voters_list)
        voters_df.drop_duplicates(inplace=True)
        voter_urls = self.save_df_as_csv(voters_df, f"data_tally_voters_{self.asOf}.csv")
        # self.cyphers.create_voters(voter_urls)
        self.cyphers.create_connect_wallets_ents_identifiers(voter_urls)

    def process_votes(self):
        votes_list = []
        logging.info("Extracing votes...")
        for proposal in self.scraper_data['data']['proposals']:
            proposalId = proposal.get('id')
            for vote in proposal.get('votes', []):
                vote_dict = {}
                vote_dict['proposalId'] = proposalId
                vote_dict['voter'] = vote.get('voter').get('address')
                vote_dict['id'] = vote.get('id')
                vote_dict['support'] = vote.get('support')
                vote_dict['weight'] = vote.get('weight')
                votes_list.append(vote_dict) 

        votes_df = pd.DataFrame(votes_list)
        votes_df.drop_duplicates(inplace=True)
        votes_urls = self.save_df_as_csv(votes_df, f"data_tally_votes_{self.asOf}.csv")
        self.cyphers.connect_votes(votes_urls)



    def run(self):
        # self.proccess_proposals()
        # self.process_voters()
        self.process_votes()
    
if __name__ == '__main__':
    ingestor = TallyIngestor()
    ingestor.run()

        


