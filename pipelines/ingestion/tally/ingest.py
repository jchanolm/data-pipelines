from .cyphers import TallyCyphers
from ..helpers import Ingestor
from ...helpers import Utils
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging
import datetime 
import logging 
import pandas as pd
import json 
import re 





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

    def process_delegates(self):
        # delegates = self.scraper_data['delegates']
        # delegates_unnested = [
        #     {
        #         'id': i.get('id'),
        #         'address': i.get('account').get('address').lower(),
        #         'delegatorsCount': i.get('delegatorsCount'),
        #         'votesCount': i.get('votesCount'),
        #         'delegateStatement': i.get('statement', {}).get('statement', '') if i.get('statement') and i.get('statement').get('statement') not in [None, ''] else None,
        #         'discourseUsername': i.get('statement', {}).get('discourseUsername', '').strip().lower() if i.get('statement') and i.get('statement').get('discourseUsername') not in [None, ''] else None
        #     }
        # for i in delegates
        # ]
        # delegates_unnested_df = pd.DataFrame(delegates_unnested)
        # ## metadata
        # delegates_main = delegates_unnested_df[['id', 'address', 'delegatorsCount', 'votesCount']]
        # delegates_main_urls = self.save_df_as_csv(delegates_main, f"delegates_data_{self.asOf}.csv")
        # # self.cyphers.create_delegates_main(delegates_main_urls)

        # ## statement text 
        # delegates_statements_df = delegates_unnested_df[['address', 'delegateStatement']]
        # delegates_statements_df = delegates_statements_df.dropna(subset=['delegateStatement'])
        # delegates_statements_urls = self.save_df_as_csv(delegates_statements_df, f"delegates_statements_data{self.asOf}.csv")
        # # self.cyphers.set_delegates_statement(delegates_statements_urls)

        # # discourse

        # # self.cyphers.connect_delegates_discourse(urls)
        # delegates_discourse_df = delegates_unnested_df[['address', 'discourseUsername']]
        # delegates_discourse_df = delegates_discourse_df.dropna(subset=['discourseUsername'])
        # delegates_discourse_df = delegates_discourse_df[delegates_discourse_df['discourseUsername'] != '']
        # delegates_discourse_urls = self.save_df_as_csv(delegates_discourse_df, f"delegates_discourse_data_{self.asOf}.csv")
        # self.cyphers.connect_delegates_discourse(delegates_discourse_urls)

        # delegators 
        delegators = self.scraper_data['delegators']
        delegators_unnested = [
            {
                'delegator': i.get('delegator').get('address').lower(),
                'delegate': i.get('delegate').get('account').get('address').lower()
            }
        for i in delegators
        ]
        delegators_df = pd.DataFrame(delegators_unnested)
        delegators_urls = self.save_df_as_csv(delegators_df, f"delegators_data_{self.asOf}.csv")
        self.cyphers.connect_delegators_delegates(delegators_urls)








    def run(self):
        # self.proccess_proposals()
        # self.process_voters()
        # self.process_votes()
        self.process_delegates()
    
if __name__ == '__main__':
    ingestor = TallyIngestor()
    ingestor.run()

        


