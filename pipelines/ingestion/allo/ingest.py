from ..helpers import Ingestor
from .cyphers import AlloCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging


class AlloIngestor(Ingestor):
    def __init__(self):
        self.cyphers = AlloCyphers()
        super().__init__("allo")

    def process_protocols(self):
        protocols = self.scraper_data['protocols']['metaPtrs']
        protocols_df = pd.DataFrame(protocols)
        protocol_urls = self.save_df_as_csv(protocols_df, f"allo_protocols_{self.asOf}.csv")
        self.cyphers.create_protocols(protocol_urls)


    def process_rounds(self):
        rounds = self.scraper_data['rounds']['rounds']
        rounds_list = [
            {
                'id': round_data.get('id').lower(),
                'applicationsStartTime': round_data.get('applicationsStartTime'),
                'applicationsEndTime': round_data.get('applicationsEndTime'),
                'matchAmount': round_data.get('matchAmount'),
                'roundEndTime': round_data.get('roundEndTime'),
                'roundFeeAddress': round_data.get('roundFeeAddress'),
                'roundFeePercentage': round_data.get('roundFeePercentage'),
                'token': round_data.get('token').lower(),
                'votingStrategy': round_data.get('votingStrategy', '').lower(),
                'payoutStrategy': round_data.get('payoutStrategy', {}).get('id', None).lower() if round_data.get('payoutStrategy') else None,
                'protocolIdRaw': round_data.get('applicationMetaPtr', {}).get('id')
            } for round_data in rounds
        ]
        rounds_df = pd.DataFrame(rounds_list)
        round_urls = self.save_df_as_csv(rounds_df, f"allo_rounds_{self.asOf}.csv")
        self.cyphers.create_rounds(round_urls)
        self.cyphers.connect_rounds_protocols(round_urls)
        self.cyphers.create_connect_round_tokens(round_urls)
        self.cyphers.create_connect_round_fee_addresses(round_urls)

        round_accounts = []
        for round in rounds:
            roundId = round.get('id').lower()
            for account in round.get('accounts', []):
                accountAddress = account.get('address')
                roleId = account.get('role', {}).get('id')
                roleRole = account.get('role', {}).get('role')

                round_accounts.append({
                    'roundId': roundId, 
                    'accountAddress': accountAddress, 
                    'roleId': roleId,
                    'role': roleRole
                })
        round_accounts_df = pd.DataFrame(round_accounts)
        round_accounts_urls = self.save_df_as_csv(round_accounts_df, f"allo_round_accounts_{self.asOf}.csv")
        self.cyphers.create_round_accounts(round_accounts_urls)
        self.cyphers.connect_round_accounts(round_accounts_urls)

    def process_projects(self):
        rounds = self.scraper_data['rounds']['rounds']
        projects_list = []
        for round in rounds:
            roundId = round.get('id').lower()
            projects = round.get('projects', [])
            for project in projects:
                projectId = project.get('project')
                projects_list.append({
                    'roundId': roundId,
                    'projectId': projectId
                })
        projects_df = pd.DataFrame(projects_list)
        project_urls = self.save_df_as_csv(projects_df, f"allo_projects_{self.asOf}.csv")
        self.cyphers.create_connect_projects(project_urls)
        self.cyphers.round_grantees()
        




    def process_payouts(self):
        payouts = self.scraper_data['payouts']['payouts']
        payouts_data = [
        
            {
                'id': i.get('id').lower(),
                'publishedDt': i.get('createdAt'),
                'amount': i.get('amount'),
                'grantee': i.get('grantee').lower(),
                'projectId': i.get('projectId').lower(),
                'protocolFee': i.get('protocolFee'),
                'token': i.get('token'),
                'txnHash': i.get('txnHash')
            }
            for i in payouts
        ]
        payouts_df = pd.DataFrame(payouts_data)
        payout_urls = self.save_json_as_csv(payouts_df, f"allo_payouts_{self.asOf}.csv")
        self.cyphers.create_grantees(payout_urls)
        self.cyphers.create_connect_payouts(payout_urls)


    def run(self):
        self.process_protocols()
        self.process_rounds()
        self.process_projects()
        self.process_payouts()
        self.save_metadata()


if __name__ == "__main__":
    ingestor = AlloIngestor()
    ingestor.run()
