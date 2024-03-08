from pipelines.ingestion.helpers.ingestor import Ingestor
from .cyphers import ManualCyphers

import json
import pandas as pd
import datetime


class ManualIngestor(Ingestor):
    def __init__(self):        
        self.cyphers = ManualCyphers()
        super().__init__(bucket_name="arbitrum-manual-ingest", load_data=False)
        self.filepath = "pipelines/ingestion/manual-research-initial/data"
        self.grantees_direct_df = pd.read_csv(f'{self.filepath}/data_grantees_direct_manual_20240307.csv')    
        self.grantees_ecosystem_df = pd.read_csv(f'{self.filepath}/data_grantee_ecosystem_manual_20240307.csv')
        self.grantees_direct_urls = self.save_df_as_csv(self.grantees_direct_df, f"data_grantees_direct_{self.asOf}.csv")
        self.grantees_ecosystem_urls = self.save_df_as_csv(self.grantees_ecosystem_df, f"data_grantees_ecosystem_{self.asOf}.csv")
        self.grants_df = pd.read_csv(f"{self.filepath}/data_grants_direct_manual_20240307.csv")

    def process_grantees(self):
        self.cyphers.create_grantees_direct(self.grantees_direct_urls)
        self.cyphers.create_grantees_ecosystem(self.grantees_ecosystem_urls)

    def process_wallets(self):
        wallets_df_direct = self.grantees_direct_df[self.grantees_direct_df['fundingWalletAddress'].notna() 
        & (self.grantees_direct_df['fundingWalletAddress'] != '')]
        wallets_direct_urls = self.save_df_as_csv(wallets_df_direct, f"data_wallets_df_ecosystem_urls_{self.asOf}.csv")
        self.cyphers.create_wallets(wallets_direct_urls)

    def process_twitter(self):
        twitter_df_direct = self.grantees_direct_df[self.grantees_direct_df['twitterHandle'].notna() & 
                                      self.grantees_direct_df['twitterHandle'].ne('')]
        twitter_df_ecosystem = self.grantees_ecosystem_df[self.grantees_ecosystem_df['twitterHandle'].notna() & 
                                      self.grantees_ecosystem_df['twitterHandle'].ne('')]

        twitter_direct_urls = self.save_df_as_csv(twitter_df_direct, f"data_twitter_direct_{self.asOf}.csv")
        twitter_ecosystem_urls = self.save_df_as_csv(twitter_df_ecosystem, f"data_twitter_df_ecosystem_urls_{self.asOf}.csv")

        self.cyphers.create_twitter_accounts(twitter_direct_urls)
        self.cyphers.create_twitter_accounts(twitter_ecosystem_urls)

        self.cyphers.connect_twitter_accounts(twitter_direct_urls)
        self.cyphers.connect_twitter_accounts(twitter_ecosystem_urls)

    def process_github(self):
        github_df_direct = self.grantees_direct_df[self.grantees_direct_df['githubHandle'].notna() & 
                                      self.grantees_direct_df['githubHandle'].ne('')]
        github_df_ecosystem = self.grantees_ecosystem_df[self.grantees_ecosystem_df['githubHandle'].notna() & 
                                      self.grantees_ecosystem_df['githubHandle'].ne('')]

        github_direct_urls = self.save_df_as_csv(github_df_direct, f"data_github_direct_{self.asOf}.csv")
        github_ecosystem_urls = self.save_df_as_csv(github_df_ecosystem, f"data_github_df_ecosystem_urls_{self.asOf}.csv")

        self.cyphers.create_githubs(github_direct_urls)
        self.cyphers.create_githubs(github_ecosystem_urls)

    def process_dune(self):
        dune_df_direct = self.grantees_direct_df[self.grantees_direct_df['duneHandle'].notna() & 
                                      self.grantees_direct_df['duneHandle'].ne('')]
        dune_direct_urls = self.save_df_as_csv(dune_df_direct, f"data_dune_direct_{self.asOf}.csv")
        self.cyphers.create_dunes(dune_direct_urls)

    def process_websites(self):
        websites_df_direct = self.grantees_direct_df[self.grantees_direct_df['websiteUrl'].notna() & 
                                      self.grantees_direct_df['websiteUrl'].ne('')]
        websites_df_ecosystem = self.grantees_ecosystem_df[self.grantees_ecosystem_df['websiteUrl'].notna() & 
                                      self.grantees_ecosystem_df['websiteUrl'].ne('')]


        website_df_direct_urls = self.save_df_as_csv(websites_df_direct, f"data_website_direct_{self.asOf}.csv")
        website_df_ecosystem_urls = self.save_df_as_csv(websites_df_ecosystem, f"data_website_df_ecosystem_urls_{self.asOf}.csv")

        self.cyphers.create_websites(website_df_direct_urls)
        self.cyphers.create_websites(website_df_ecosystem_urls)

    def process_orgs(self):
        self.cyphers.create_orgs()

    def process_grants(self):
        grants_initiatives_df = self.grants_df[['name','grantInitiative', 'grantsPlatform', 'grantTokenAddress']]
        grants_initiatives_df =  grants_initiatives_df[grants_initiatives_df['grantInitiative'].notna() & 
                                       grants_initiatives_df['grantInitiative'].ne('')]
        grants_urls = self.save_df_as_csv(grants_initiatives_df, f'data_grants_{self.asOf}.csv')
        self.cyphers.create_grants(grants_urls)
        self.cyphers.connect_grants(grants_urls)

    def process_funders(self):   
        self.grants_df['funder'] = self.grants_df['funder'].apply(lambda x: x.split(';') if pd.notna(x) else x)
        exploded_funders_df = self.grants_df.explode('funder')
        exploded_funders_df = exploded_funders_df[exploded_funders_df['funder'].notna() & (exploded_funders_df['funder'] != '')]
        deduped_funders_df = exploded_funders_df.drop_duplicates()
        deduped_funders_df_urls = self.save_df_as_csv(deduped_funders_df, f"data_funders_grants_{self.asOf}.csv")
        self.cyphers.connect_funders_grants(deduped_funders_df_urls)

    def process_grants_platforms(self):
        self.cyphers.connect_grants_platforms()

    def process_funding_wallets(self):
        self.cyphers.connect_funding_wallet_grant()

    def process_grants_snapshot(self):
        self.cyphers.connect_approvals_snapshot()

    def process_submission_grants(self):
        self.cyphers.connect_submission_grants()


    def run(self):
        self.process_grantees()
        self.process_wallets()
        self.process_twitter()
        self.process_github()
        self.process_orgs()
        self.process_dune()
        self.process_websites()
        self.process_grants()
        self.process_funders()
        self.process_grants_platforms()
        self.process_funding_wallets()
        self.process_grants_snapshot()
        # self.process_submission_grants()



if __name__ == '__main__':
    ingestor = ManualIngestor()
    ingestor.run()
