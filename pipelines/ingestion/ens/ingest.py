from ..helpers import Ingestor
from .cyphers import ENSCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging


class ENSIngestor(Ingestor):
    def __init__(self):
        self.cyphers = ENSCyphers()
        super().__init__("ens")

    def prepare_domains(self):
        print(self.scraper_data.keys())
        domains = pd.DataFrame(self.scraper_data["domains"])
        domains = domains[domains["name"].apply(len) < 400]
        domains = domains[~domains["name"].isna()]
        domains = domains[~domains["createdAt"].isna()]
        resolvedAddresses = domains[~domains["resolvedAddress"].isna()]
        owners = domains[~domains["owner"].isna()]
        wallets = set(list(resolvedAddresses["resolvedAddress"].unique()) + list(resolvedAddresses["owner"].unique()))
        wallets = [{"address": address} for address in wallets if not self.is_zero_address(address)]
        return domains, resolvedAddresses, owners, wallets
    
    def ingest_domains(self):
        logging.info("Ingesting Domains")
        domains, resolvedAddresses, owners, wallets = self.prepare_domains()

        urls = self.save_json_as_csv(wallets, f"ingest_domains_wallets_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)

        urls = self.save_df_as_csv(domains, f"ingest_domains_{self.asOf}")
        self.cyphers.create_or_merge_ens_domains(urls)

        urls = self.save_df_as_csv(resolvedAddresses, f"ingest_resolvedAddresses_{self.asOf}")
        self.cyphers.link_ens_resolved_addresses(urls)

        urls = self.save_df_as_csv(owners, f"ingest_owners_{self.asOf}")
        self.cyphers.link_ens_owners(urls)
    
    def run(self):
        self.ingest_domains()

if __name__ == "__main__":
    ingestor = ENSIngestor()
    ingestor.run()
