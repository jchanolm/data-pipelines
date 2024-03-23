from ..helpers import Ingestor
from .cyphers import MultisigCyphers
import datetime
import pandas
from typing import Dict, List, Any
import logging


class MultisigIngestor(Ingestor):
    def __init__(self):
        self.cyphers = MultisigCyphers()
        super().__init__("multisigs")

    def prepare_multisig_wallet_data(self):
        all_wallets = set([x["multisig"] for x in self.scraper_data["multisig"]] + [x["ownerAddress"] for x in self.scraper_data["multisig"]])
        data = [{"address": wallet} for wallet in all_wallets if wallet != "" and wallet is not None]
        return data

    def ingest_multisig(self):
        logging.info("Ingesting multisigs...")

        wallet_data = self.prepare_multisig_wallet_data()
        urls = self.save_json_as_csv(wallet_data, f"ingestor_wallets_{self.asOf}")
        self.cyphers.create_or_merge_multisig_wallets(urls)

        multisig_data = (
            pandas.DataFrame(self.scraper_data["multisig"]).drop_duplicates(subset=["multisig"]).to_dict("records")
        )

        # add multisig labels
        urls = self.save_json_as_csv(multisig_data, f"ingestor_multisig_{self.asOf}")
        self.cyphers.add_multisig_labels_data(urls)

        multisig_signers_data = (
            pandas.DataFrame(self.scraper_data["multisig"])
            .drop_duplicates(subset=["multisig", "ownerAddress"])
            .to_dict("records")
        )

        # add signer relationships (multisig-wallet)
        urls = self.save_json_as_csv(multisig_signers_data, f"ingestor_multisig_signers_{self.asOf}")
        self.cyphers.link_multisig_signer(urls)

        multisig_creators_data = (
            pandas.DataFrame(self.scraper_data["multisig"])
            .drop_duplicates(subset=["multisig", "creator"])
            .to_dict("records")
        )

        # add signer relationships (multisig-wallet)
        urls = self.save_json_as_csv(multisig_creators_data, f"ingestor_multisig_creators_{self.asOf}")
        self.cyphers.link_multisig_creators(urls)

    def run(self):
        self.ingest_multisig()
        self.save_metadata()


if __name__ == "__main__":
    ingestor = MultisigIngestor()
    ingestor.run()
