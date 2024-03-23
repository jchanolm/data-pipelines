from ..helpers import Ingestor
from .cyphers import TwitterEnsCyphers
import json
import pandas
import datetime


class TwitterEnsIngestor(Ingestor):
    def __init__(self):
        self.cyphers = TwitterEnsCyphers()
        super().__init__("twitter-ens")
        self.metadata["last_date_ingested"] = self.end_date
        if isinstance(self.end_date, datetime.datetime):
            self.metadata["last_date_ingested"] = self.end_date.strftime("%Y-%m-%d")

    def ingest_ens(self):
        print("Ingesting data...")
        print(self.scraper_data.keys())
        print(self.scraper_data['accounts'][0])

        df = pandas.DataFrame(self.scraper_data["accounts"])
        df = df.loc[(df["ens"].apply(self.filtering) == True) & (df["handle"].apply(self.filtering) == True)]
        self.scraper_data["accounts"] = df.to_dict("records")

        urls = self.save_json_as_csv(
            self.scraper_data["accounts"], f"ingestor_accounts_{self.asOf}"
        )
        self.cyphers.create_or_merge_twitter_wallets(urls)  # add wallet nodes
        self.cyphers.create_or_merge_twitter_alias(urls)  # add alias nodes
        # self.cyphers.create_or_merge_twitter(urls)  # add twitter account nodes

        self.cyphers.link_twitter_alias(urls)  # link alias to twitter account
        self.cyphers.link_wallet_alias(urls)  # link wallet to alias

    @staticmethod
    def filtering(x):
        return x.isascii() and len(x) > 0

    def run(self):
        self.ingest_ens()
        # self.save_metadata()


if __name__ == "__main__":

    ingestor = TwitterEnsIngestor()
    ingestor.run()
