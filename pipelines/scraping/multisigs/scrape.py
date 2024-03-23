from ..helpers import Scraper
import os
import logging

DEBUG = os.environ.get("DEBUG", False)

class MultisigScraper(Scraper):
    def __init__(self, bucket_name="multisigs"):
        super().__init__(bucket_name)
        self.graph_url = "https://api.thegraph.com/subgraphs/name/multis/multisig-mainnet"
        self.cutoff_timestamp = self.metadata.get("cutoff_timestamp", 0)
        self.interval = 1000
        self.data["multisig"] = []
        self.data["transactions"] = []

    def get_multisig_and_transactions(self):
        skip = 0
        wallets = ["init"]
        transactions = ["init"]
        retry = 0
        if DEBUG:
            req = 0
            max_req = 5
        while len(wallets) > 0 or len(transactions) > 0:
            print(skip)
            if DEBUG:
                req += 1
                if req > max_req:
                    break
            variables = {"first": self.interval, "skip": skip, "cutoff": self.cutoff_timestamp}
            query = """query($first: Int!, $skip: Int!, $cutoff: BigInt!) {
                        wallets(first: $first, skip: $skip, orderBy: stamp, orderDirection:desc, where:{stamp_gt: $cutoff}) {
                            id
                            creator
                            network
                            stamp
                            factory
                            owners
                            balanceEther
    		                required
                        }
                        transactions(first: $first, skip: $skip, orderBy: stamp, orderDirection:desc, where:{stamp_gt: $cutoff}) {
                            stamp
                            block
                            hash
                            wallet {
                                id
                            }
                            destination
                        }
                    }"""

            result = self.call_the_graph_api(self.graph_url, query, variables, ["wallets", "transactions"])
            if result != None:
                wallets = result["wallets"]
                transactions = result["transactions"]
                for wallet in wallets:
                    for owner in wallet["owners"]:
                        tmp = {
                            "multisig": wallet["id"],
                            "ownerAddress": owner,
                            "threshold": int(wallet["required"]),
                            "occurDt": int(wallet["stamp"]),
                            "network": wallet["network"],
                            "factory": wallet["factory"],
                            # "version": wallet["version"],
                            "creator": wallet["creator"], 
                            "timestamp": wallet["stamp"]
                        }
                        self.data["multisig"].append(tmp)
                for transaction in transactions:
                    tmp = {
                        "timestamp": transaction["stamp"],
                        "block": transaction["block"],
                        "from": transaction["wallet"]["id"],
                        "to": transaction["destination"],
                        "txHash": transaction["hash"]
                    }
                    self.data["transactions"].append(tmp)
                skip += self.interval
                retry = 0
                logging.info(f"Query success, skip is at: {skip}")
            else:
                retry += 1
                if retry > 10:
                    skip += self.interval
                logging.error(f"Query unsuccessful, skip is at: {skip}")
        logging.info("Found {} multisig and {} transactions".format(
            len(self.data["multisig"]), len(self.data["transactions"])))

    def run(self):
        self.get_multisig_and_transactions()
        self.metadata["cutoff_timestamp"] = int(self.runtime.timestamp())
        self.save_metadata()
        self.save_data()

if __name__ == "__main__":
    scraper = MultisigScraper()
    scraper.run()
