from tqdm import tqdm
from ..helpers import Scraper
import os

DEBUG = os.environ.get("DEBUG", False)

class ENSScraper(Scraper):
    def __init__(self, bucket_name="ens"):
        super().__init__(bucket_name)
        self.graph_url = "https://api.thegraph.com/subgraphs/name/ensdomains/ens"
        self.last_block = self.metadata.get("last_block", 0)
        self.interval = 500
        self.chunk_size = 10000

    def clean_events(self, events):
        results = []
        for event in events:
            if "registrant" in event:
                event["type"] = "NameRegistered"
                event["registrant"] = event["registrant"].get("id", None)
            elif "newOwner" in event:
                event["type"] = "NameTransferred"
                event["newOwner"] = event["newOwner"].get("id", None)
            else:
                event["type"] = "NameRenewed"
            results.append(event)
        return results

    def detect_registration(self, clean_events):
        registrations = []
        registration_detected = False
        for event in clean_events:
            if event["type"] == "NameRegistered":
                tmp_registration = {
                    "expiryDate": event.get("expiryDate", None),
                    "registrant": event.get("registrant", None),
                    "transactionID": event.get("transactionID", None),
                    "blockNumber": event.get("blockNumber", None)
                }
                registration_detected = True
            if registration_detected and event["type"] == "NameTransferred" and event.get("transactionID", None) == tmp_registration["transactionID"]:
                tmp_registration["owner"] = event.get("newOwner", None)
                registrations.append(tmp_registration)
                tmp_registration = {}
                registration_detected = False
        return registrations

    # Complex logic but clean to get transfers
    def detect_transfers(self, clean_events):
        transfers = []
        transfer_detected = False
        for event in clean_events:
            if transfer_detected and event["type"] == "NameTransferred":
                owner = event.get("newOwner", None)
                tmp_transfer["to"] = owner
                transfers.append(tmp_transfer)

            tmp_transfer = {}
            transfer_detected = False

            if event["type"] == "NameTransferred":
                owner = event.get("newOwner", None)
                if not self.is_zero_address(owner):
                    tmp_transfer = {
                        "from": owner,
                        "transactionID": event.get("transactionID", None),
                        "blockNumber": event.get("blockNumber", None)
                    }
                    transfer_detected = True
        return transfers

    def clean_registration_data(self, data):
        domain = {
            "labelName": data.get("registration", {}).get("domain", {}).get("labelName", None),
            "name": data.get("registration", {}).get("domain", {}).get("name", None),
            "createdAt": data.get("registration", {}).get("domain", {}).get("createdAt", None),
            "owner": data.get("registration", {}).get("domain", {}).get("owner", None),
            "resolvedAddress": data.get("registration", {}).get("domain", {}).get("resolvedAddress", None)
        }

        if domain["owner"]:
            domain["owner"] = domain["owner"].get("id", None)
        if domain["resolvedAddress"]:
            domain["resolvedAddress"] = domain["resolvedAddress"].get("id", None)

        clean_events = self.clean_events(data["registration"].get("events", []))
        registrations = self.detect_registration(clean_events)
        for registration in registrations:
            registration["name"] = domain["name"]
        transfers = self.detect_transfers(clean_events)
        for transfer in transfers:
            transfer["name"] = domain["name"]
        return domain, registrations, transfers

    def get_ens_domains(self):
        results = []
        pbar = tqdm(range(self.chunk_size // self.interval), desc=f"Getting {self.chunk_size} ENS regsitration and event data from the Graph...")
        for i in pbar:
            pbar.set_description(f"Getting {self.chunk_size} ENS regsitration and event data from the Graph... BlockNumber is at: {self.last_block}")
            variables = {"first": self.interval, "cutoff": self.last_block}
            query = """
            query($first: Int!, $cutoff: Int!) {
                nameRegistereds(first: $first, orderBy: blockNumber, orderDirection: asc, where: {blockNumber_gt: $cutoff}) {
                    blockNumber
                    registration {
                        cost
                        registrationDate
                        events {
                            ... on NameRegistered {
                                id
                                expiryDate
                                registrant {
                                    id
                                }
                                transactionID
                            }
                            ... on NameRenewed {
                                id
                                expiryDate
                                transactionID
                            }
                            ... on NameTransferred {
                                id
                                newOwner {
                                    id
                                }
                                transactionID
                            }
                            blockNumber
                        }
                        domain {
                            labelName
                            name
                            owner {
                                id
                            }
                            resolvedAddress {
                                id
                            }
                            createdAt
                        }
                    }
                    transactionID
                }
            }
            """
            data = self.call_the_graph_api(self.graph_url, query, variables, ["nameRegistereds"])
            nameRegistereds = data["nameRegistereds"]
            if nameRegistereds == None or len(nameRegistereds) == 0:
                break
            results.extend(nameRegistereds)
            self.last_block = nameRegistereds[-1]["blockNumber"]
            self.metadata["last_block"] = self.last_block
        self.data["domains"] = []
        self.data["registrations"] = []
        self.data["transfers"] = []
        for result in tqdm(results, desc="Cleaning results..."):
            domain, registrations, transfers = self.clean_registration_data(result)
            self.data["domains"].append(domain)
            self.data["registrations"].extend(registrations)
            self.data["transfers"].extend(transfers)
        return len(results) > 0

    def get_ens_subdomains(self):
        pass

    def run(self):
        more_data = True
        chunk_id = 0
        while more_data:
            more_data = self.get_ens_domains()
            self.save_data(chunk_prefix=chunk_id)
            self.save_metadata()
            chunk_id += 1
            if DEBUG and chunk_id > 10:
                break

if __name__ == "__main__":
    S = ENSScraper()
    S.run()
