from .cyphers import SnapshotCyphers
from ..helpers import Ingestor
from ...helpers import Utils
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging
import datetime 
import logging 
import pandas 
import json 
import re 

class SnapshotIngestor(Ingestor):
    def __init__(self):
        self.cyphers = SnapshotCyphers()
        super().__init__("snapshot")
        self.metadata["last_date_ingested"] = self.end_date
        if isinstance(self.end_date, datetime.datetime):
            self.metadata["last_date_ingested"] = self.end_date.strftime("%Y-%m-%d")

    def process_spaces(self):
        space_data = {
            "spaces": [],
            "strategy_list": [],
            "ens": [],
            "members": [],
            "admins": [],
            "tokens": [],
            "strategy_relationships": [],
        }
        for entry in self.scraper_data["spaces"]:
            current_dict = {}
            current_dict["snapshotId"] = entry["id"]
            current_dict[
                "profileUrl"
            ] = f"https://cdn.stamp.fyi/space/{current_dict['snapshotId']}?s=160&cb=a1b40604488c19a1"
            current_dict["name"] = entry["name"]
            current_dict["description"] = self.cyphers.sanitize_text(entry.get("about", ""))
            current_dict["chainId"] = entry.get("network", "")
            current_dict["symbol"] = entry.get("symbol", "")
            current_dict["twitterHandle"] = entry.get("twitter", "")
            current_dict['github'] = entry.get('github', )
            current_dict["twitterHandle"] = current_dict["twitterHandle"] if current_dict["twitterHandle"] else ""

            if "strategies" in entry and entry["strategies"]:
                for strategy in entry["strategies"]:
                    space_data["strategy_list"].append({"space": entry["id"], "strategy": strategy})

            for member in entry["members"]:
                space_data["members"].append({"space": entry["id"], "address": member.lower()})

            for admin in entry["admins"]:
                space_data["admins"].append({"space": entry["id"], "address": admin.lower()})

            if "ens" in entry:
                ens = entry["ens"]
                space_data["ens"].append(
                    {
                        "name": entry["id"],
                        "address": ens["address"].lower(),
                        "tokenId": ens["token_id"],
                        "contractAddress": ens["trans"]["rawContract"]["address"],
                    }
                )

            space_data["spaces"].append(current_dict)

        space_data["ens"] = pandas.DataFrame(space_data["ens"]).drop_duplicates(["name"]).to_dict("records")

        for item in space_data["strategy_list"]:
            space = item.get("space", None)
            entry = item.get("strategy", None)
            params = entry.get("params", None)
            address = params.get("address", None)

            isAddress = re.compile("^0x[a-fA-F0-9]{40}$")
            if space and address and isAddress.match(address):
                tmp = {
                "space": space,
                "token": address.lower()
                }
                space_data["strategy_relationships"].append(current_dict)
            
                tmp = {
                    "contractAddress": address.lower(),
                    "symbol": params.get("symbol", ""),
                    "decimals": params.get("decimals", -1),
                }
                space_data["tokens"].append(tmp)
        
        return space_data
  
    def process_proposals(self):
        proposal_data = {"proposals": []}
        for entry in self.scraper_data["proposals"]:
            current_dict = {}
            current_dict["snapshotId"] = entry["id"]
            current_dict["url"] = entry["ipfs"]
            current_dict["address"] = entry["author"].lower() or ""
            current_dict["createdAt"] = entry["created"] or 0
            current_dict["type"] = entry["type"] or -1
            current_dict["spaceId"] = entry["space"]["id"]

            current_dict["title"] = self.cyphers.sanitize_text(entry["title"])
            current_dict["text"] = self.cyphers.sanitize_text(entry["body"])

            choices = self.cyphers.sanitize_text(json.dumps(entry["choices"]))

            current_dict["startDt"] = entry["start"] or 0
            current_dict['choices'] = choices
            current_dict["endDt"] = entry["end"] or 0
            current_dict["state"] = entry["state"] or ""
            current_dict["url"] = entry["link"].strip() or ""

            proposal_data["proposals"].append(current_dict)

        proposal_df = (
                pandas.DataFrame(proposal_data["proposals"]).drop_duplicates("snapshotId").dropna(subset=["address"])
            )
        proposal_data["proposals"] = proposal_df.to_dict("records")
        return proposal_data
    \
        def process_votes(self):
            vote_data = {"votes": []}
            for entry in self.scraper_data["votes"]:
                current_dict = {}
                current_dict["id"] = entry["id"]
                current_dict["voter"] = entry["voter"].lower() or ""
                current_dict["votedAt"] = entry["created"]
                current_dict["ipfs"] = entry["ipfs"]

                try:
                    current_dict["choice"] = self.cyphers.sanitize_text(json.dumps(entry["choice"]))
                    current_dict["proposalId"] = entry["proposal"]["id"]
                    current_dict["spaceId"] = entry["space"]["id"]
                except:
                    continue

                vote_data["votes"].append(current_dict)

            vote_data["votes"] = pandas.DataFrame(vote_data["votes"]).drop_duplicates("id").to_dict("records")
            return vote_data

    def ingest_spaces(self):
        print("Ingesting spaces...")
        space_data = self.process_spaces()

        # add space nodes
        # urls = self.save_json_as_csv(space_data["spaces"], f"ingestor_spaces_{self.asOf}")
        # self.cyphers.create_or_merge_spaces(urls)

        print(space_data['spaces'][0:5])
        # add twitter nodes, twitter-space relationships
        twitter_df = pandas.DataFrame(space_data["spaces"])[
            ["snapshotId", "twitterHandle"]
        ].drop_duplicates(subset=["twitterHandle"])
        twitter_df = twitter_df[twitter_df["twitterHandle"] != ""]
        twitter_dict = twitter_df.to_dict("records")
        urls = self.save_json_as_csv(twitter_dict, f"ingestor_twitter_{self.asOf}")
        self.cyphers.create_or_merge_space_twitter(urls)
        self.cyphers.link_space_twitter(urls)

        # add token nodes
        urls = self.save_json_as_csv(space_data["tokens"], f"ingestor_tokens_{self.asOf}")
        self.cyphers.create_or_merge_space_tokens(urls)

        ## github df
        github_df = pandas.DataFrame(space_data['spaces'])[['snapshotId', 'github']]
        github_df.drop_duplicates(subset=['github'], inplace=True) 
        github_urls = self.save_df_as_csv(github_df, f"data_spaces_githubs_{self.asOf}")
        self.cyphers.create_connect_githubs(github_urls)

        # add strategy relationships (token-space)
        urls = self.save_json_as_csv(
            space_data["strategy_relationships"], f"ingestor_strategies_{self.asOf}"
        )
        self.cyphers.link_strategies(urls)

        # add ens nodes, ens relationships, space-alias relationships
        urls = self.save_json_as_csv(space_data["ens"], f"ingestor_ens_{self.asOf}")
        self.cyphers.create_or_merge_space_ens(urls)
        self.cyphers.link_space_ens(urls)
        self.cyphers.link_space_alias(urls)

        # add member wallet nodes, member-space relationships
        urls = self.save_json_as_csv(space_data["members"], f"ingestor_members_{self.asOf}")
        self.cyphers.create_or_merge_members(urls)
        self.cyphers.link_member_spaces(urls)

        # add admin wallet nodes, admin-space relationships
        urls = self.save_json_as_csv(space_data["admins"], f"ingestor_admins_{self.asOf}")
        self.cyphers.create_or_merge_members(urls)
        self.cyphers.link_admin_spaces(urls)

    def ingest_proposals(self):
        print("Ingesting proposals...")
        proposal_data = self.process_proposals()

        # add proposal nodes, proposal-space relationships, proposal author wallet nodes, proposal-author relationships
        urls = self.save_json_as_csv(proposal_data["proposals"], f"ingestor_proposals_{self.asOf}")
        self.cyphers.create_or_merge_proposals(urls)
        self.cyphers.link_proposal_spaces(urls)
        self.cyphers.create_or_merge_authors(urls)
        self.cyphers.link_proposal_authors(urls)

    def ingest_votes(self):
        print("Ingesting votes...")
        vote_data = self.process_votes()

        wallet_dict = [
            {"address": wallet}
            for wallet in set([x["voter"] for x in vote_data["votes"]])
            if wallet != "" and wallet is not None
        ]

        # add vote wallet nodes
        urls = self.save_json_as_csv(wallet_dict, f"ingestor_wallets_{self.asOf}")
        self.cyphers.create_or_merge_voters(urls)

        # add vote relationships (proposal-wallet)
        urls = self.save_json_as_csv(vote_data["votes"], f"ingestor_votes_{self.asOf}")
        self.cyphers.link_votes(urls)


    def run(self):
        self.ingest_spaces()
        # self.ingest_proposals()
        # self.ingest_votes()

        # self.save_metadata()
        # self.save_data()
        logging.info("Run complete!")

if __name__ == '__main__':
    ingestor = SnapshotIngestor()
    ingestor.run()

