import os
import time
from . import Requests
from hexbytes import HexBytes
from .web3Utils import Web3Utils


class Etherscan(Requests):
    def __init__(self, max_retries=5) -> None:
        self.chains = ["ethereum", "optimism", "polygon", "arbitrum", "binance", "goerli"]
        self.etherscan_api_url = {
            "ethereum": "https://api.etherscan.io/api",
            "goerli": "https://api-goerli.etherscan.io/api",
            "optimism": "https://api-optimistic.etherscan.io/api",
            "polygon": "https://api.polygonscan.com/api",
            "arbitrum": "https://api.arbiscan.io/api",
            "binance": "https://api.bscscan.com/api",
        }
        self.etherscan_api_keys = {
            "ethereum": os.environ.get("ETHERSCAN_API_KEY", ""),
            "goerli": os.environ.get("ETHERSCAN_API_KEY", ""),
            "optimism": os.environ.get("ETHERSCAN_API_KEY_OPTIMISM", ""),
            "polygon": os.environ.get("ETHERSCAN_API_KEY_POLYGON", ""),
            "arbitrum": os.environ.get("ETHERSCAN_API_KEY_ARBITRUM", ""),
            "binance": os.environ.get("ETHERSCAN_API_KEY_BINANCE", ""),
        }
        self.headers = {"Content-Type": "application/json"}
        self.pagination_count = 1000
        self.max_retries = max_retries
        self.w3utils = Web3Utils()
        super().__init__()

    def is_valid_response(
        self, response: dict | list | str | int, response_type: type = dict, is_list: bool = True
    ) -> bool:
        "Checks if the body of the response is valid by checking if it matches the expected type."
        if response == None:
            return False
        if type(response) != response_type:
            return False
        if response_type == dict and "result" not in response:
            return False
        if is_list and response_type == dict and type(response.get("result", None)) != list:
            return False
        if response_type == list and len(response) == 0:
            return False
        return True

    def convert_etherscan_log_to_web3_log(self, log: dict) -> dict:
        log["transactionHash"] = HexBytes(log["transactionHash"])
        log["blockHash"] = HexBytes(log["blockHash"])
        log["topics"] = [HexBytes(topic) for topic in log["topics"]]
        log["removed"] = False
        log["blockNumber"] = int(log["blockNumber"], 16)
        if log["logIndex"] != "0x":
            log["logIndex"] = int(log["logIndex"], 16)
        else:
            log["logIndex"] = 0
        if log["transactionIndex"] != "0x":
            log["transactionIndex"] = int(log["transactionIndex"], 16)
        else:
            log["transactionIndex"] = 0
        return log

    def get_last_block_number(self, chain: str = "ethereum", counter: int = 0) -> int | None:
        """
        Helper method to get the latests block number.
        parameters:
            - chain: (ethereum|optimism|polygon) the chain of interest
        """
        time.sleep(counter)
        if counter > self.max_retries:
            return None

        params = {"module": "proxy", "action": "eth_blockNumber", "apikey": self.etherscan_api_keys[chain]}

        content = self.get_request(self.etherscan_api_url[chain], params=params, headers=self.headers, json=True)
        if self.is_valid_response(content):
            block_number = int(content["result"], 16)
        else:
            return self.get_last_block_number(chain=chain, counter=counter + 1)
        return block_number

    def get_token_holders(self, tokenAddress: str, page: int = 1, offset: int = 1000, chain: str = "ethereum", counter: int = 0) -> list[dict] | None:
        """
        Helper method to get the token holders of any token from Etherscan
        parameters:
            - tokenAddress: (address) The contract address that is of interest
            - offset: (int) To change the number of results returned by each query, max 1000. You should probably not touch this.
            - chain: (ethereum|optimism|polygon) the chain of interest
        """
        results = []
        time.sleep(counter)
        if counter > self.max_retries:
            return None

        params = {
            "apikey": self.etherscan_api_keys[chain],
            "module": "token",
            "action": "tokenholderlist",
            "contractaddress": tokenAddress,
            "page": page,
            "offset": offset,
        }
        content = self.get_request(self.etherscan_api_url[chain], params=params, headers=self.headers, json=True)
        if self.is_valid_response(content):
            result = content["result"]
            if len(result) > 0:
                nextResult = self.get_token_holders(tokenAddress, page=page + 1, offset=offset)
                if nextResult:
                    results.extend(nextResult)
            results.extend(result)
        else:
            return self.get_token_holders(tokenAddress, page=page, offset=offset, counter=counter + 1)
        return results

    def get_token_information(self, tokenAddress: str, chain: str = "ethereum", counter: int = 0) -> dict | None:
        """
        Helper method to get the token metadata of any token from Etherscan
        parameters:
            - tokenAddress: (address) The contract address that is of interest
            - chain: (ethereum|optimism|polygon) the chain of interest
        """
        time.sleep(counter)
        if counter > self.max_retries:
            return None

        params = {
            "module": "token",
            "action": "tokeninfo",
            "contractaddress": tokenAddress,
            "apikey": self.etherscan_api_keys[chain]
        }

        content = self.get_request(self.etherscan_api_url[chain], params=params, headers=self.headers, json=True)
        if self.is_valid_response(content) and len(content["result"]) > 0:
            result = content["result"]
            return result[0]
        else:
            self.get_token_information(tokenAddress, counter=counter + 1)

    def get_contract_deployer(self, contractAddresses: str, chain: str = "ethereum", counter: int = 0) -> dict | None:
        """
        Helper method to get the address of the deployer of a contract.
        parameters:
            - contractAddresses: ([address]) An array of contract addresses, up to 5 address!
            - chain: (ethereum|optimism|polygon) the chain of interest
        """

        assert len(contractAddresses) <= 5, "contractAddress cannot be more than 5 addresses"

        time.sleep(counter)
        if counter > self.max_retries:
            return None

        params = {
            "module": "contract",
            "action": "getcontractcreation",
            "contractaddresses": ",".join(contractAddresses),
            "apikey": self.etherscan_api_keys[chain],
        }
        content = self.get_request(self.etherscan_api_url[chain], params=params, headers=self.headers, json=True)
        if self.is_valid_response(content):
            result = content["result"]
            return result
        else:
            self.get_contract_deployer(contractAddresses, counter=counter + 1)

    def get_event_logs(
        self,
        address: str,
        fromBlock: int | None = None,
        toBlock: int | None = None,
        topic0: str | None = None,
        page: int = 1,
        offset: int = 1000,
        chain: str = "ethereum",
        counter: str = 0,
    ) -> list[dict] | None:
        """
        Helper method to get the transactions logs of a smart contract.
        parameters:
            - address: (address) A contract addresses.
            - fromBlock: (int) The starting block to get transactions from
            - toBlock: (int) The end block to get transactions from
            - topic0: (hex) To filter on the first topic.
            - chain: (ethereum|optimism|polygon) the chain of interest
        """
        time.sleep(counter)
        if counter > self.max_retries:
            return None

        results = []
        params = {
            "module": "logs",
            "action": "getLogs",
            "address": address,
            "fromBlock": fromBlock,
            "toBlock": toBlock,
            "topic0": topic0,
            "page": page,
            "offset": offset,
            "apikey": self.etherscan_api_keys[chain],
        }
        content = self.get_request(self.etherscan_api_url[chain], params=params, headers=self.headers, json=True)
        if self.is_valid_response(content):
            result = content["result"]
            if len(result) > 0:
                nextResult = self.get_event_logs(
                    address,
                    fromBlock=fromBlock,
                    toBlock=toBlock,
                    topic0=topic0,
                    chain=chain,
                    page=page + 1,
                    offset=offset,
                )
                if nextResult:
                    results.extend(nextResult)
            results.extend(result)
        else:
            return self.get_event_logs(
                address,
                fromBlock=fromBlock,
                toBlock=toBlock,
                topic0=topic0,
                chain=chain,
                page=page,
                offset=offset,
                counter=counter + 1,
            )
        return results

    def parse_event_logs(
        self,
        contractAddress: str,
        logs: list[dict],
        eventName: str,
        topic: str | None = None,
        abi: dict | None = None,
        chain: str = "ethereum",
    ) -> list[dict] | None:
        """
        Parse the event logs from a transaction.
        If the ABI is not provided in the parameters, it automatically retrieves the abi of the contract using the etherscan API.
        Parameters:
          - contractAddress: The address of the deployed contract.
          - logs: The list of logs to parse
          - eventName: The name of the event to parse.
          - topic: Optional filter to filter for only a given topic (only valid for topic0)
          - abi: Optional a dictionary object of the contract ABI
          - chain: The chain where the contract is deployed
        """
        if not abi:
            abi = self.get_smart_contract_ABI(contractAddress, chain)
        contract = self.w3utils.get_smart_contract(contractAddress, abi)

        if topic:
            logs = [log for log in logs if topic in log["topics"]]

        results = []
        for log in logs:
            w3log = self.convert_etherscan_log_to_web3_log(log)
            result = contract.events[eventName]().processLog(w3log)
            results.append(result)
        return results

    def get_decoded_event_logs(
        self,
        address: str,
        eventName: str,
        fromBlock: int | None = None,
        toBlock: int | None = None,
        topic0: str | None = None,
        abi: dict | None = None,
        chain: str = "ethereum",
    ) -> list[dict] | None:
        """
        Wrapper method that calls internal functions to get the decoded transactions logs of a smart contract for a given Event.
        It is recommended to use the topic0 filter to make the queries faster!
        parameters:
            - address: (address) A contract addresses.
            - eventName: (string) The name of the event. Must be the exact ABI spelling.
            - fromBlock: (int) The starting block to get transactions from
            - toBlock: (int) The end block to get transactions from
            - topic0: (hex) To filter on the first topic.
            - chain: (ethereum|optimism|polygon) the chain of interest
        """
        raw_logs = self.get_event_logs(address, fromBlock=fromBlock, toBlock=toBlock, topic0=topic0, chain=chain)
        decoded_logs = self.parse_event_logs(address, raw_logs, eventName, topic=topic0, abi=abi, chain=chain)
        return decoded_logs

    def get_internal_transactions(
        self,
        address: str,
        startBlock: int,
        endBlock: int,
        sort: str = "asc",
        page: int = 1,
        offset: int = 10000,
        chain: str = "ethereum",
        counter: int = 0,
    ) -> list[dict] | None:
        """
        Helper method to get the internal transactions of a smart contract.
        parameters:
            - address: (address) A contract addresses.
            - startBlock: (int) The starting block to get transactions from
            - endBlock: (int) The end block to get transactions from
            - sort: (asc|desc) The sorting order.
            - chain: (ethereum|optimism|polygon) the chain of interest
        """
        results = []
        time.sleep(counter)
        if counter > self.max_retries:
            return None

        params = {
            "module": "account",
            "action": "txlistinternal",
            "address": address,
            "startblock": startBlock,
            "endblock": endBlock,
            "page": page,
            "offset": offset,
            "sort": sort,
            "apikey": self.etherscan_api_keys[chain],
        }

        content = self.get_request(self.etherscan_api_url[chain], params=params, headers=self.headers, json=True)
        if content and (
            (type(content) == dict and content["message"] == "No transactions found")
            or "No transactions found" in content
        ):
            return results
        if self.is_valid_response(content):
            result = content["result"]
            if len(result) > 0:
                nextResult = self.get_internal_transactions(
                    address, startBlock, endBlock, sort=sort, page=page + 1, offset=offset
                )
                if nextResult:
                    results.extend(nextResult)
            results.extend(result)
        else:
            return self.get_internal_transactions(
                address, startBlock, endBlock, sort=sort, page=page + 1, offset=offset, counter=counter + 1
            )
        return results

    def get_smart_contract_ABI(self, address: str, chain: str = "ethereum", counter: int = 0) -> dict | None:
        """
        Helper method to get the ABI of a published smart contract. The smart contract needs to have verified its ABI.
        parameters:
            - address: (address) A contract addresses.
            - chain: (ethereum|optimism|polygon) the chain of interest
        """
        time.sleep(counter)
        if counter > self.max_retries:
            return None

        params = {
            "module": "contract",
            "action": "getabi",
            "address": address,
            "apikey": self.etherscan_api_keys[chain],
        }

        content = self.get_request(self.etherscan_api_url[chain], params=params, headers=self.headers, json=True)
        if self.is_valid_response(content, is_list=False):
            result = content["result"]
            return result
        else:
            self.get_smart_contract_ABI(address, counter=counter + 1)
