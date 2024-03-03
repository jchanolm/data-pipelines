import logging
import time
from typing import Any
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)
import gql
from gql.transport.aiohttp import AIOHTTPTransport, log as gql_log

gql_log.setLevel(logging.WARNING)

class Requests:
    "This section handles all requests and API calls."
    def __init__(self) -> None:
        pass

    def get_request(self, 
                    url: str, 
                    params: dict|None = None, 
                    headers: dict|None = None, 
                    allow_redirects: bool = True, 
                    decode: bool = True, 
                    json: bool = False, 
                    ignore_retries: bool = False, 
                    retry_on_403: bool = False, 
                    retry_on_404: bool = True, 
                    counter: int = 0, 
                    max_retries: int = 10) -> dict | str | requests.models.Response | None:
        """This makes a GET request to a url and return the data.
        It can take params and headers as parameters following python's request library.
        The method can return the raw request content, you must then parse the content with the correct parser.
        By default, it handles it for you as a text or json with the arguments decode or json
            arguments:
                - url: The url for the get request
                - params: a dictionary containing the query parameter for the request
                - headers: a dictionary containing the headers for the request
                - allow_redirects: Wether or not to allow redirects
                - decode: Used to decode the returned value in the case were it is not a json object (ex: a README file)
                - json: Automaticallyt decodes the returned values as a JSON
                - ignore_retries: if True, will return None on any failure, if False will retry the query
                - retry_on_403: If True, will retry the query on a 403 Forbidden error
                - retry_on_404: If True, will retry the query on a 404 Missing error
                - max_retries: Change this to change the max number of allowed retries
        """
        time.sleep(counter * max_retries)
        if counter > max_retries:
            return None
        try:
            r = requests.get(url, params=params, headers=headers, allow_redirects=allow_redirects, verify=False)
            if not retry_on_404 and r.status_code == 404:
                return None
            elif r.status_code == 204:
                return None
            elif not ignore_retries and retry_on_403 and (r.status_code == 403 or "403 Forbidden" in r.content.decode("UTF-8")):
                logging.error(f"403 forbidden detected: {r.content.decode('UTF-8')}")
                return self.get_request(url, params=params, headers=headers, allow_redirects=allow_redirects, counter=counter + 1, max_retries=max_retries)
            elif not ignore_retries and r.status_code != 200:
                logging.error(f"Error: {r.content}")
                logging.error(f"Status code not 200: {r.status_code} Retrying in {counter*10}s (counter = {counter})...")
                return self.get_request(url, params=params, headers=headers, allow_redirects=allow_redirects, counter=counter + 1, max_retries=max_retries)
            if not json and decode:
                return r.content.decode("UTF-8")
            if json:
                return r.json()
            return r
        except Exception as e:
            logging.error(f"An unrecoverable exception occurred: {e}")
            return self.get_request(url, params=params, headers=headers, allow_redirects=allow_redirects, counter=counter + 1, max_retries=max_retries)

    def post_request(self, 
                     url: str, 
                     data: dict|None = None, 
                     json: dict|None = None, 
                     headers: dict|None = None, 
                     decode: bool = True, 
                     return_json: bool = False, 
                     ignore_retries: bool = False, 
                     retry_on_403: bool = False, 
                     retry_on_404: bool = True,
                     counter: int = 0, 
                     max_retries:int = 10) -> None | dict | str | requests.models.Response:
        """This makes a POST request to a url and return the data.
        It can take params. json payload and headers as parameters following python's request library.
        The method returns the raw request content, you must then parse the content with the correct parser.
            arguments:
                - url: The url for the get request
                - data: a dictionary containing the query parameter for the request
                - json: a dictionary containing the json payload for the request
                - headers: a dictionary containing the headers for the request
                - decode: Used to decode the returned value in the case were it is not a json object (ex: a README file)
                - return_json: Automaticallyt decodes the returned values as a JSON
                - ignore_retries: if True, will return None on any failure, if False will retry the query
                - retry_on_403: If True, will retry the query on a 403 Forbidden error
                - retry_on_404: If True, will retry the query on a 404 Missing error
                - max_retries: Change this to change the max number of allowed retries
        """

        time.sleep(counter * max_retries)
        if counter > max_retries:
            return None
        try:
            r = requests.post(url, data=data, json=json, headers=headers, verify=False)
            if not ignore_retries and r.status_code == 404 and retry_on_404:
                logging.error(f"Status code is 404: {url}\nRetrying {counter*10}s (counter = {counter})...")
                return self.post_request(url, data=data, json=json, headers=headers, decode=decode, ignore_retries=ignore_retries, retry_on_403=retry_on_403, retry_on_404=retry_on_404, counter=counter + 1, max_retries=max_retries)
            if not ignore_retries and r.status_code == 403 and retry_on_403:
                logging.error(f"Status code is 403: {url}\n{r.content}\nRetrying {counter*10}s (counter = {counter})...")
                return self.post_request(url, data=data, json=json, headers=headers, decode=decode, ignore_retries=ignore_retries, retry_on_403=retry_on_403, retry_on_404=retry_on_404, counter=counter + 1, max_retries=max_retries)
            if not ignore_retries and r.status_code <= 200 and r.status_code > 300:
                logging.error(f"Status code not 200: {r.status_code} Retrying {counter*10}s (counter = {counter})...")
                return self.post_request(url, data=data, json=json, headers=headers, decode=decode, ignore_retries=ignore_retries, retry_on_403=retry_on_403, retry_on_404=retry_on_404, counter=counter + 1, max_retries=max_retries)
            if return_json:
                return r.json()
            if decode:
                return r.content.decode("UTF-8")
            return r
        except Exception as e:
            logging.error(f"An unrecoverable exception occurred: {e}")
            return self.post_request(url, data=data, json=json, headers=headers, decode=decode, ignore_retries=ignore_retries, retry_on_403=retry_on_403, retry_on_404=retry_on_404, counter=counter + 1, max_retries=max_retries)

    def patch_request(self, 
                     url: str, 
                     data: dict|None = None, 
                     json: dict|None = None, 
                     headers: dict|None = None, 
                     decode: bool = True, 
                     return_json: bool = False, 
                     ignore_retries: bool = False, 
                     retry_on_403: bool = False, 
                     retry_on_404: bool = True,
                     counter: int = 0, 
                     max_retries:int = 10) -> None | dict | str | requests.models.Response:
        """This makes a PATCH request to a url and return the data.
        It can take params. json payload and headers as parameters following python's request library.
        The method returns the raw request content, you must then parse the content with the correct parser.
            arguments:
                - url: The url for the get request
                - data: a dictionary containing the query parameter for the request
                - json: a dictionary containing the json payload for the request
                - headers: a dictionary containing the headers for the request
                - decode: Used to decode the returned value in the case were it is not a json object (ex: a README file)
                - return_json: Automaticallyt decodes the returned values as a JSON
                - ignore_retries: if True, will return None on any failure, if False will retry the query
                - retry_on_403: If True, will retry the query on a 403 Forbidden error
                - retry_on_404: If True, will retry the query on a 404 Missing error
                - max_retries: Change this to change the max number of allowed retries
        """

        time.sleep(counter * max_retries)
        if counter > max_retries:
            return None
        try:
            r = requests.patch(url, data=data, json=json, headers=headers, verify=False)
            if not ignore_retries and r.status_code == 404 and retry_on_404:
                logging.error(f"Status code is 404: {url}\nRetrying {counter*10}s (counter = {counter})...")
                return self.post_request(url, data=data, json=json, headers=headers, decode=decode, ignore_retries=ignore_retries, retry_on_403=retry_on_403, retry_on_404=retry_on_404, counter=counter + 1, max_retries=max_retries)
            if not ignore_retries and r.status_code == 403 and retry_on_403:
                logging.error(f"Status code is 403: {url}\n{r.content}\nRetrying {counter*10}s (counter = {counter})...")
                return self.post_request(url, data=data, json=json, headers=headers, decode=decode, ignore_retries=ignore_retries, retry_on_403=retry_on_403, retry_on_404=retry_on_404, counter=counter + 1, max_retries=max_retries)
            if not ignore_retries and r.status_code <= 200 and r.status_code > 300:
                logging.error(f"Status code not 200: {r.status_code} Retrying {counter*10}s (counter = {counter})...")
                return self.post_request(url, data=data, json=json, headers=headers, decode=decode, ignore_retries=ignore_retries, retry_on_403=retry_on_403, retry_on_404=retry_on_404, counter=counter + 1, max_retries=max_retries)
            if return_json:
                return r.json()
            if decode:
                return r.content.decode("UTF-8")
            return r
        except Exception as e:
            logging.error(f"An unrecoverable exception occurred: {e}")
            return self.patch_request(url, data=data, json=json, headers=headers, decode=decode, ignore_retries=ignore_retries, retry_on_403=retry_on_403, retry_on_404=retry_on_404, counter=counter + 1, max_retries=max_retries)

    def call_the_graph_api(self, 
                           graph_url: str, 
                           query: str, 
                           variables: dict = {}, 
                           result_names: list = [], 
                           counter: int = 0, 
                           max_retries: int = 10):
        """
        Helper function to call the Graph API to handle the graphQL handling.
        arguments:
            - graph_url: the URL for the API endpoint of the subgraph to target
            - query: the graphQL query string as a string (not a gql object!) 
            - variables: The dictionary of variables to pass to the graphQL query  
            - result_names: The list of expected variable names for the response (used to check that the query was succesful)
            - max_retries: The number of max_retries allowed.
        """
        time.sleep(counter)
        if counter > max_retries:
            time.sleep(counter)
            return None
        gql_query =  gql.gql(query)
        transport = AIOHTTPTransport(url=graph_url)
        client = gql.Client(transport=transport, fetch_schema_from_transport=True)
        try:
            result = client.execute(gql_query, variables)
            for result_name in result_names:
                if result.get(result_name, None) == None:
                    logging.error(f"The Graph API did not return {result_name}, counter: {counter}")
                    return self.call_the_graph_api(graph_url, query, variables, result_names, counter=counter + 1, max_retries=max_retries)
        except Exception as e:
            logging.error(f"An exception occured getting The Graph API {e} counter: {counter} client: {client}")
            return self.call_the_graph_api(graph_url, query, variables, result_names, counter=counter + 1, max_retries=max_retries)
        return result