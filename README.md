# Grant data ingestion framework

This repo contains scripts for scraping on and off-chain data relevant for understanding the Web3 grants ecosystem.

<img width="932" alt="Screenshot 2024-03-03 at 12 37 55 PM" src="https://github.com/jchanolm/arbitrum-data/assets/160365885/c90054de-498b-4094-aaa3-7cdd4333d8c2">


Some examples: DAO proposals funding grants, wallet addresses and Github activity of grant recipients, forum posts related to grant discussions...etc


The code is organized into two three modules:
- `scraping`: scripts to collect data from relavant data sources, i.e. Gitcoin.
- `ingestion` scripts to ingest data into Neo4J, a graph database, with a common ontology. For instance, you can query for wallets that voted on Snapshot proposals that funded DeFi focused grants, donated to them on Gitcoin, or posted about them on Discourse.
- `postProcessing`: scripts to enrich data in Neo4J with additional attributes to facilitate downstream analysis.

## Install
---- 
Create a virtual environment using **Python 3.10. 
After that, install requirements w/ `pip3 install -r requirements.txt`


## Docker Image
-----
Package the module into a Docker image for cloud deployment or local use. 
To build the image, run 
`docker build . -t arbitrum-pipelines`
 
Then, execute the modules directly through the Docker image 
by replacing "module", "service", and "action" with the desired pipeline module. 

I.e.  `docker run --env-file .env arbitrum-pipelines python3 -m pipelines.[module].[service].[action]`

`.env` template is in dotenv.template.

For your convenience, the repo also contains a `Docker Compose` file. 
