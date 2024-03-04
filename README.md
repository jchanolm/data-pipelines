# arbitrum-data

## Tl;dr 

This repo contains scripts and data for building a knowedge graph of the entire Arbitrum ecosystem,
which connects on and off-chain data about grantees, grants, voters, proposals, etc. 

I'm passionate about making web3 data more accessible and actionable and will continue to expand
and enhance this database to make it more useful for the Arbitrum ecosystem.


## Structure

All code / data is in `notebooks/`.

Each source has its own directory, i.e. `snapshot/`, `discourse/`

Each directory contains a Jupyter notebook (scraping/ingestion code) as well as `.csv` files with the scraped/ingested data.

Each directory also contains a README.md with additional context, i.e. stats, ontology, etc.


## Process

### Step 1 - Manual Data Collection

I started with manual data collection to become familiar with the grants data landscape.
I started with primary sources and made sure to collect supporting documents and other metadata for each grantee.
The raw data is available in `notebooks/initial-ingest/inputs/`

After that, I ingested the data into Neo4J, a popular graph database solution.
You can spin up your own (free) cloud Neo4J instance through Neo4J aura: https://neo4j.com/cloud/aura-free/

<img width="932" alt="Screenshot 2024-03-03 at 12 37 55 PM" src="https://github.com/jchanolm/arbitrum-data/assets/160365885/c90054de-498b-4094-aaa3-7cdd4333d8c2">


### Step 2 - Snapshot Data Pipeline

Next I built a pipeline that scrapes all Snapshot voters and proprosals. 

`notebooks/snapshot`

I also linked Grantees to the Snapshot proprosals which approved their funding, which enables queries like
`give me every wallet that voted to approve funding for multiple (passing) STIP grant proprosals`



## Todos 

**1. Publish dashboard to make the graph more accessible**

**2. Build pipelines to programatically expand and enrich the graph**
- Ingest all Arbitrum Foundation forum/Discourse posts
- Ingest all Tally proprosals/votes
- Ingest all contributors to grantee Githubs
- Ingest all Arbitrum delegation data
- Ingest Gitcoin grants funded by wallets that have voted on grant-related Arbitrum Snapshot proprosals
- Extract entities/links/etc from documents to identify grantee affiliates

3. Build dashboard with NeoDash (https://github.com/neo4j-labs/neodash) to make graph more accessible

4. Identify additional Arbitrum ecosystem grants and grantees
   - Gitcoin grantees who received Arbitrum matching fund
   - Grantees who received grants from other ecosystems
