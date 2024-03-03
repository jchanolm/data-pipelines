# arbitrum-data

## Tl;dr 

My objective was to build a knowledge graph covering the Arbitrum ecosystem, including grantees, grantee identifiers (i.e. Funding Wallet, Github), and grant metadata (i.e. amount, grantee submission document, related votes.)

I didn't hear about this opportunity until last Thursday, so I haven't completed as much as I like.

That said, I'm passionate about making web3 data more accessible and actionable and will continue to expand
and enhance this database to make it more useful for downstream analytics use cases, i.e. generating Sybil/Airdrop datasets.

**Quick highlights**

- 165 Grantees supported by 9 grant initiatives
- 75 Grantee Github accounts
- 108 Grantee Funding Wallet addresses
  
### Step 1

I started with manual data collection to become familiar with the grants data landscape.
I started with primary sources and made sure to collect supporting documents and other metadata for each grantee.
Raw data is in `notebooks/initial-ingest/inputs/`

### Step 2

I cleaned and organized the data before restructuring it to ingest into Neo4J, a popular graph database solution.

<img width="932" alt="Screenshot 2024-03-03 at 12 37 55 PM" src="https://github.com/jchanolm/arbitrum-data/assets/160365885/c90054de-498b-4094-aaa3-7cdd4333d8c2">


### Step 3

I exported clean+organized data covering grantees, grants, and grant funders as `.csvs` `notebooks/initial-ingest/outputs`, where it can be downloaded for personal use or ingested into other datasets.

### Next steps
1. Build pipelines to programatically expand and enrich the graph
- Grabbing forum post + proposal text will provide grantee affiliates (i.e. members of teams that received grants, contributors to grantee repos) as well as faciliating RAG retrieval (by connecting those documents to entities and the graph and setting embeddings on those documents.
2. Strategically ingest data to improve usefulness specifically for Sybil/Airdrop use case
  - Contributors to liquidity pools funded by grant
  - Wallets that have voted on multiple Tally/Snapshot grants proposals
  - Wallets that have donated to multiple Arbitrum-related Gitcoin/Allo grants
  - Signers on multisigs that received grant funds
  - Contributors to Github repos that have received grant funds

 3. Grant identification and extraction from Gitcoin/Allo
    - I estimate that there are ~100 additional recipients of matching funds/other indirect support from Arb Foundation on Gitcoin
   
4. Identify grants received from other ecosystems (i.e. Optomism)

