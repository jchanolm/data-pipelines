## Overview

This notebook uses Snapshot's API all Arbitrum DAO Snapshot proposals, proposal authors,
and voters.
- 145 proprosals
- 20 proprosal authors
- 150,000 voters 


Connecting Snapshot data to the graph enhances its value for generating Sybil/Airdrop relevant datasets.

For instance, the Arbitrum Foundation could factor in whether a wallet voted on multiple
grant-related Snapshot Proprosals into future airdrops to reward those wallets for their commmitment
to responsibly funding public goods:

```
match (wallet:Wallet)-[r:VOTED]->(prop:Proposal)-[:APPROVED_FUNDING]-(e:Entity)
with wallet, count(distinct(prop)) as votes
where votes > 3
match (wallet)-[:VOTED]-(p:Proposal)-[:APPROVED_FUNDING]-(e:Entity)
return distinct wallet.address as address, count(distinct(p)) as votes, collect(distinct(e.name)) as funded_entities order by votes desc
```

This query gives us 3,025 wallets who voted on three or more STIP grants, in descending order.
There are ~200 wallets which voted on every single STIP proprosal -- are they highly engaged or bots?? 

(File in `airdrop-datasets/`)

