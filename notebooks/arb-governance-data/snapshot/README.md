## Overview

This notebook uses Snapshot's API all Arbitrum DAO Snapshot proposals, proposal authors,
and voters.
- 145 proprosals
- 20 proprosal authors
- 150,000 voters 

The Snapshot data is connected to the our  grantee data through Snapshot Proprosals.

![Screenshot 2024-03-03 at 10 30 51 PM](https://github.com/jchanolm/arbitrum-data/assets/160365885/db00e4bd-8cff-40e2-98e2-41ba9838f6c4)

*(Here you can see how the wallet Saavy, a STIP grant recipient, received funds from was used to vote on several other STIP grants)*

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

