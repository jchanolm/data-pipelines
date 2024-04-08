

# Manual research
I did manual research on Arbitrum-related grants, including cross-system grants, to become familiar with the Grants data landscape.

## Quick stats

- 250 Grantees
- 9 Grant Initiatives
- 135 Grantee Twitter accounts
- 121 Grantee websites
- 108 Grantee funding wallets
- 75 Grantee Github accounts


I'll do a more comprehensive write-up comprehensive later this weekend.

## Graph ingest

`ingestor.py` and `cypher.py` contain code for ingesting the connected grants data (Grants, Grantees, Funders, Grant Initiatives, Wallets, etc) into Neo4j, a popular Graph database.

`cypher.py` is the place to go if you want to become familiar with the schema.


## Other notes
- Every data point is sourced -- i.e. Discourse posts, Snapshot proposals, Buidlbox submissions, etc.
