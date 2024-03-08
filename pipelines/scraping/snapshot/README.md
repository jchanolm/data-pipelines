
# Overview
`SnapshotScraper` retrieves governance proposals and their corresponding votes from the Arbitrum DAO Snapshot space. It runs GraphQL queries against the Snapshot API, parses and refines the returned proposals/votes, and stores the results in /data

# Data
The script, by default, saves to S3 to streamline pipelines.
Once the repo is more mature I'll add helper functions to save locally or to IPFS.

Datafile: ipfs://bafybeihu23kfmowefwj2ztolc42ajyx7gqd7i5wfvwnypvqhmel5hwrorm/

**Proposals:**
{
  "proposals": [
    {
      "id": "string",
      "title": "string",
      "body": "string",
      "choices": ["string"],
      "start": "timestamp",
      "end": "timestamp",
      "state": "string",
      "author": "string"
    }
  ]
}

**Votes**
{
  "votes": [
    {
      "proposalId": "string",
      "voter": "string",
      "choice": "number",
      "id": "dict"
    }
  ]
}

