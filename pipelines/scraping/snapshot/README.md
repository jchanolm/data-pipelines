# Overview
`SnapshotScraper` retrieves governance proposals and their corresponding votes from the Arbitrum DAO Snapshot space.
It runs GraphQL queries against the Snapshot API, parses and refines the returned proposals/votes, and stores the results in `/data`

# Data
### !IMPORTANT!
The script returns **Proposals** and **Votes** as keys in `data.json`.
However, the file returned is over 100mb, which exceeds Github's size limits. 
I hosted the file at this url: https://arb-data-grants-public.s3.amazonaws.com/data_2024-03-05.json

## Proposals: 

```
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
```


## Votes
```
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
```

## Todos
- Clean proposal text
- Add embeddings (in post-processing)
