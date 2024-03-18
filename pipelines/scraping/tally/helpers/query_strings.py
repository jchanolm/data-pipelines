
delegators_query = """
query Delegates($input: DelegatesInput!) {
    delegates(input: $input) {
        nodes {
        ... on Delegate {
            id
            account {
            id
            address
            }
            chainId
            delegatorsCount
            votesCount
        }
    }
        pageInfo {
        firstCursor
        lastCursor
        count
        }
    }
}
"""