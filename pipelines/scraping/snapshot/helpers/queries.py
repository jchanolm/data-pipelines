proposals_query = """
        {{
            proposals(
                first: {0},
                skip: {1},
                where: {{
                    space: "arbitrumfoundation.eth"
                }},
                orderBy: "created",
                orderDirection: desc
            ) {{
                id
                ipfs
                author
                created
                space {{
                id
                name
                }}
                network
                type
                strategies {{
                name
                params
                }}      
                plugins
                title
                body
                choices
                start
                end
                snapshot
                state
                link
            }}
        }}
        """
proposal_status_query = """
        {{
            proposals(
                where: {{id: "{0}"}}
            ) {{
                id
                state
            }}
        }}
        """

votes_query = """
        {{
            votes (
                first: {0}
                skip: {1}
                orderBy: "created",
                orderDirection: desc,
                where: {{
                proposal_in: {2}
                }}
            ) {{
                id
                ipfs
                voter
                created
                choice
                proposal {{
                id
                choices
                }}
                space {{
                id
                name
                }}
            }}
        }}
        """