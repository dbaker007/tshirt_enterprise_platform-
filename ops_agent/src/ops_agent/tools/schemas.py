# ops_agent/tools/schemas.py

from ops_agent.tools.hold_queries import list_pending_holds

# OpenAI-compliant JSON definition schema layout whitelisting capabilities to Llama
LIST_PENDING_HOLDS_SCHEMA = {
    "type": "function",
    "function": {
        "name": "list_pending_holds",
        "description": (
            "Queries the distributed cluster shards to aggregate and list all orders currently "
            "frozen inside a manual fraud review hold based on search filters. Can optionally "
            "execute a bulk operational macro action to resolve all matched rows simultaneously."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "state_code": {
                    "type": "string",
                    "description": "The two-letter US state code string to filter results (e.g., 'KY', 'OH').",
                },
                "min_amount": {
                    "type": "number",
                    "description": "The minimum order dollar value amount threshold to match.",
                },
                "max_amount": {
                    "type": "number",
                    "description": "The maximum order dollar value amount threshold to match.",
                },
                "customer_name": {
                    "type": "string",
                    "description": "Full or partial name string of the customer to look up.",
                },
                "zip_code": {
                    "type": "string",
                    "description": "The 5-digit zip code string to filter shipping destinations.",
                },
                "action_verdict": {
                    "type": "string",
                    "enum": ["APPROVE", "REJECT"],
                    "description": (
                        "Optional administrative action choice to execute bulk changes on all matched rows. "
                        "Leave empty or omit if the user only wants to view, list, or check the records. "
                        "Set to 'APPROVE' to release or override the holds, or 'REJECT' to let them die."
                    ),
                },
            },
            "required": [],
        },
    },
}

# Master runtime function dispatcher lookup registry table
AVAILABLE_TOOLS = {
    "list_pending_holds": list_pending_holds,
}
