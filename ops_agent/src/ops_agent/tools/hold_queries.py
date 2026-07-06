# ops_agent/src/ops_agent/tools/hold_queries.py

import logging
import os
from typing import Any, Dict, List

import httpx

logger = logging.getLogger("OPS_AGENT.TOOLS.QUERIES")

# Centralize service mesh destination lookups via cluster-internal host domains natively
FINANCE_SERVICE_URL = os.getenv("FINANCE_API_URL", "http://localhost:8001")
SALES_SERVICE_URL = os.getenv("SALES_API_URL", "http://sales-order-entry-service:8000")


async def list_pending_holds(
    state_code: str = None,
    min_amount: float = None,
    max_amount: float = None,
    customer_name: str = None,
    zip_code: str = None,
    action_verdict: str = None,
) -> Dict[str, Any] | List[Dict[str, Any]]:
    """
    Asynchronous Operational Macro Action Tool. Aggregates and filters cluster records natively [1.1].
    If action_verdict is specified, automatically resolves all matched transaction rows inside the shards [1.1].
    """
    logger.info(
        f"🔍 [TOOL INVOCATION]: list_pending_holds called | "
        f"State: {state_code} | Min: {min_amount} | Max: {max_amount} | "
        f"Customer: {customer_name} | Zip: {zip_code} | Action: {action_verdict}"
    )

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{SALES_SERVICE_URL}/api/ui/pending-reviews")

        if response.status_code != 200:
            logger.error(
                f"Failed to fetch pending reviews from Sales API. Status: {response.status_code}"
            )
            return []

        all_holds = response.json()

    except Exception as err:
        logger.error(
            f"Network drop crossing boundaries to aggregate shard records: {str(err)}"
        )
        return []

    filtered_results = []
    for order in all_holds:
        order_amount = float(order.get("amount", 0.0))
        address_obj = order.get("shipping_address", {})

        # 1. Evaluate Minimum Amount Constraint
        if min_amount is not None and float(min_amount) != 0:
            if order_amount < float(min_amount):
                continue

        # 2. Evaluate Maximum Amount Constraint
        if max_amount is not None and float(max_amount) != 0:
            if order_amount > float(max_amount):
                continue

        # 3. Evaluate Geographical State Code Constraint
        if state_code is not None and str(state_code).strip() != "":
            target_state = str(state_code).upper().strip()
            order_state = str(address_obj.get("state", "")).upper().strip()
            if order_state != target_state:
                continue

        # 4. Evaluate Customer Name Matching (Case-Insensitive Substring)
        if customer_name is not None and str(customer_name).strip() != "":
            target_name = str(customer_name).lower().strip()
            order_name = str(order.get("customer_name", "")).lower().strip()
            if target_name not in order_name:
                continue

        # 5. Evaluate Geographical Zip Code Constraint
        if zip_code is not None and str(zip_code).strip() != "":
            target_zip = str(zip_code).strip()
            order_zip = str(address_obj.get("postal_code", "")).strip()
            if order_zip != target_zip:
                continue

        filtered_results.append(order)

    # 🏁 READ-ONLY BRANCH: Return the filtered rows cleanly if no action verdict was requested
    if not action_verdict or str(action_verdict).strip() == "":
        logger.info(
            f"✔ [READ COMPLETED]: Returning {len(filtered_results)} matching hold records."
        )
        return filtered_results

    # 🛑 MUTATION BRANCH: Process bulk microservice updates across your open connection pool [1.1]
    clean_verdict = str(action_verdict).upper().strip()
    success_count = 0
    failure_count = 0
    mutation_details = []

    logger.info(
        f"⚡ [MACRO ENGINE]: Initiating bulk resolution loop for {len(filtered_results)} records -> [{clean_verdict}]"
    )

    for match in filtered_results:
        order_id = match.get("order_id")
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.post(
                    f"{SALES_SERVICE_URL}/sales/override",
                    json={"order_id": str(order_id), "verdict": clean_verdict},
                )

            if res.status_code == 200:
                success_count += 1
                mutation_details.append({"order_id": order_id, "status": "SUCCESS"})
            else:
                failure_count += 1
                mutation_details.append(
                    {
                        "order_id": order_id,
                        "status": "FAILED_BY_GATEWAY",
                        "code": res.status_code,
                    }
                )
        except Exception as err:
            failure_count += 1
            mutation_details.append(
                {"order_id": order_id, "status": "ERROR", "message": str(err)}
            )

    return {
        "status": "COMPLETED",
        "total_matched_count": len(filtered_results),
        "successful_mutations_count": success_count,
        "failed_mutations_count": failure_count,
        "batch_details": mutation_details,
        "original_records": filtered_results,  # Enforced to preserve test scanning capability [1.1]
    }
