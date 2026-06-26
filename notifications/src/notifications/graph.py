import json
import logging
from typing import Any, Dict, Literal

from langgraph.graph import END, START, StateGraph
from opentelemetry import trace
from typing_extensions import TypedDict

# 🟢 STANDARDIZED IMPORTS: Pull your stateless database workers and connection factory
from notifications.db import (
    SessionLocal,
    persist_communication_ledger_record,
    stage_notifications_saga_reply,
)

logger = logging.getLogger("NOTIFICATIONS_SERVICE.GRAPH")
tracer = trace.get_tracer("notifications-alert-service")


class NotificationsState(TypedDict):
    """The data payload state bucket passed sequentially between nodes."""

    order_event: Dict[str, Any]
    action: str
    status: str


# =========================================================================
# THE RESTRUCTURED GRAPH MATRIX ENTRYWAY ROUTER
# =========================================================================
def route_initial_ingress_directive(
    state: NotificationsState,
) -> Literal["process_notification_routing", "process_compensation_rollback"]:
    """GATEWAY ROUTER: Inspects the raw control action before any business nodes execute."""
    # Permissively fall back across framework routing variants
    action = state.get("action") or state.get("action_type") or "NEW_SALE"

    if action == "CANCEL_TRANSACTION":
        return "process_compensation_rollback"
    return "process_notification_routing"


# =========================================================================
# GRAPH NODE ACTION FUNCTIONS (The Core Business Logic Hub)
# =========================================================================
def process_notification_routing(state: NotificationsState) -> Dict[str, Any]:
    """BUSINESS LOGIC NODE: Orchestrates customer messaging alert broadcasts for forward checkout."""
    with tracer.start_as_current_span("execute_notification_broadcast") as span:
        order_event = state["order_event"]
        action = state.get("action", "NEW_SALE")

        order_id = order_event.get("order_id", "unknown-uuid")
        customer_name = order_event.get("customer_name", "Anonymous Buyer")

        logger.info(
            f"Dispatching Alert: [WELCOME_AND_INVOICE_EMAIL] for Order UUID: {order_id}"
        )

        # TRANSACTION UNIT OF WORK: Explicit lifecycle block inside the execution node!
        db = SessionLocal()
        try:
            persist_communication_ledger_record(
                db=db,
                order_id=str(order_id),
                customer_name=str(customer_name),
                ledger_status="NOTIFICATION_SENT",
            )
            stage_notifications_saga_reply(
                db=db,
                order_id=str(order_id),
                wire_status="SUCCESS",
                ledger_status="NOTIFICATION_SENT",
            )
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(
                f"❌ Failed to commit notifications alert transaction: {str(e)}"
            )
            raise e
        finally:
            db.close()

        return {"status": "COMPLETED", "order_event": order_event, "action": action}


def process_compensation_rollback(state: NotificationsState) -> Dict[str, Any]:
    """BUSINESS LOGIC NODE: Safely unpacks, logs, and transmits compensation recall transactions."""
    with tracer.start_as_current_span("execute_notification_compensation") as span:
        order_event = state["order_event"]
        action = state.get("action", "CANCEL_TRANSACTION")

        # Unpack the nested JSON string payload transmitted by the orchestrator envelope
        if "payload" in order_event:
            try:
                if isinstance(order_event["payload"], str):
                    unpacked_payload = json.loads(order_event["payload"])
                    order_event = {**order_event, **unpacked_payload}
            except Exception as parse_err:
                logger.error(
                    f"⚠️ Failed to parse nested outbox payload string context: {str(parse_err)}"
                )

        order_id = order_event.get("order_id", "unknown-uuid")
        customer_name = order_event.get("customer_name", "Anonymous Buyer")

        logger.info(
            f"Dispatching Alert: [ORDER_CANCELLED_COMPENSATION_EMAIL] for Order UUID: {order_id}"
        )

        db = SessionLocal()
        try:
            persist_communication_ledger_record(
                db=db,
                order_id=str(order_id),
                customer_name=str(customer_name),
                ledger_status="ROLLED_BACK",
            )
            stage_notifications_saga_reply(
                db=db,
                order_id=str(order_id),
                wire_status="ROLLED_BACK",
                ledger_status="ROLLED_BACK",
            )
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(
                f"❌ Failed to commit notifications compensation transaction: {str(e)}"
            )
            raise e
        finally:
            db.close()

        return {"status": "COMPLETED", "order_event": order_event, "action": action}


# =========================================================================
# ASSEMBLING THE WORKFLOW MATRIX (Symmetrical LangGraph restructure)
# =========================================================================
builder = StateGraph(NotificationsState)

builder.add_node("process_notification_routing", process_notification_routing)
builder.add_node("process_compensation_rollback", process_compensation_rollback)

# Bind the entryway conditional routing selector straight onto the START node
builder.add_conditional_edges(START, route_initial_ingress_directive)

builder.add_edge("process_notification_routing", END)
builder.add_edge("process_compensation_rollback", END)

notifications_graph_engine = builder.compile()
