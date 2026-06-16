import logging
from typing import Any, Dict

from opentelemetry import trace

# Import your clean repository function directly from your sibling database file
from notifications.db import execute_notification_task_and_stage_reply

logger = logging.getLogger("NOTIFICATIONS_SERVICE.GRAPH")
tracer = trace.get_tracer("notifications-alert-service")


def notifications_subgraph_node(order_event: dict, action: str):
    """BUSINESS LOGIC NODE: Orchestrates specific customer messaging alert broadcasts

    based on central conductor transaction states.
    """
    # 🟢 CHILD SPAN: Automatically tracks inside the active trace timeline tree context
    with tracer.start_as_current_span("execute_notification_broadcast") as span:
        order_id = order_event.get("order_id")
        customer_name = order_event.get("customer_name", "Anonymous Buyer")

        span.set_attribute("order.correlation_id", order_id or "unknown")
        span.set_attribute("saga.action_directive", action)

        logger.info(f"Command Ingested | Action: {action} | Order UUID: {order_id}")

        if action == "NEW_SALE":
            alert_type = "WELCOME_AND_INVOICE_EMAIL"
        elif action == "CANCEL_TRANSACTION":
            alert_type = "COMPLIANCE_HOLD_ALERT_SMS"
        else:
            alert_type = "UNKNOWN_ERROR_ALERT"
            span.set_status(
                trace.Status(
                    trace.StatusCode.ERROR, description="Unknown Action Received"
                )
            )

        # Invoke database outbox dual-write function positionally
        execute_notification_task_and_stage_reply(
            str(order_id), str(customer_name), str(action)
        )

        logger.info(
            f"Alert Lifecycle Dispatched | Order UUID: {order_id} | Action Triggered: {action}"
        )
