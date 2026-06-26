import logging
from typing import Any, Dict, Literal

from langgraph.graph import END, START, StateGraph
from opentelemetry import trace
from typing_extensions import TypedDict

# 🟢 STANDARDIZED IMPORTS: Pull your stateless database workers and connection factory
from finance.db import (
    SessionLocal,
    persist_financial_ledger_record,
    stage_finance_saga_reply,
)

logger = logging.getLogger("FINANCE_SERVICE.GRAPH")
tracer = trace.get_tracer("finance-auditing-service")


class FinanceState(TypedDict):
    """The data payload state bucket passed sequentially between nodes."""

    order_event: Dict[str, Any]
    action: str
    status: str


# =========================================================================
# THE RESTRUCTURED FINANCE GRAPH MATRIX ENTRYWAY ROUTER
# =========================================================================
def route_initial_ingress_directive(
    state: FinanceState,
) -> Literal["evaluate_financial_fraud_risk", "execute_compensation_rollback"]:
    """GATEWAY ROUTER: Inspects the raw control action before any business nodes execute."""
    # Permissively fall back across framework routing variants to capture the control string
    action = state.get("action") or state.get("status") or "NEW_SALE"

    if action == "CANCEL_TRANSACTION":
        return "execute_compensation_rollback"
    return "evaluate_financial_fraud_risk"


# =========================================================================
# GRAPH NODE ACTION FUNCTIONS (The Core Business Logic Hub)
# =========================================================================
def evaluate_financial_fraud_risk(state: FinanceState) -> Dict[str, Any]:
    """BUSINESS LOGIC NODE: Evaluates order size threshold margins strictly for forward compliance."""
    with tracer.start_as_current_span("langgraph_evaluate_fraud_risk") as span:
        event = state["order_event"]

        # Safe conversion pattern guards against raw upstream payload type variations
        try:
            order_amount = float(event.get("amount", 0.0))
        except (ValueError, TypeError):
            order_amount = 0.0

        span.set_attribute("order.correlation_id", event.get("order_id", "unknown"))
        span.set_attribute("evaluation.amount", order_amount)

        logger.info(
            f"LangGraph Evaluating Risk Node | Transaction Amount: ${order_amount}"
        )

        if order_amount > 200.0:
            logger.warning(
                f"Risk Threshold Exceeded! Flagging Potential Fraud for Order UUID: {event.get('order_id')}"
            )
            span.set_attribute("fraud.flagged", True)
            return {"status": "TRIGGER_FRAUD_REJECTION", "order_event": event}

        return {"status": "PASSED_RISK_CHECKS", "order_event": event}


def execute_approval(state: FinanceState) -> Dict[str, Any]:
    with tracer.start_as_current_span("langgraph_execute_finance_approval"):
        event = state["order_event"]
        order_id = event.get("order_id", "unknown-uuid")
        logger.info(f"Financial Clearance Engine Approved | Order UUID: {order_id}")

        # 🟢 TRANSACTION UNIT OF WORK: Explicit lifecycle block inside the execution node!
        db = SessionLocal()
        try:
            persist_financial_ledger_record(
                db, order_id, ledger_status="CREDIT_APPROVED"
            )
            stage_finance_saga_reply(
                db, order_id, wire_status="SUCCESS", ledger_status="CREDIT_APPROVED"
            )
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(f"❌ Failed to commit finance approval transaction: {str(e)}")
            raise e
        finally:
            db.close()

        return {"status": "COMPLETED", "order_event": event}


def execute_fraud_rejection(state: FinanceState) -> Dict[str, Any]:
    with tracer.start_as_current_span("langgraph_execute_fraud_rejection") as span:
        event = state["order_event"]
        order_id = event.get("order_id", "unknown-uuid")
        span.set_status(
            trace.Status(trace.StatusCode.ERROR, description="Risk Threshold Exceeded")
        )
        logger.warning(f"Financial Clearance Engine Aborted | Order UUID: {order_id}")

        # 🟢 TRANSACTION UNIT OF WORK: Symmetrical fail-safe logic mapping
        db = SessionLocal()
        try:
            persist_financial_ledger_record(
                db, order_id, ledger_status="PAYMENT_REJECTED"
            )
            stage_finance_saga_reply(
                db, order_id, wire_status="FAILED", ledger_status="PAYMENT_REJECTED"
            )
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(
                f"❌ Failed to commit finance fraud rejection transaction: {str(e)}"
            )
            raise e
        finally:
            db.close()

        return {"status": "COMPLETED", "order_event": event}


def execute_compensation_rollback(state: FinanceState) -> Dict[str, Any]:
    """COMPENSATION NODE: Releases held customer credit allocations on failure events."""
    with tracer.start_as_current_span("langgraph_execute_finance_rollback"):
        event = state["order_event"]
        order_id = event.get("order_id", "unknown-uuid")
        logger.info(
            f"Financial Compensation Fired | Releasing credit line for Order: {order_id}"
        )

        # 🟢 TRANSACTION UNIT OF WORK: Symmetrical compensation signature mapping
        db = SessionLocal()
        try:
            persist_financial_ledger_record(
                db, order_id, ledger_status="CREDIT_LINE_RELEASED"
            )
            stage_finance_saga_reply(
                db,
                order_id,
                wire_status="ROLLED_BACK",
                ledger_status="CREDIT_LINE_RELEASED",
            )
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(
                f"❌ Failed to commit finance compensation transaction: {str(e)}"
            )
            raise e
        finally:
            db.close()

        return {"status": "COMPLETED", "order_event": event}


# =========================================================================
# THE CONDITIONAL ROUTING EDGE
# =========================================================================
def route_risk_decision(
    state: FinanceState,
) -> Literal["execute_fraud_rejection", "execute_approval"]:
    if state.get("status") == "TRIGGER_FRAUD_REJECTION":
        return "execute_fraud_rejection"
    return "execute_approval"


# =========================================================================
# ASSEMBLING THE WORKFLOW MATRIX (Symmetrical LangGraph)
# =========================================================================
builder = StateGraph(FinanceState)

builder.add_node("evaluate_financial_fraud_risk", evaluate_financial_fraud_risk)
builder.add_node("execute_approval", execute_approval)
builder.add_node("execute_fraud_rejection", execute_fraud_rejection)
builder.add_node("execute_compensation_rollback", execute_compensation_rollback)

# 🟢 FIX: Bind the entryway conditional routing router straight to the START node!
builder.add_conditional_edges(START, route_initial_ingress_directive)

# Map remaining forward path steps linearly
builder.add_conditional_edges("evaluate_financial_fraud_risk", route_risk_decision)

builder.add_edge("execute_approval", END)
builder.add_edge("execute_fraud_rejection", END)
builder.add_edge("execute_compensation_rollback", END)

finance_graph_engine = builder.compile()
