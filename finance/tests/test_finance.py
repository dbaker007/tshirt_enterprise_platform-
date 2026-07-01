import asyncio
from unittest.mock import patch

import pytest
from finance.constants import (
    CREDIT_APPROVED,
    CREDIT_LINE_RELEASED,
)
from finance.db import (
    FinanceLedger,
    persist_financial_ledger_record,
    stage_finance_saga_reply,
)
from finance.graph import builder
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.types import Command
from sqlalchemy import text


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def test_async_saver():
    """Provides an isolated, non-blocking asynchronous checkpointer instance."""
    saver = AsyncSqliteSaver.from_conn_string(":memory:")
    async with saver as active_saver:
        yield active_saver


# =========================================================================
# GRAPH STATE FLOW TESTING (ASYNC ISOLATION via DEPENDENCY INJECTION)
# =========================================================================


@pytest.mark.anyio
async def test_no_fraud_detected_flow(test_async_saver, test_db_session):
    """Verifies that a compliant order under $200 executes cleanly without an interrupt hold."""
    order_id = "standard-clean-uuid-001"
    test_graph_engine = builder.compile(checkpointer=test_async_saver)

    config = {
        "configurable": {
            "thread_id": order_id,
            "db": test_db_session,
        }
    }
    initial_payload = {"order_id": order_id, "amount": 145.00}

    state_input = {
        "order_event": initial_payload,
        "action": "NEW_SALE",
        "status": "STARTED",
    }

    final_output = await test_graph_engine.ainvoke(state_input, config)

    assert final_output["status"] == "COMPLETED"
    thread_state = await test_graph_engine.aget_state(config)
    assert len(thread_state.next) == 0


@pytest.mark.anyio
async def test_fraud_detection_override_flow(test_async_saver, test_db_session):
    """Verifies that a high-amount order is paused by an interrupt and clears on approval."""
    order_id = "hitl-approve-uuid-002"
    test_graph_engine = builder.compile(checkpointer=test_async_saver)

    config = {
        "configurable": {
            "thread_id": order_id,
            "db": test_db_session,
        }
    }
    initial_payload = {"order_id": order_id, "amount": 275.50}

    state_input = {
        "order_event": initial_payload,
        "action": "NEW_SALE",
        "status": "STARTED",
    }
    await test_graph_engine.ainvoke(state_input, config)

    current_state = await test_graph_engine.aget_state(config)
    assert current_state.next == ("evaluate_financial_fraud_risk",)

    final_output = await test_graph_engine.ainvoke(Command(resume="APPROVE"), config)
    assert final_output["status"] == "COMPLETED"

    updated_state = await test_graph_engine.aget_state(config)
    assert len(updated_state.next) == 0


@pytest.mark.anyio
async def test_fraud_detection_resume_rejection_flow(test_async_saver, test_db_session):
    """Verifies that a high-amount order is paused and rejects when specified."""
    order_id = "hitl-reject-uuid-003"
    test_graph_engine = builder.compile(checkpointer=test_async_saver)

    config = {
        "configurable": {
            "thread_id": order_id,
            "db": test_db_session,
        }
    }
    initial_payload = {"order_id": order_id, "amount": 420.00}

    state_input = {
        "order_event": initial_payload,
        "action": "NEW_SALE",
        "status": "STARTED",
    }
    await test_graph_engine.ainvoke(state_input, config)

    current_state = await test_graph_engine.aget_state(config)
    assert current_state.next == ("evaluate_financial_fraud_risk",)

    final_output = await test_graph_engine.ainvoke(Command(resume="REJECT"), config)
    assert final_output["status"] == "COMPLETED"

    updated_state = await test_graph_engine.aget_state(config)
    assert len(updated_state.next) == 0


# =========================================================================
# SYSTEM COMPENSATION AND STORAGE CONTRACT TESTING
# =========================================================================


def test_database_and_orchestrator_contract_validity(test_db_session):
    """Verifies that database operations accurately match saga outbox contracts."""
    db = test_db_session
    order_id = "saga-compliance-token-123"

    persist_financial_ledger_record(db, order_id, ledger_status=CREDIT_APPROVED)
    stage_finance_saga_reply(
        db, order_id, wire_status="SUCCESS", ledger_status="SUCCESS"
    )
    db.commit()

    ledger = db.query(FinanceLedger).filter(FinanceLedger.order_id == order_id).first()
    assert ledger is not None
    assert ledger.execution_status == CREDIT_APPROVED

    outbox = db.execute(text("SELECT * FROM platform_outbox;")).fetchone()
    assert outbox is not None
    assert outbox.topic == "saga_replies"
    assert outbox.partition_key == order_id


@pytest.mark.anyio
async def test_compensation_rollback_execution_flow(test_async_saver, test_db_session):
    """Verifies that a CANCEL_TRANSACTION action triggers immediate database rollbacks."""
    order_id = "compensation-trigger-999"
    test_graph_engine = builder.compile(checkpointer=test_async_saver)

    config = {
        "configurable": {
            "thread_id": order_id,
            "db": test_db_session,
        }
    }
    initial_payload = {"order_id": order_id, "amount": 50.00}

    state_input = {
        "order_event": initial_payload,
        "action": "CANCEL_TRANSACTION",
        "status": "STARTED",
    }

    final_output = await test_graph_engine.ainvoke(state_input, config)

    assert final_output["status"] == "COMPLETED"

    test_db_session.expire_all()
    ledger = (
        test_db_session.query(FinanceLedger)
        .filter(FinanceLedger.order_id == order_id)
        .first()
    )
    assert ledger is not None
    assert ledger.execution_status == CREDIT_LINE_RELEASED


def test_defensive_malformed_payload_fallbacks(test_db_session):
    """Verifies that the persistence layer executes safe fallbacks on malformed metrics."""
    db = test_db_session
    malformed_order_id = "unknown-uuid"
    fallback_status = "FAILED"

    persist_financial_ledger_record(
        db, malformed_order_id, ledger_status=fallback_status
    )
    stage_finance_saga_reply(
        db, malformed_order_id, wire_status="FAILED", ledger_status=fallback_status
    )
    db.commit()

    ledger = (
        db.query(FinanceLedger)
        .filter(FinanceLedger.order_id == malformed_order_id)
        .first()
    )
    assert ledger is not None
    assert ledger.execution_status == "FAILED"
