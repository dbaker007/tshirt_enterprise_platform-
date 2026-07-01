import json
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import text

with (
    patch("confluent_kafka.Consumer"),
    patch("confluent_kafka.schema_registry.SchemaRegistryClient", create=True),
    patch("confluent_kafka.schema_registry.avro.AvroSerializer", create=True),
    patch("confluent_kafka.schema_registry.avro.AvroDeserializer", create=True),
    patch(
        "observability.db.get_platform_database_url", return_value="sqlite:///:memory:"
    ),
):
    from shipping.app import ShippingConsumerApplication
    from shipping.constants import (
        FREIGHT_ROUTE_RELEASED,
        LEGAL_REJECTION_MI,
        ROLLED_BACK,
        SHIPMENT_SECURED,
        SUCCESS,
    )
    from shipping.db import (
        ShippingLedger,
        persist_shipping_ledger_record,
        stage_shipping_saga_reply,
    )
    from shipping.graph import shipping_graph_engine


# =========================================================================
# 📡 APPLICATION BOUNDARY INTEGRATION TESTING
# =========================================================================


@patch("shipping.app.shipping_graph_engine.invoke")
def test_shipping_consumer_app_forwards_payload_and_action_to_graph(
    mock_graph_invoke, test_db_session
):
    """Verifies that the parent application wrapper accurately initializes and forwards

    both the event data and action context into the LangGraph engine on ingestion loops.
    """

    def mock_session_factory():
        return test_db_session

    with patch("shipping.app.init_shipping_db"):
        app = ShippingConsumerApplication()
        app.SessionLocal = mock_session_factory

        sample_payload = {
            "order_id": "ship-test-999",
            "shipping_address": {"state": "TX"},
        }
        sample_action = "NEW_SALE"

        app.execute_business_logic(order_payload=sample_payload, action=sample_action)

    expected_config = {
        "configurable": {"thread_id": "ship-test-999", "db": test_db_session}
    }

    mock_graph_invoke.assert_called_once_with(
        {
            "order_event": sample_payload,
            "action_type": sample_action,
            "status": "STARTED",
        },
        expected_config,
    )


# =========================================================================
# 🧠 GRAPH ROUTING & STATE FLOW TESTING
# =========================================================================


def test_shipping_graph_clears_standard_geography_green(test_db_session):
    """Verifies that standard geographical locations route successfully to fulfillment nodes."""
    payload = {"order_id": "ship-uuid-001", "shipping_address": {"state": "OH"}}
    config = {"configurable": {"thread_id": "ship-uuid-001", "db": test_db_session}}

    result = shipping_graph_engine.invoke(
        {"order_event": payload, "action_type": "NEW_SALE"}, config
    )

    assert result.get("status") == "COMPLETED"
    assert result["order_event"]["order_id"] == "ship-uuid-001"


def test_shipping_graph_catches_michigan_compliance_hold(test_db_session):
    """Verifies that shipping addresses inside Michigan are intercepted and rejected natively."""
    payload = {"order_id": "ship-violation-101", "shipping_address": {"state": "MI"}}
    config = {
        "configurable": {"thread_id": "ship-violation-101", "db": test_db_session}
    }

    result = shipping_graph_engine.invoke(
        {"order_event": payload, "action_type": "NEW_SALE"}, config
    )

    assert result.get("status") == "COMPLETED"


def test_shipping_graph_triggers_compensation_rollback_on_cancel(test_db_session):
    """Verifies that a CANCEL_TRANSACTION action directive routes straight to rollback logic."""
    payload = {"order_id": "ship-cancel-202"}
    config = {"configurable": {"thread_id": "ship-cancel-202", "db": test_db_session}}

    result = shipping_graph_engine.invoke(
        {"order_event": payload, "action_type": "CANCEL_TRANSACTION"}, config
    )

    assert result.get("status") == "COMPLETED"


# =========================================================================
# 🗄️ STATELESS PERSISTENCE & STORAGE INTEGRATION TESTING
# =========================================================================


def test_database_persistence_and_orchestrator_payload_contract(test_db_session):
    """Verifies that shipping operations write the semantic reason to the ledger

    while passing pure control signals onto the orchestration wire.
    """
    db = test_db_session
    order_id = "ship-compliance-999"

    persist_shipping_ledger_record(db, order_id, ledger_status=SHIPMENT_SECURED)
    stage_shipping_saga_reply(
        db=db,
        order_id=order_id,
        wire_status="SUCCESS",
        ledger_status=SUCCESS,
    )
    db.commit()

    ledger = (
        db.query(ShippingLedger)
        .filter(ShippingLedger.order_id == "ship-compliance-999")
        .first()
    )
    assert ledger is not None
    assert ledger.execution_status == SHIPMENT_SECURED

    outbox = db.execute(text("SELECT * FROM platform_outbox;")).fetchone()
    assert outbox is not None
    assert outbox.topic == "saga_replies"

    parsed_payload = json.loads(outbox.payload)
    assert parsed_payload["status"] == "SUCCESS"
    assert parsed_payload["ledger_status"] == SUCCESS


def test_compensation_rollback_writes_failure_control_signal_correctly(test_db_session):
    """Verifies that a failure branch records the semantic cause to the ledger

    and transmits the rigid 'FAILED' control signal to trigger sagas rollbacks.
    """
    db = test_db_session
    order_id = "ship-failure-111"

    persist_shipping_ledger_record(db, order_id, ledger_status=LEGAL_REJECTION_MI)
    stage_shipping_saga_reply(
        db=db,
        order_id=order_id,
        wire_status="FAILED",
        ledger_status=LEGAL_REJECTION_MI,
    )
    db.commit()

    ledger = (
        db.query(ShippingLedger)
        .filter(ShippingLedger.order_id == "ship-failure-111")
        .first()
    )
    assert ledger is not None
    assert ledger.execution_status == LEGAL_REJECTION_MI

    outbox = db.execute(text("SELECT * FROM platform_outbox;")).fetchone()
    assert outbox is not None

    parsed_payload = json.loads(outbox.payload)
    assert parsed_payload["status"] == "FAILED"
    assert parsed_payload["ledger_status"] == LEGAL_REJECTION_MI


def test_database_persistence_and_universal_outbox_mirror(test_db_session):
    """Verifies that shipping operations dual-write state responses straight to the central outbox table."""
    db = test_db_session
    order_id = "ship-compliance-999"

    persist_shipping_ledger_record(db, order_id, ledger_status=SHIPMENT_SECURED)
    stage_shipping_saga_reply(
        db=db,
        order_id=order_id,
        wire_status="SUCCESS",
        ledger_status=SUCCESS,
    )
    db.commit()

    ledger = (
        db.query(ShippingLedger)
        .filter(ShippingLedger.order_id == "ship-compliance-999")
        .first()
    )
    assert ledger is not None
    assert ledger.execution_status == SHIPMENT_SECURED

    outbox = db.execute(text("SELECT * FROM platform_outbox;")).fetchone()
    assert outbox is not None
    assert outbox.topic == "saga_replies"
    assert outbox.partition_key == "ship-compliance-999"
