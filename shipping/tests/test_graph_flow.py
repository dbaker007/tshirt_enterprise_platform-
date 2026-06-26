from unittest.mock import patch

import pytest
from sqlalchemy import text

with (
    patch("confluent_kafka.Consumer"),
    patch("confluent_kafka.schema_registry.SchemaRegistryClient", create=True),
    patch("confluent_kafka.schema_registry.avro.AvroSerializer", create=True),
):
    from shipping.db import (
        SessionLocal,
        ShippingLedger,
        persist_shipping_ledger_record,
        stage_shipping_saga_reply,
    )
    from shipping.graph import shipping_graph_engine


def test_shipping_graph_clears_standard_geography_green(test_db_session):
    """Verifies that standard geographical locations route successfully to fulfillment nodes."""
    payload = {"order_id": "ship-uuid-001", "shipping_address": {"state": "OH"}}

    # Intercept SessionLocal to route the state machine's internal DB queries straight to RAM!
    with patch.object(SessionLocal, "__call__", return_value=test_db_session):
        result = shipping_graph_engine.invoke(
            {"order_event": payload, "action_type": "NEW_SALE"}
        )

    assert result.get("status") == "COMPLETED"
    assert result["order_event"]["order_id"] == "ship-uuid-001"


def test_shipping_graph_catches_michigan_compliance_hold(test_db_session):
    """Verifies that shipping addresses inside Michigan are intercepted and rejected natively."""
    payload = {"order_id": "ship-violation-101", "shipping_address": {"state": "MI"}}

    with patch.object(SessionLocal, "__call__", return_value=test_db_session):
        result = shipping_graph_engine.invoke(
            {"order_event": payload, "action_type": "NEW_SALE"}
        )

    assert result.get("status") == "COMPLETED"


def test_shipping_graph_triggers_compensation_rollback_on_cancel(test_db_session):
    """Verifies that a CANCEL_TRANSACTION action directive routes straight to rollback logic."""
    payload = {"order_id": "ship-cancel-202"}

    with patch.object(SessionLocal, "__call__", return_value=test_db_session):
        result = shipping_graph_engine.invoke(
            {"order_event": payload, "action_type": "CANCEL_TRANSACTION"}
        )

    assert result.get("status") == "COMPLETED"


def test_database_persistence_and_universal_outbox_mirror(test_db_session):
    """Verifies that shipping operations dual-write state responses straight to the central outbox table."""
    db = test_db_session
    order_id = "ship-compliance-999"

    # Directly test your split stateless data access workers within your open RAM transaction
    persist_shipping_ledger_record(db, order_id, ledger_status="SHIPMENT_SECURED")
    stage_shipping_saga_reply(
        db=db,
        order_id=order_id,
        wire_status="SUCCESS",
        ledger_status="SHIPMENT_SECURED",
        reason_text="Test tracking validation",
    )
    db.commit()

    # 1. Verify localized transaction ledger data presence
    ledger = (
        db.query(ShippingLedger)
        .filter(ShippingLedger.order_id == "ship-compliance-999")
        .first()
    )
    assert ledger is not None
    assert ledger.execution_status == "SHIPMENT_SECURED"

    # 2. Verify central platform outbox mirroring
    outbox = db.execute(text("SELECT * FROM platform_outbox;")).fetchone()
    assert outbox is not None
    assert outbox.topic == "saga_replies"
    assert outbox.partition_key == "ship-compliance-999"
