import json
from unittest.mock import patch

from sqlalchemy import text

with (
    patch("confluent_kafka.Consumer"),
    patch("confluent_kafka.schema_registry.SchemaRegistryClient", create=True),
    patch("confluent_kafka.schema_registry.avro.AvroSerializer", create=True),
):
    from shipping.db import (
        ShippingLedger,
        persist_shipping_ledger_record,
        stage_shipping_saga_reply,
    )


def test_database_persistence_and_orchestrator_payload_contract(test_db_session):
    """Verifies that shipping operations write the semantic reason to the ledger

    while passing pure control signals onto the orchestration wire.
    """
    db = test_db_session
    order_id = "ship-compliance-999"
    reason_txt = "Freight routes successfully locked on carrier schedule."

    # Execute forward success step using split stateless workers
    persist_shipping_ledger_record(db, order_id, ledger_status="SHIPMENT_SECURED")
    stage_shipping_saga_reply(
        db=db,
        order_id=order_id,
        wire_status="SUCCESS",
        ledger_status="SHIPMENT_SECURED",
        reason_text=reason_txt,
    )
    db.commit()

    # 1. Verify localized transaction ledger records the rich semantic status
    ledger = (
        db.query(ShippingLedger)
        .filter(ShippingLedger.order_id == "ship-compliance-999")
        .first()
    )
    assert ledger is not None
    assert ledger.execution_status == "SHIPMENT_SECURED"

    # 2. Verify central platform outbox captures the pure status control signal
    outbox = db.execute(text("SELECT * FROM platform_outbox;")).fetchone()
    assert outbox is not None
    assert outbox.topic == "saga_replies"

    parsed_payload = json.loads(outbox.payload)
    assert parsed_payload["status"] == "SUCCESS"
    assert parsed_payload["reason"] == reason_txt


def test_compensation_rollback_writes_failure_control_signal_correctly(test_db_session):
    """Verifies that a failure branch records the semantic cause to the ledger

    and transmits the rigid 'FAILED' control signal to trigger sagas rollbacks.
    """
    db = test_db_session
    order_id = "ship-failure-111"
    reason_txt = (
        "Legal distribution constraint prohibits shirt logistics inside Michigan."
    )

    # Execute failure step using split stateless workers
    persist_shipping_ledger_record(db, order_id, ledger_status="LEGAL_REJECTION_MI")
    stage_shipping_saga_reply(
        db=db,
        order_id=order_id,
        wire_status="FAILED",
        ledger_status="LEGAL_REJECTION_MI",
        reason_text=reason_txt,
    )
    db.commit()

    # 1. Localized ledger retains full human-readable error context
    ledger = (
        db.query(ShippingLedger)
        .filter(ShippingLedger.order_id == "ship-failure-111")
        .first()
    )
    assert ledger is not None
    assert ledger.execution_status == "LEGAL_REJECTION_MI"

    # 2. Outbox wire envelope strips domain specificity down to the rigid control signal
    outbox = db.execute(text("SELECT * FROM platform_outbox;")).fetchone()
    assert outbox is not None

    parsed_payload = json.loads(outbox.payload)
    assert parsed_payload["status"] == "FAILED"
    assert parsed_payload["reason"] == reason_txt
