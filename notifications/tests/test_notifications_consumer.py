import json
import os
import uuid

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Import your actual production nodes and verification models natively
from notifications.app import notifications_subgraph_node
from notifications.db import CommunicationLedger, NotificationOutbox

from .test_db import get_clean_test_db_session

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"


@pytest.fixture(scope="module")
def avro_serializer():
    """Loads the shared CommandEnvelope data contract straight from the root schemas/ hub."""
    client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "schemas", "command_envelope.avsc"
    )
    with open(schema_path, "r") as f:
        schema_str = f.read()
    return AvroSerializer(client, schema_str, lambda obj, ctx: obj)


@pytest.fixture(scope="function")
def clean_db():
    """Fixture calling our encapsulated test data-access wrapper out-of-band."""
    db = get_clean_test_db_session()
    try:
        yield db
    finally:
        db.close()


def test_notifications_node_successfully_processes_new_sale_command(
    avro_serializer, clean_db
):
    """SCENARIO: Conductor commands a NEW_SALE.

    Service must stage a draft receipt on disk and drop a success reply to the outbox.
    """
    generated_order_id = str(uuid.uuid4())

    # 1. Build an explicit context payload matching what the Sales Conductor will send
    mock_context_payload = {
        "customer_name": "Saga Commander",
        "item_id": "SHIRT_GOLD_LIMITED",
    }

    mock_command_envelope = {
        "command_id": str(uuid.uuid4()),
        "order_id": generated_order_id,
        "action": "NEW_SALE",
        "payload": json.dumps(mock_context_payload),
    }

    # 2. Directly invoke your production node out-of-band to execute the loop
    notifications_subgraph_node(mock_command_envelope)

    # 3. VERIFICATION ASSERTIONS: Query the database through the test session handle
    saved_ledger = (
        clean_db.query(CommunicationLedger)
        .filter(CommunicationLedger.order_id == generated_order_id)
        .first()
    )
    assert saved_ledger is not None
    assert saved_ledger.alert_type == "ORDER_RECEIPT_DRAFT"
    assert saved_ledger.status == "PENDING_FINANCIAL_CLEARANCE"

    saved_outbox = (
        clean_db.query(NotificationOutbox)
        .filter(NotificationOutbox.key == generated_order_id)
        .first()
    )
    assert saved_outbox is not None
    assert saved_outbox.topic == "saga_replies"

    # Parse the outbox JSON to prove it matches the global SagaReply data contract fields
    reply_data = json.loads(saved_outbox.payload)
    assert reply_data["department"] == "NOTIFICATIONS"
    assert reply_data["status"] == "SUCCESS"


def test_notifications_node_successfully_processes_cancellation_command(
    avro_serializer, clean_db
):
    """SCENARIO: Conductor commands a CANCEL_TRANSACTION rollback.

    Service must abort the receipt, log a fraud alert, and stage a reply outbox row.
    """
    generated_order_id = str(uuid.uuid4())

    mock_context_payload = {"customer_name": "Saga Commander"}

    mock_command_envelope = {
        "command_id": str(uuid.uuid4()),
        "order_id": generated_order_id,
        "action": "CANCEL_TRANSACTION",
        "payload": json.dumps(mock_context_payload),
    }

    # 1. Directly invoke your production node out-of-band to execute the rollback loop
    notifications_subgraph_node(mock_command_envelope)

    # 2. VERIFICATION ASSERTIONS: Prove the compensating step modified the disk state
    saved_ledger = (
        clean_db.query(CommunicationLedger)
        .filter(CommunicationLedger.order_id == generated_order_id)
        .first()
    )
    assert saved_ledger is not None
    assert saved_ledger.alert_type == "SECURITY_FRAUD_WARNING"
    assert saved_ledger.status == "DISPATCHED_FRAUD_ALERT"

    saved_outbox = (
        clean_db.query(NotificationOutbox)
        .filter(NotificationOutbox.key == generated_order_id)
        .first()
    )
    assert saved_outbox is not None

    reply_data = json.loads(saved_outbox.payload)
    assert reply_data["department"] == "NOTIFICATIONS"
    assert reply_data["status"] == "SUCCESS"
