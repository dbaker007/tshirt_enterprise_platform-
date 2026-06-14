import json
import os
import threading
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

# Import your active production loop assets directly for the background process thread
from finance.app import (
    extract_command_payload,
    finance_graph_engine,
    initialize_consumer_dependencies,
)
from finance.db import FinanceLedger, FinanceOutbox

from .test_db import get_clean_test_db_session

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"


@pytest.fixture(scope="module")
def schema_registry():
    """Initializes the central schema registry contract link."""
    return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


@pytest.fixture(scope="module")
def command_serializer(schema_registry):
    """Loads the shared CommandEnvelope schema contract straight from the root schemas/ hub."""
    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "schemas", "command_envelope.avsc"
    )
    with open(schema_path, "r") as f:
        schema_str = f.read()
    return AvroSerializer(schema_registry, schema_str, lambda obj, ctx: obj)


@pytest.fixture(scope="function")
def clean_db():
    """Fixture calling our encapsulated test data-access wrapper out-of-band."""
    db = get_clean_test_db_session()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture(scope="function")
def background_consumer_thread():
    """Spawns an independent background thread running your live app.py polling engine

    to catch real Kafka messages sent during test execution blocks.
    """
    stop_event = threading.Event()

    def run_consumer():
        # Setup production connections once
        avro_deserializer = initialize_consumer_dependencies()

        config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"test_finance_group_{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        consumer = Consumer(config)
        consumer.subscribe(["finance_commands"])

        while not stop_event.is_set():
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                break

            try:
                order_payload = extract_command_payload(msg, avro_deserializer)
                finance_graph_engine.invoke({"order_event": order_payload})
                consumer.commit(msg, asynchronous=False)
            except Exception:
                consumer.commit(msg, asynchronous=False)

        consumer.close()

    thread = threading.Thread(target=run_consumer, daemon=True)
    thread.start()
    yield
    stop_event.set()
    thread.join(timeout=2.0)


# =========================================================================
# INTEGRATION TEST CASES
# =========================================================================


def test_finance_consumer_automatically_approves_orders_under_threshold(
    command_serializer, clean_db, background_consumer_thread
):
    """SCENARIO: An order under $200.00 is packaged into a CommandEnvelope,

    dropped onto Kafka, and processed into a CREDIT_APPROVED status.
    """
    generated_order_id = str(uuid.uuid4())
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    # 1. Structure a real delivery address block to fulfill our strict data contract
    address_record = {
        "street": "123 Default Way",
        "city": "Defaultville",
        "state": "OH",
        "postal_code": "43215",
    }

    # 2. Package the internal order data dictionary tree matching OrderPayloadRecord fields
    order_payload = {
        "customer_name": "Finance Master",
        "customer_email": "finance@enterprise.io",
        "amount": 89.0,  # ◄── UNDER THE $200.00 THRESHOLD
        "item_id": "SHIRT_SUITE_GOLD",
        "shipping_address": address_record,
    }

    # 3. FIXED: Pass the order_payload dictionary tree directly without using json.dumps()!
    command_envelope = {
        "command_id": str(uuid.uuid4()),
        "order_id": generated_order_id,
        "action": "NEW_SALE",
        "payload": order_payload,
    }

    # 4. Serialize and push out-of-band onto the cluster topic
    context = SerializationContext("finance_commands", MessageField.VALUE)
    producer.produce(
        topic="finance_commands",
        key=generated_order_id,
        value=command_serializer(command_envelope, context),
    )
    producer.flush()

    # 5. Defensive database verification polling loop (gives thread time to process)
    saved_ledger = None
    for _ in range(10):
        time.sleep(0.5)
        saved_ledger = (
            clean_db.query(FinanceLedger)
            .filter(FinanceLedger.order_id == generated_order_id)
            .first()
        )
        if saved_ledger:
            break

    # 6. CORE STATE ASSERTIONS
    assert saved_ledger is not None, (
        "❌ Test Failure: Background thread never saved FinanceLedger row!"
    )
    assert saved_ledger.status == "CREDIT_APPROVED"

    saved_outbox = (
        clean_db.query(FinanceOutbox)
        .filter(FinanceOutbox.key == generated_order_id)
        .first()
    )
    assert saved_outbox is not None
    assert saved_outbox.topic == "saga_replies"


def test_finance_consumer_automatically_rejects_orders_over_threshold(
    command_serializer, clean_db, background_consumer_thread
):
    """SCENARIO: An order over $200.00 is packaged into a CommandEnvelope,

    dropped onto Kafka, triggers the fraud node, and records a PAYMENT_REJECTED status.
    """
    generated_order_id = str(uuid.uuid4())
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    address_record = {
        "street": "123 Default Way",
        "city": "Defaultville",
        "state": "OH",
        "postal_code": "43215",
    }

    order_payload = {
        "customer_name": "Risk Challenger",
        "customer_email": "spammer@enterprise.io",
        "amount": 250.0,  # ◄── EXCEEDS THE $200.00 LIMIT
        "item_id": "SHIRT_SUITE_GOLD",
        "shipping_address": address_record,
    }

    command_envelope = {
        "command_id": str(uuid.uuid4()),
        "order_id": generated_order_id,
        "action": "NEW_SALE",
        "payload": order_payload,
    }

    context = SerializationContext("finance_commands", MessageField.VALUE)
    producer.produce(
        topic="finance_commands",
        key=generated_order_id,
        value=command_serializer(command_envelope, context),
    )
    producer.flush()

    saved_ledger = None
    for _ in range(10):
        time.sleep(0.5)
        saved_ledger = (
            clean_db.query(FinanceLedger)
            .filter(FinanceLedger.order_id == generated_order_id)
            .first()
        )
        if saved_ledger:
            break

    # 5. CORE FRAUD STATE ASSERTIONS
    assert saved_ledger is not None, (
        "❌ Test Failure: Background thread never saved FinanceLedger row!"
    )
    assert saved_ledger.status == "PAYMENT_REJECTED"

    saved_outbox = (
        clean_db.query(FinanceOutbox)
        .filter(FinanceOutbox.key == generated_order_id)
        .first()
    )
    assert saved_outbox is not None

    payload_data = json.loads(saved_outbox.payload)
    assert payload_data["status"] == "PAYMENT_REJECTED"
