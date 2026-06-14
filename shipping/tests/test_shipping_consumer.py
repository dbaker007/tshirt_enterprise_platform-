import json
import os
import threading
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

# Import your active production loop assets directly for the background process thread
from shipping.app import (
    extract_command_payload,
    initialize_consumer_dependencies,
    shipping_graph_engine,
)
from shipping.db import ShippingLedger, ShippingOutbox

from .test_db import get_clean_test_db_session

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"


@pytest.fixture(scope="module")
def schema_registry():
    return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


@pytest.fixture(scope="module")
def command_serializer(schema_registry):
    """Loads the strictly-typed CommandEnvelope data contract straight from root schemas/."""
    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "schemas", "command_envelope.avsc"
    )
    with open(schema_path, "r") as f:
        schema_str = f.read()
    return AvroSerializer(schema_registry, schema_str, lambda obj, ctx: obj)


@pytest.fixture(scope="function")
def clean_db():
    db = get_clean_test_db_session()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture(scope="function")
def background_consumer_thread():
    stop_event = threading.Event()

    def run_consumer():
        avro_deserializer = initialize_consumer_dependencies()
        config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"test_shipping_group_{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        consumer = Consumer(config)
        consumer.subscribe(["shipping_commands"])

        while not stop_event.is_set():
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                break

            try:
                order_payload, action = extract_command_payload(msg, avro_deserializer)
                shipping_graph_engine.invoke(
                    {"order_event": order_payload, "action_type": action}
                )
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


def test_shipping_consumer_successfully_processes_new_sale_command(
    command_serializer, clean_db, background_consumer_thread
):
    """SCENARIO: Order targeting non-restricted state passes fulfillment successfully."""
    generated_order_id = str(uuid.uuid4())
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    # Enforce clear data compliance mappings matching your nested record definitions
    address_record = {
        "street": "456 Freedom Way",
        "city": "Columbus",
        "state": "OH",
        "postal_code": "43215",
    }

    order_payload = {
        "customer_name": "Saga Commander",
        "customer_email": "saga.commander@enterprise.io",
        "amount": 124.0,
        "item_id": "SHIRT_LIMITED_GOLD_L",
        "shipping_address": address_record,
    }

    command_envelope = {
        "command_id": str(uuid.uuid4()),
        "order_id": generated_order_id,
        "action": "NEW_SALE",
        "payload": order_payload,
    }

    context = SerializationContext("shipping_commands", MessageField.VALUE)
    producer.produce(
        topic="shipping_commands",
        key=generated_order_id,
        value=command_serializer(command_envelope, context),
    )
    producer.flush()

    saved_ledger = None
    for _ in range(10):
        time.sleep(0.5)
        saved_ledger = (
            clean_db.query(ShippingLedger)
            .filter(ShippingLedger.order_id == generated_order_id)
            .first()
        )
        if saved_ledger:
            break

    assert saved_ledger is not None
    assert saved_ledger.status == "SHIPMENT_SECURED"


def test_shipping_consumer_rejects_michigan_orders_and_triggers_rollback(
    command_serializer, clean_db, background_consumer_thread
):
    """SCENARIO: Order targeting Michigan hits the legal boundary filter.

    It must mark the ledger as LEGAL_REJECTION_MI and drop a FAILED reply status token.
    """
    generated_order_id = str(uuid.uuid4())
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    address_record = {
        "street": "789 Restricted Rd",
        "city": "Detroit",
        "state": "MI",
        "postal_code": "48201",
    }

    order_payload = {
        "customer_name": "Saga Compliance Challenger",
        "customer_email": "violator@protonmail.com",
        "amount": 99.0,
        "item_id": "SHIRT_RESTRICTED_BLUE",
        "shipping_address": address_record,
    }

    command_envelope = {
        "command_id": str(uuid.uuid4()),
        "order_id": generated_order_id,
        "action": "NEW_SALE",
        "payload": order_payload,
    }

    context = SerializationContext("shipping_commands", MessageField.VALUE)
    producer.produce(
        topic="shipping_commands",
        key=generated_order_id,
        value=command_serializer(command_envelope, context),
    )
    producer.flush()

    saved_ledger = None
    for _ in range(10):
        time.sleep(0.5)
        saved_ledger = (
            clean_db.query(ShippingLedger)
            .filter(ShippingLedger.order_id == generated_order_id)
            .first()
        )
        if saved_ledger:
            break

    # Verify the local database records the compliance block
    assert saved_ledger is not None
    assert saved_ledger.status == "LEGAL_REJECTION_MI"

    # Verify a failure saga reply is staged inside the outbox table
    saved_outbox = (
        clean_db.query(ShippingOutbox)
        .filter(ShippingOutbox.key == generated_order_id)
        .first()
    )
    assert saved_outbox is not None

    reply_data = json.loads(saved_outbox.payload)
    assert reply_data["status"] == "FAILED"
    assert "Michigan" in reply_data["reason"]
