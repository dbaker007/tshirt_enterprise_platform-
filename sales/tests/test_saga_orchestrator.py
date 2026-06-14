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
from sales.db import Outbox, SagaState

# Import your active production orchestrator loop assets directly
from sales.saga_orchestrator import (
    process_incoming_saga_reply,
)

from .test_db import get_clean_test_db_session

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"


@pytest.fixture(scope="module")
def schema_registry():
    """Initializes the central schema registry contract link."""
    return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


@pytest.fixture(scope="module")
def reply_serializer(schema_registry):
    """Loads the shared SagaReply schema contract straight from the root schemas/ hub."""
    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "schemas", "saga_reply.avsc"
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
def background_orchestrator_thread():
    """Spawns an independent background thread running your live saga_orchestrator.py

    polling engine to intercept and route replies sent during test execution blocks.
    """
    stop_event = threading.Event()

    def run_orchestrator():
        # Setup modern database schemas natively once
        from sales.db import init_sales_db

        init_sales_db()

        # Compile an explicit, dedicated deserializer pointing directly to your reply schema
        client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        schema_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "schemas", "saga_reply.avsc"
        )
        with open(schema_path, "r") as f:
            reply_schema_str = f.read()

        reply_deserializer = AvroDeserializer(
            client, reply_schema_str, lambda obj, ctx: obj
        )

        config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"test_orchestrator_group_{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        consumer = Consumer(config)
        consumer.subscribe(["saga_replies"])

        while not stop_event.is_set():
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                break

            try:
                # FIXED: Decodes using the dedicated reply schema instead of a command envelope!
                context = SerializationContext(msg.topic(), MessageField.VALUE)
                reply_payload = reply_deserializer(msg.value(), context)

                # Forward data packet straight into your production checklist processor
                process_incoming_saga_reply(reply_payload)
                consumer.commit(msg, asynchronous=False)
            except Exception:
                consumer.commit(msg, asynchronous=False)

        consumer.close()

    thread = threading.Thread(target=run_orchestrator, daemon=True)
    thread.start()
    yield
    stop_event.set()
    thread.join(timeout=2.0)


# =========================================================================
# SAGA ORCHESTRATION INTEGRATION TEST CASES
# =========================================================================


def test_orchestrator_completes_saga_when_all_departments_clear_success(
    reply_serializer, clean_db, background_orchestrator_thread
):
    """SCENARIO: All departments stream a SUCCESS outcome token to saga_replies.

    The Conductor checklist engine must mark the master order state status as COMPLETED.
    """
    generated_order_id = str(uuid.uuid4())
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    context = SerializationContext("saga_replies", MessageField.VALUE)

    # 1. Seed an active, un-started Saga log row natively into your database
    initial_log = SagaState(
        order_id=generated_order_id,
        status="STARTED",
        finance_status="PENDING",
        shipping_status="PENDING",
        notifications_status="PENDING",
    )
    clean_db.add(initial_log)
    clean_db.commit()

    # 2. Simulate worker daemons reporting their success states over the Kafka network
    departments_replies = [
        {"department": "FINANCE", "status": "SUCCESS"},
        {"department": "SHIPPING", "status": "SUCCESS"},
        {"department": "NOTIFICATIONS", "status": "SUCCESS"},
    ]

    for reply in departments_replies:
        payload = {
            "order_id": generated_order_id,
            "department": reply["department"],
            "status": reply["status"],
            "reason": "Test clearance pass",
            "timestamp": "2026-06-14T12:00:00Z",
        }
        producer.produce(
            topic="saga_replies",
            key=generated_order_id,
            value=reply_serializer(payload, context),
        )
    producer.flush()

    # 3. Defensive database verification polling loop
    saga_state_log = None
    for _ in range(10):
        time.sleep(0.5)
        # Refresh session cache to view out-of-band background updates
        clean_db.expire_all()
        saga_state_log = (
            clean_db.query(SagaState)
            .filter(SagaState.order_id == generated_order_id)
            .first()
        )
        if saga_state_log and saga_state_log.status == "COMPLETED":
            break

    # 4. CORE ORCHESTRATION STATE ASSERTION
    assert saga_state_log is not None
    assert saga_state_log.status == "COMPLETED", (
        f"❌ Expected COMPLETED, but got {saga_state_log.status}"
    )


def test_orchestrator_triggers_compensating_cancellations_upon_worker_failure(
    reply_serializer, clean_db, background_orchestrator_thread
):
    """SCENARIO: Finance department reports a PAYMENT_REJECTED fraud error token.

    The Conductor must flip global status to REJECTED and stage compensating commands to the outbox.
    """
    generated_order_id = str(uuid.uuid4())
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    context = SerializationContext("saga_replies", MessageField.VALUE)

    # 1. Seed an active, un-started Saga log row natively into your database
    initial_log = SagaState(
        order_id=generated_order_id,
        status="STARTED",
        finance_status="PENDING",
        shipping_status="PENDING",
        notifications_status="PENDING",
    )
    clean_db.add(initial_log)
    clean_db.commit()

    # 2. Simulate the Finance department reporting a fraud rejection over the network
    failure_payload = {
        "order_id": generated_order_id,
        "department": "FINANCE",
        "status": "PAYMENT_REJECTED",
        "reason": "FRAUD_THRESHOLD_EXCEEDED",
        "timestamp": "2026-06-14T12:05:00Z",
    }
    producer.produce(
        topic="saga_replies",
        key=generated_order_id,
        value=reply_serializer(failure_payload, context),
    )
    producer.flush()

    # 3. Defensive database verification polling loop
    saga_state_log = None
    staged_compensations = []
    for _ in range(10):
        time.sleep(0.5)
        clean_db.expire_all()
        saga_state_log = (
            clean_db.query(SagaState)
            .filter(SagaState.order_id == generated_order_id)
            .first()
        )
        staged_compensations = (
            clean_db.query(Outbox).filter(Outbox.key == generated_order_id).all()
        )
        if (
            saga_state_log
            and saga_state_log.status == "REJECTED"
            and len(staged_compensations) > 0
        ):
            break

    # 4. CORE STATE ASSERTIONS
    assert saga_state_log is not None
    assert saga_state_log.status == "REJECTED"
    assert saga_state_log.finance_status == "PAYMENT_REJECTED"

    # 🛡️ THE COMPENSATION PROOF LOCK: Verify exactly 2 rollbacks were written (shipping & notifications)
    # The triggering department (FINANCE) is skipped to break infinite command loops!
    assert len(staged_compensations) == 2, (
        f"❌ Expected 2 compensations, but found {len(staged_compensations)}"
    )

    staged_topics = [cmd.topic for cmd in staged_compensations]
    assert "shipping_commands" in staged_topics
    assert "notifications_commands" in staged_topics

    # Confirm the raw serialized packet action string is explicitly directed to roll back
    sample_envelope = json.loads(staged_compensations[0].payload)
    assert sample_envelope["action"] == "CANCEL_TRANSACTION"
