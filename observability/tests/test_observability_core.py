# observability/tests/test_observability_core.py

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

# 🟢 SOLUTION: Import your new Core metadata and outbox_table instead of the legacy ORM classes! [1.1]
from observability.outbox import metadata, outbox_table, stage_outbox_message
from observability.tracing import KafkaHeaderGetter, kafka_getter
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="function")
def test_ram_session():
    """Generates an independent, isolated relational memory canvas for shared utility tests."""
    engine = create_engine("sqlite:///:memory:")
    # 🟢 SOLUTION: Bind and draw the abstract core metadata tables onto the local SQLite memory canvas! [1.1]
    metadata.create_all(bind=engine)
    TestingSessionLocal = sessionmaker(bind=engine)
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()


def test_stage_outbox_message_successfully_injects_and_serializes(test_ram_session):
    """Verifies that the shared outbox utility correctly captures thread payloads
    and dual-writes an explicit JSON string entry straight into the platform table log.
    """
    mock_payload = {"order_id": "global-test-uuid-999", "status": "APPROVED"}

    success = stage_outbox_message(
        db=test_ram_session,
        topic="test_topic",
        partition_key="global-test-uuid-999",
        payload=mock_payload,
    )

    assert success is True

    # 🟢 SOLUTION: Use core select execution models instead of the old legacy ORM tracking models! [1.1]
    query = select(outbox_table)
    record = test_ram_session.execute(query).fetchone()

    assert record is not None
    # SQLAlchemy Core rows can be accessed via column name string tokens directly [1.1]
    assert record._mapping["topic"] == "test_topic"
    assert record._mapping["partition_key"] == "global-test-uuid-999"

    parsed_payload = json.loads(record._mapping["payload"])
    assert parsed_payload["order_id"] == "global-test-uuid-999"


def test_stage_outbox_message_raises_exception_on_serialization_failure(
    test_ram_session,
):
    """Verifies that the outbox utility throws an explicit exception if the payload contains
    non-serializable data, preventing silent data drops down the pipeline.
    """
    broken_payload = {"timestamp": datetime.utcnow()}

    with pytest.raises(Exception):
        stage_outbox_message(
            db=test_ram_session,
            topic="fail_topic",
            partition_key="error-101",
            payload=broken_payload,
        )


def test_kafka_header_getter_resolves_byte_and_string_keys_cleanly():
    """Verifies that the case-insensitive W3C header getter parses both raw wire
    bytes and text string tokens without throwing string allocation exceptions.
    """
    getter = KafkaHeaderGetter()

    mock_headers = [
        (b"traceparent", b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        ("Custom-Header", "StandardStringValue"),
    ]

    binary_result = getter.get(mock_headers, "traceparent")
    assert binary_result == ["00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"]

    string_result = getter.get(mock_headers, "custom-header")
    assert string_result == ["StandardStringValue"]


def test_kafka_header_getter_handles_empty_or_missing_carriers_gracefully():
    """Verifies that the propagation getter returns safe defaults when headers are missing."""
    getter = KafkaHeaderGetter()

    assert getter.get([], "traceparent") is None
    assert getter.get(None, "traceparent") is None
    assert getter.keys([]) == []
