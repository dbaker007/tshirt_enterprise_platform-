import json
from unittest.mock import MagicMock, patch

import pytest

# Shield file loading by intercepting the centralized infrastructure factory at the absolute top [1.1]
with patch(
    "observability.db.get_platform_database_url", return_value="sqlite:///:memory:"
):
    from outbox_daemon.db import Outbox
    from outbox_daemon.main import process_single_row


@patch("outbox_daemon.main.Producer")
@patch("outbox_daemon.main.SchemaRegistryClient")
@patch("outbox_daemon.main.AvroSerializer")
def test_daemon_extracts_and_serializes_valid_row_cleanly(
    mock_serializer, mock_registry, mock_producer_class, test_daemon_ram_session
):
    """Verifies that the daemon correctly parses valid rows and pushes them onto the broker."""
    mock_producer_instance = MagicMock()
    mock_producer_class.return_value = mock_producer_instance

    # Create a clean mock serializer callable that returns dummy bytes instead of hitting the network [1.1]
    mock_serializer_instance = MagicMock()
    mock_serializer_instance.return_value = b"mock-serialized-avro-bytes"

    sample_payload = {
        "order_id": "daemon-test-101",
        "department": "FINANCE",
        "status": "SUCCESS",
        "reason": "Test clearance verified via unit check.",
        "timestamp": "2026-06-24T12:00:00Z",
    }

    row_entry = Outbox(
        topic="saga_replies",
        partition_key="daemon-test-101",
        payload=json.dumps(sample_payload),
        trace_context="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    )
    test_daemon_ram_session.add(row_entry)
    test_daemon_ram_session.commit()

    # Patch both the producer and the reply_serializer on the daemon main module namespace [1.1]
    with (
        patch("outbox_daemon.main.producer", mock_producer_instance),
        patch("outbox_daemon.main.reply_serializer", mock_serializer_instance),
    ):
        success = process_single_row(db=test_daemon_ram_session, row=row_entry)
        assert success is True
        mock_producer_instance.produce.assert_called_once()


def test_daemon_throws_value_error_if_payload_column_is_null(
    test_daemon_ram_session,
):
    """Verifies that the processing pipeline instantly flags and fails corrupted database null entries."""
    corrupted_row = Outbox(
        topic="saga_replies", partition_key="corrupted-key-999", payload=None
    )

    with pytest.raises(ValueError):
        process_single_row(db=test_daemon_ram_session, row=corrupted_row)
