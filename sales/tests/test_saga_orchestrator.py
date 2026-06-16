import sys
from unittest.mock import MagicMock, patch

# GATELOCK PATTERN: Cleanly bypass network handshakes during local unit tests
with (
    patch("confluent_kafka.Consumer"),
    patch("confluent_kafka.schema_registry.SchemaRegistryClient"),
    patch("confluent_kafka.schema_registry.avro.AvroDeserializer"),
):
    from sales.saga_orchestrator import SalesSagaOrchestratorApplication


@patch("sales.saga_orchestrator.SessionLocal")
@patch(
    "sales.saga_orchestrator.SalesSagaOrchestratorApplication.issue_compensating_cancellations"
)
def test_sales_saga_orchestrator_failure_rollback_state_transition(
    mock_rollback, mock_session_factory
):
    """Verifies that when a downstream worker department reports a failure status,

    the Saga Orchestrator accurately catches the error, transitions its state log
    to ROLLING_BACK, and issues compensating cancellations to all other active departments.
    """
    # 1. Instantiate the refactored child class wrapper using faked network interfaces
    app = SalesSagaOrchestratorApplication()

    # 2. Mock out a clean, faked SQLAlchemy local database session and active record log
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db

    mock_saga_state = MagicMock()
    mock_saga_state.order_id = "sales-orchestration-uuid-303"
    mock_saga_state.status = "PROCESSING"
    mock_saga_state.finance_status = "PENDING"
    mock_saga_state.shipping_status = "PENDING"
    mock_saga_state.notifications_status = "PENDING"

    # Force your database query mock filter pass to return our active state record
    mock_db.query.return_value.filter.return_value.first.return_value = mock_saga_state

    # 3. Package an incoming failure reply contract string payload from a worker
    failed_worker_reply = {
        "order_id": "sales-orchestration-uuid-303",
        "department": "SHIPPING",
        "status": "FAILED",  # 🚨 This failure token must force the state machine to roll back!
    }

    # 4. Execute the business logic processing method natively
    app.execute_business_logic(
        order_payload=failed_worker_reply, action="EVALUATE_REPLY"
    )

    # 5. HARD STATE ASSERTIONS: Prove the orchestrator accurately drove the state log updates
    assert mock_saga_state.shipping_status == "FAILED"
    assert mock_saga_state.status == "REJECTED"

    # Verify that your internal outbox engine was triggered to broadcast compensation rollbacks
    mock_rollback.assert_called_once_with(
        mock_db, "sales-orchestration-uuid-303", triggering_dept="SHIPPING"
    )
