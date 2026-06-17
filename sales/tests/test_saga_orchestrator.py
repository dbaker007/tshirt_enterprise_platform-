from unittest.mock import patch

import pytest
from sales.db import SagaState
from sales.saga_orchestrator import SalesSagaOrchestratorApplication

from .test_db import get_clean_test_db_session


@pytest.fixture(scope="function")
def clean_db():
    db = get_clean_test_db_session()
    try:
        yield db
    finally:
        db.close()


def test_sales_saga_orchestrator_failure_rollback_state_transition(clean_db):
    """SCENARIO: Verifies that when a downstream worker department reports a failure status,

    the real Saga Orchestrator accurately catches the error on the shared database
    and transitions the master tracking log state to REJECTED.
    """
    target_order_uuid = "sales-orchestration-uuid-303"

    initial_state = SagaState(
        order_id=target_order_uuid,
        saga_status="STARTED",
        finance_status="PENDING",
        shipping_status="PENDING",
        notifications_status="PENDING",
    )
    clean_db.add(initial_state)
    clean_db.commit()

    # Wrap the constructor initialization loop safely to avoid confluent network handshake crashes
    with (
        patch("confluent_kafka.Consumer"),
        patch("confluent_kafka.schema_registry.SchemaRegistryClient"),
        patch("confluent_kafka.schema_registry.avro.AvroDeserializer"),
    ):
        app = SalesSagaOrchestratorApplication()

    failed_worker_reply = {
        "order_id": target_order_uuid,
        "department": "SHIPPING",
        "status": "FAILED",
    }

    # Execute business logic directly against the shared development database session naturalmente
    app.execute_business_logic(
        order_payload=failed_worker_reply, action="EVALUATE_REPLY"
    )

    # Re-query the shared database session to verify the final terminal states
    updated_state = (
        clean_db.query(SagaState)
        .filter(SagaState.order_id == target_order_uuid)
        .first()
    )

    assert updated_state.shipping_status == "FAILED"
    assert updated_state.saga_status == "REJECTED"
