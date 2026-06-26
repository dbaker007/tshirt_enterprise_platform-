import json
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

with (
    patch("confluent_kafka.Consumer"),
    patch("confluent_kafka.schema_registry.SchemaRegistryClient", create=True),
    patch("confluent_kafka.schema_registry.avro.AvroDeserializer", create=True),
):
    from observability.outbox import Base as OutboxBase
    from sales.orchestrator.db import Base, SagaState
    from sales.orchestrator.main import SalesSagaOrchestratorApplication


@pytest.fixture(scope="function")
def test_orchestrator_ram_session():
    """Generates an independent, isolated relational memory canvas for orchestrator tests."""
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=engine)
    OutboxBase.metadata.create_all(bind=engine)

    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()


def test_orchestrator_advances_to_in_transit_on_complete_success_matrix(
    test_orchestrator_ram_session,
):
    """Verifies that the orchestrator advances saga_status to IN_TRANSIT when all workers register success."""
    running_saga = SagaState(
        order_id="saga-test-uuid-001",
        saga_status="STARTED",
        finance_status="PENDING",
        shipping_status="PENDING",
        notifications_status="PENDING",
    )
    test_orchestrator_ram_session.add(running_saga)
    test_orchestrator_ram_session.commit()

    with patch(
        "sales.orchestrator.main.SessionLocal",
        return_value=test_orchestrator_ram_session,
    ):
        app = SalesSagaOrchestratorApplication()

        # 🟢 FIX: Passing "SUCCESS" ensures the orchestrator treats these as approved forward steps!
        app.process_incoming_saga_reply(
            {
                "order_id": "saga-test-uuid-001",
                "department": "FINANCE",
                "status": "SUCCESS",
            }
        )
        app.process_incoming_saga_reply(
            {
                "order_id": "saga-test-uuid-001",
                "department": "SHIPPING",
                "status": "SUCCESS",
            }
        )
        app.process_incoming_saga_reply(
            {
                "order_id": "saga-test-uuid-001",
                "department": "NOTIFICATIONS",
                "status": "SUCCESS",
            }
        )

    updated_saga = (
        test_orchestrator_ram_session.query(SagaState)
        .filter(SagaState.order_id == "saga-test-uuid-001")
        .first()
    )
    assert updated_saga.saga_status == "IN_TRANSIT"
    assert updated_saga.finance_status == "SUCCESS"
    assert updated_saga.shipping_status == "SUCCESS"
    assert updated_saga.notifications_status == "SUCCESS"


@patch("sales.orchestrator.main.SessionLocal")
def test_orchestrator_triggers_rollbacks_on_worker_failure(
    mock_session_maker, test_orchestrator_ram_session
):
    """Verifies that a FAILED worker packet forces a saga rejection and triggers compensating commands."""
    mock_session_maker.return_value = test_orchestrator_ram_session

    running_saga = SagaState(
        order_id="saga-failure-uuid-999",
        saga_status="STARTED",
        finance_status="CREDIT_APPROVED",  # Finance already cleared
        shipping_status="PENDING",
        notifications_status="PENDING",
    )
    test_orchestrator_ram_session.add(running_saga)
    test_orchestrator_ram_session.commit()

    app = SalesSagaOrchestratorApplication()

    # Ingest a direct failure signal (e.g., Shipping legal violation)
    app.process_incoming_saga_reply(
        {
            "order_id": "saga-failure-uuid-999",
            "department": "SHIPPING",
            "status": "FAILED",
        }
    )

    # 1. Ensure the master state was forcefully aborted on disk
    updated_saga = (
        test_orchestrator_ram_session.query(SagaState)
        .filter(SagaState.order_id == "saga-failure-uuid-999")
        .first()
    )
    assert updated_saga.saga_status == "REJECTED"

    # 2. Ensure compensating rollback messages were dual-written to your central platform outbox table! [1.1]
    outbox_count = test_orchestrator_ram_session.execute(
        text("SELECT count(*) FROM platform_outbox;")
    ).scalar()
    assert (
        outbox_count == 2
    )  # 2 cancellations staged (Finance and Notifications, bypassing the failed Shipping)
