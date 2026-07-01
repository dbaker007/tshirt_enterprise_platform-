from unittest.mock import patch

import pytest
from sqlalchemy import text

with (
    patch("confluent_kafka.Consumer"),
    patch("confluent_kafka.schema_registry.SchemaRegistryClient", create=True),
    patch("confluent_kafka.schema_registry.avro.AvroDeserializer", create=True),
    patch("opentelemetry.trace.get_tracer"),
    patch("opentelemetry.sdk.trace.TracerProvider"),
    patch(
        "observability.db.get_platform_database_url", return_value="sqlite:///:memory:"
    ),
):
    from sales.orchestrator.main import SalesSagaOrchestratorApplication
    from sales.shared_models import SagaState


def test_orchestrator_advances_to_in_transit_on_complete_success_matrix(
    test_sales_ram_session,
):
    """Verifies that the orchestrator advances saga_status to IN_TRANSIT when all workers register success."""
    db = test_sales_ram_session
    order_id = "saga-test-uuid-001"

    running_saga = SagaState(
        order_id=order_id,
        saga_status="STARTED",
        finance_status="PENDING",
        shipping_status="PENDING",
        notifications_status="PENDING",
        customer_name="Test Buyer",
        customer_email="test@platform.internal",
        amount=100.00,
        item_id="SHIRT_STANDARD_BLUE",
    )
    db.add(running_saga)
    db.commit()

    with patch("sales.orchestrator.main.init_orchestrator_db"):
        app = SalesSagaOrchestratorApplication()

        app.process_incoming_saga_reply(
            {
                "order_id": order_id,
                "department": "FINANCE",
                "status": "SUCCESS",
                "ledger_status": "SUCCESS",
            },
            db=db,
        )
        app.process_incoming_saga_reply(
            {
                "order_id": order_id,
                "department": "SHIPPING",
                "status": "SUCCESS",
                "ledger_status": "SUCCESS",
            },
            db=db,
        )
        app.process_incoming_saga_reply(
            {
                "order_id": order_id,
                "department": "NOTIFICATIONS",
                "status": "SUCCESS",
                "ledger_status": "SUCCESS",
            },
            db=db,
        )
        db.commit()

    updated_saga = db.query(SagaState).filter(SagaState.order_id == order_id).first()
    assert updated_saga.saga_status == "IN_TRANSIT"
    assert updated_saga.finance_status == "SUCCESS"
    assert updated_saga.shipping_status == "SUCCESS"
    assert updated_saga.notifications_status == "SUCCESS"


def test_orchestrator_triggers_rollbacks_on_worker_failure(test_sales_ram_session):
    """Verifies that a FAILED worker packet forces a saga rejection and triggers compensating commands."""
    db = test_sales_ram_session
    order_id = "saga-failure-uuid-999"

    running_saga = SagaState(
        order_id=order_id,
        saga_status="STARTED",
        finance_status="CREDIT_APPROVED",
        shipping_status="PENDING",
        notifications_status="PENDING",
        customer_name="Test Buyer",
        customer_email="test@platform.internal",
        amount=100.00,
        item_id="SHIRT_STANDARD_BLUE",
    )
    db.add(running_saga)
    db.commit()

    with patch("sales.orchestrator.main.init_orchestrator_db"):
        app = SalesSagaOrchestratorApplication()

        app.process_incoming_saga_reply(
            {
                "order_id": order_id,
                "department": "SHIPPING",
                "status": "FAILED",
                "ledger_status": "LEGAL_REJECTION_MI",
            },
            db=db,
        )
        db.commit()

    updated_saga = db.query(SagaState).filter(SagaState.order_id == order_id).first()
    assert updated_saga.saga_status == "REJECTED"
    assert updated_saga.shipping_status == "LEGAL_REJECTION_MI"

    outbox_count = db.execute(text("SELECT count(*) FROM platform_outbox;")).scalar()
    assert outbox_count == 2
