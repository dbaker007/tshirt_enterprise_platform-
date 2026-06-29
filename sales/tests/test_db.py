import uuid

from sales.order_entry.db import (
    Customer,
    Invoice,
    SagaState,
    initialize_saga_state_tracking,
    persist_invoice_record,
    resolve_or_create_customer,
    stage_saga_command_envelopes,
)
from sqlalchemy import text


def test_sales_order_entry_workers_execute_atomically(test_sales_ram_session):
    """Verifies that the split stateless workers correctly populate customer records,

    invoice records, saga tracking matrices, and outbox frames within an open transaction.
    """
    db = test_sales_ram_session
    generated_order_id = str(uuid.uuid4())
    order_amount = 120.50

    customer_info = {"name": "Bob Vance", "email": "bob@vanceair.com"}
    avro_compatible_payload = {
        "customer_name": "Bob Vance",
        "customer_email": "bob@vanceair.com",
        "amount": order_amount,
        "item_id": "SHIRT_PREMIUM_RED",
        "shipping_address": {
            "street": "123 Default Way",
            "city": "Scranton",
            "state": "PA",
            "postal_code": "18503",
        },
    }

    # Execute your split stateless workers using the shared transaction context
    customer_record = resolve_or_create_customer(db, customer_info)
    invoice_record = persist_invoice_record(
        db=db,
        order_id=generated_order_id,
        customer_id=customer_record.id,
        amount=order_amount,
    )

    # 🟢 SOLUTION: Align parameters by passing the payload mapping block natively!
    initialize_saga_state_tracking(db, generated_order_id, avro_compatible_payload)
    stage_saga_command_envelopes(db, generated_order_id, avro_compatible_payload)

    # Commit the RAM transaction out-of-band to verify persistence
    db.commit()

    # 1. Verify Customer presence
    customer = db.query(Customer).filter(Customer.email == "bob@vanceair.com").first()
    assert customer is not None
    assert customer.customer_name == "Bob Vance"

    # 2. Verify Invoice mapping
    invoice = db.query(Invoice).filter(Invoice.order_id == generated_order_id).first()
    assert invoice is not None
    assert invoice.amount == 120.50

    # 3. Verify Saga Orchestration row instantiation
    saga = db.query(SagaState).filter(SagaState.order_id == generated_order_id).first()
    assert saga is not None
    assert saga.saga_status == "STARTED"
    assert saga.customer_name == "Bob Vance"
    assert saga.amount == 120.50

    # 4. Verify three separate command rows were staged in your central platform outbox table!
    outbox_count = db.execute(text("SELECT count(*) FROM platform_outbox;")).scalar()
    assert outbox_count == 3
