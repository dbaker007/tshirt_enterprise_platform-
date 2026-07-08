# shipping/src/shipping/db.py

import os
from datetime import datetime

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from observability.outbox import stage_outbox_message
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Session


class Base(DeclarativeBase):
    pass


class ShippingLedger(Base):
    __tablename__ = "shipping_ledger"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, unique=True, index=True)
    execution_status = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


# =========================================================================
# 🟢 HYBRID SHARD SCHEMAS INITIALIZATION
# =========================================================================
def init_shipping_db(engine) -> None:
    """Binds and maps the core relational shipping schemas straight onto the shipping_domain shard."""
    # 🟢 SOLUTION: Wall off shipping business state data inside its private schema workspace [1.1]
    TARGET_SCHEMA = "shipping_domain"
    Base.metadata.schema = TARGET_SCHEMA

    Base.metadata.create_all(bind=engine)


async def get_shipping_checkpointer() -> AsyncSqliteSaver:
    """Instantiates and returns the persistent non-blocking asynchronous SQLite checkpointer instance."""
    checkpoint_db_path = os.environ.get(
        "SHIPPING_CHECKPOINT_DB_PATH", "shipping_checkpoints.sqlite"
    )
    return AsyncSqliteSaver.from_conn_string(checkpoint_db_path)


# =========================================================================
# 🟢 STATELESS DATA ACCESS WORKERS (Shared Unit-of-Work Targets)
# =========================================================================
def persist_shipping_ledger_record(
    db: Session, order_id: str, ledger_status: str
) -> None:
    """Stateless data access worker.
    Uses a safe integrity-catch block to handle race conditions across dialects.
    """
    target_order_id = str(order_id)
    status_str = str(ledger_status)

    try:
        with db.begin_nested():
            new_record = ShippingLedger(
                order_id=target_order_id, execution_status=status_str
            )
            db.add(new_record)
            db.flush()
    except IntegrityError:
        record = (
            db.query(ShippingLedger)
            .filter(ShippingLedger.order_id == target_order_id)
            .first()
        )
        if record:
            record.execution_status = status_str
            db.flush()


def stage_shipping_saga_reply(
    db: Session,
    order_id: str,
    wire_status: str,
    ledger_status: str,
) -> None:
    """Stateless data access worker.
    Packages the standardized saga contract and routes it into the central platform outbox.
    """
    reply_envelope = {
        "order_id": str(order_id),
        "department": "SHIPPING",
        "status": str(wire_status),
        "ledger_status": str(ledger_status),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

    # 🟢 SOLUTION: Dispatches straight to your pristine, un-translated public outbox logging framework [1.1]
    stage_outbox_message(
        db=db,
        topic="saga_replies",
        partition_key=str(order_id),
        payload=reply_envelope,
    )


def get_shipping_ledger_by_order_id(
    db: Session, order_id: str
) -> ShippingLedger | None:
    """Programmatic Lookup: Retrieves the local shipping shard record for a specific order UUID."""
    return (
        db.query(ShippingLedger)
        .filter(ShippingLedger.order_id == str(order_id))
        .first()
    )


def get_all_shipping_ledgers_by_status(
    db: Session, execution_status: str
) -> list[ShippingLedger]:
    """Programmatic Filter: Returns a list of all shipping records matching a specific execution status."""
    return (
        db.query(ShippingLedger)
        .filter(ShippingLedger.execution_status == str(execution_status))
        .all()
    )
