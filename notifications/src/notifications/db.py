# notifications/src/notifications/db.py

from datetime import datetime

from observability.outbox import stage_outbox_message
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Session


class Base(DeclarativeBase):
    pass


class CommunicationLedger(Base):
    __tablename__ = "communication_ledger"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, unique=True, index=True)
    customer_name = Column(String)
    execution_status = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


# =========================================================================
# 🟢 HYBRID SHARD SCHEMAS INITIALIZATION
# =========================================================================
def init_notifications_db(engine) -> None:
    """Binds and maps the business communication schemas straight onto the notifications_domain shard."""
    # 🟢 SOLUTION: Wall off internal notification data records into their private schema workspace [1.1]
    TARGET_SCHEMA = "notifications_domain"
    Base.metadata.schema = TARGET_SCHEMA

    Base.metadata.create_all(bind=engine)


# =========================================================================
# 🟢 STATELESS DATA ACCESS WORKERS (Shared Unit-of-Work Targets)
# =========================================================================
def persist_communication_ledger_record(
    db: Session, order_id: str, customer_name: str, ledger_status: str
) -> None:
    """Stateless data access worker.
    Uses a safe integrity-catch block to handle race conditions across dialects.
    """
    target_order_id = str(order_id)
    name_str = str(customer_name)
    status_str = str(ledger_status)

    try:
        with db.begin_nested():
            new_record = CommunicationLedger(
                order_id=target_order_id,
                customer_name=name_str,
                execution_status=status_str,
            )
            db.add(new_record)
            db.flush()
    except IntegrityError:
        record = (
            db.query(CommunicationLedger)
            .filter(CommunicationLedger.order_id == target_order_id)
            .first()
        )
        if record:
            record.customer_name = name_str
            record.execution_status = status_str
            db.flush()


def stage_notifications_saga_reply(
    db: Session, order_id: str, wire_status: str, ledger_status: str
) -> None:
    """Stateless data access worker.
    Packages the standardized saga contract and routes it into the central platform outbox.
    """
    reply_envelope = {
        "order_id": str(order_id),
        "department": "NOTIFICATIONS",
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


def get_communication_ledger_by_order_id(
    db: Session, order_id: str
) -> CommunicationLedger | None:
    """Programmatic Lookup: Retrieves the local communication record for a specific order UUID."""
    return (
        db.query(CommunicationLedger)
        .filter(CommunicationLedger.order_id == str(order_id))
        .first()
    )


def get_all_communication_ledgers_by_status(
    db: Session, execution_status: str
) -> list[CommunicationLedger]:
    """Programmatic Filter: Returns a list of all communication records matching a specific execution status."""
    return (
        db.query(CommunicationLedger)
        .filter(CommunicationLedger.execution_status == str(execution_status))
        .all()
    )
