from datetime import datetime

from sqlalchemy import Column, DateTime, Float, String
from sqlalchemy.orm import DeclarativeBase


class SharedBase(DeclarativeBase):
    pass


class SagaState(SharedBase):
    """Unified master saga tracking log schema shared symmetrically

    between the REST API entrypoint and the background Orchestrator loop [1.1].
    """

    __tablename__ = "saga_states"
    order_id = Column(String, primary_key=True, index=True)
    saga_status = Column(String, nullable=False)
    finance_status = Column(String, default="PENDING")
    shipping_status = Column(String, default="PENDING")
    notifications_status = Column(String, default="PENDING")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # 🟢 Explicit tracking metrics columns populated during checkout initialization [1.1]
    customer_name = Column(String, nullable=True)
    customer_email = Column(String, nullable=True)
    amount = Column(Float, nullable=True)
    item_id = Column(String, nullable=True)

    # 🟢 SOLUTION: Explicitly add the missing delivery metrics columns to satisfy your endpoint!
    shipping_street = Column(String, nullable=True)
    shipping_city = Column(String, nullable=True)
    shipping_state = Column(String, nullable=True)
    shipping_postal = Column(String, nullable=True)
