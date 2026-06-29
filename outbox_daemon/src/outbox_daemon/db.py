import os
from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


# =========================================================================
# 🗄️ Core Outbox Log Shard Schema
# =========================================================================
class Outbox(Base):
    """The central unified platform outbox transaction log shard table layout."""

    __tablename__ = "platform_outbox"
    id = Column(Integer, primary_key=True, index=True)
    topic = Column(String, nullable=False)
    partition_key = Column(String, nullable=False)
    payload = Column(Text, nullable=False)
    trace_context = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


def init_outbox_db(engine) -> None:
    """Binds and maps the outbox logging table schemas natively onto the provided engine context."""
    Base.metadata.create_all(bind=engine)
