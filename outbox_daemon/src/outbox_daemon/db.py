import os
from datetime import datetime

from observability.db import get_platform_database_url
from sqlalchemy import Column, DateTime, Integer, String, Text, create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker


class Base(DeclarativeBase):
    pass


# =========================================================================
# 📡 COREDNS INTERNAL CLUSTER NETWORK CHANNEL (Standardized Environment-Aware)
# =========================================================================
LOCAL_PORT = os.environ.get("OUTBOX_DAEMON_DB_PORT", "5432")
DATABASE_URL = get_platform_database_url(port=LOCAL_PORT)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# =========================================================================
# 🗄️ RELEVANT ENGINE POLLED TABLES ONLY
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


# 🟢 FIX: Enforce uniform, self-contained schema initialization boundaries!
def init_outbox_db():
    Base.metadata.create_all(bind=engine)
