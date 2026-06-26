import os
from datetime import datetime

from observability.db import get_platform_database_url
from sqlalchemy import Column, DateTime, String, create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker


class Base(DeclarativeBase):
    pass


# =========================================================================
# 📡 COREDNS NETWORK ROUTING CONTROLS (Environment-Aware)
# =========================================================================
LOCAL_PORT = os.environ.get("SALES_SAGA_DB_PORT", "5432")
DATABASE_URL = get_platform_database_url(port=LOCAL_PORT)


engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# =========================================================================
# 🗄️ RELEVANT MICROSERVICE TRANSACTING TABLES ONLY
# =========================================================================
class SagaState(Base):
    __tablename__ = "saga_states"
    order_id = Column(String, primary_key=True, index=True)
    saga_status = Column(String, nullable=False)
    finance_status = Column(String, default="PENDING")
    shipping_status = Column(String, default="PENDING")
    notifications_status = Column(String, default="PENDING")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def init_orchestrator_db():
    Base.metadata.create_all(bind=engine)
