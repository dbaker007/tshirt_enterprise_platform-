import pytest

# 🟢 CRITICAL MODEL IMPORTS: Forcing python to parse and register the table schemas in memory [1.1]
from finance.db import Base, FinanceLedger
from observability.outbox import Base as OutboxBase
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="function")
def test_db_session():
    """Generates a high-speed, completely isolated SQL database container loop inside volatile RAM."""
    # 🟢 RAM ISOLATION: Explicitly bind the session engine straight to an in-memory socket!
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )

    # Idempotently map the core relational schema definitions straight onto the transient SQLite engine [1.1]
    Base.metadata.create_all(bind=engine)
    OutboxBase.metadata.create_all(bind=engine)

    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = TestingSessionLocal()

    try:
        yield session
    finally:
        session.close()
