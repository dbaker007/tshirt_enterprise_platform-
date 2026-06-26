import pytest

# 🟢 STANDARDIZED LEAN IMPORTS: Importing schemas safely without side effects [1.1]
from notifications.db import Base, CommunicationLedger
from observability.outbox import Base as OutboxBase
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="function")
def test_db_session():
    """Generates a high-speed, completely isolated SQL database container loop inside volatile RAM."""
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )

    # Construct the schemas dynamically inside your volatile host memory block
    Base.metadata.create_all(bind=engine)
    OutboxBase.metadata.create_all(bind=engine)

    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()
