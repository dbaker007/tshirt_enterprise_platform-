import pytest
from observability.outbox import Base as OutboxBase

# Forcing Python to parse and register the table schemas in memory before creation
from shipping.db import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="function")
def test_db_session():
    """Generates a high-speed, completely isolated SQL database container loop inside volatile RAM."""
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )

    Base.metadata.create_all(bind=engine)
    OutboxBase.metadata.create_all(bind=engine)

    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()
