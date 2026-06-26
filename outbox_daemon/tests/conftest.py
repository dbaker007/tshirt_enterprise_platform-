import pytest

# GATELOCK PATTERN: Cleanly bypass network assets by using structural create parameters
from outbox_daemon.db import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="function")
def test_daemon_ram_session():
    """Generates an independent, isolated relational memory canvas for daemon testing."""
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=engine)

    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()
