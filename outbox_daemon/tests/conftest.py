import pytest
from outbox_daemon.db import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="function")
def test_daemon_ram_session():
    """Generates a high-speed, completely isolated SQL database canvas inside volatile RAM

    by pinning a single persistent connection across asynchronous task threads [1.1].
    """
    # Use a standard, clean in-memory database engine pool
    test_engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )

    # 🟢 SOLUTION: Open and preserve a single raw connection channel handle! [1.1]
    connection = test_engine.connect()

    # Draw the core outbox log relational schemas inside this specific connection space [1.1]
    Base.metadata.create_all(bind=connection)

    # Build an independent session maker factory bound directly to the persistent connection channel [1.1]
    TestingSessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=connection
    )
    session = TestingSessionLocal()

    try:
        yield session
    finally:
        session.close()
        connection.close()  # Clean up the raw connection handle after the test lifecycle concludes [1.1]
