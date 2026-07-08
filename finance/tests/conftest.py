# finance/tests/conftest.py

import pytest
from finance.db import Base

# 🟢 SOLUTION: Import the clean Core metadata object instead of the legacy ORM class! [1.1]
from observability.outbox import metadata as outbox_metadata
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="function")
def test_db_session():
    """Generates a high-speed, completely isolated SQL database canvas inside volatile RAM
    by pinning a single persistent connection across asynchronous task threads.
    """
    test_engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )

    connection = test_engine.connect()
    Base.metadata.create_all(bind=connection)

    # 🟢 SOLUTION: Draw the abstract outbox logging structure onto your persistent connection! [1.1]
    outbox_metadata.create_all(bind=connection)

    TestingSessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=connection
    )
    session = TestingSessionLocal()

    try:
        yield session
    finally:
        session.close()
        connection.close()
