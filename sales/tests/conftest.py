# sales/tests/conftest.py

import pytest

# 🟢 SOLUTION: Import your clean Core metadata object instead of the legacy Base class! [1.1]
from observability.outbox import metadata as outbox_metadata
from sales.order_entry.db import Base as OrderBase
from sales.shared_models import SharedBase as SagaBase
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="function")
def test_sales_ram_session():
    """Generates a high-speed, completely isolated SQL database canvas inside volatile RAM
    by pinning a single persistent connection across asynchronous task threads [1.1].
    """
    # Use a standard, clean in-memory database engine pool
    test_engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )

    # Open and preserve a single raw connection channel handle! [1.1]
    connection = test_engine.connect()

    # Draw all core sales domain relational schemas inside this specific connection space [1.1]
    OrderBase.metadata.create_all(bind=connection)
    SagaBase.metadata.create_all(bind=connection)

    # 🟢 SOLUTION: Draw the abstract outbox logging structure onto your persistent connection! [1.1]
    outbox_metadata.create_all(bind=connection)

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
