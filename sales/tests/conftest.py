import pytest
from observability.outbox import Base as OutboxBase
from sales.orchestrator.db import Base as SagaBase
from sales.orchestrator.db import SagaState

# Forcing Python to parse and register the table schemas in memory before creation
from sales.order_entry.db import Base as OrderBase
from sales.order_entry.db import Customer, Invoice
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="function")
def test_sales_ram_session():
    """Generates an independent, isolated relational memory canvas for sales testing."""
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )

    # Bind and construct all schemas natively inside volatile host RAM
    OrderBase.metadata.create_all(bind=engine)
    SagaBase.metadata.create_all(bind=engine)
    OutboxBase.metadata.create_all(bind=engine)

    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()
