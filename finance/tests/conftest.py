import pytest
from finance.db import Base
from observability.outbox import Base as OutboxBase
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
    OutboxBase.metadata.create_all(bind=connection)

    TestingSessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=connection
    )
    session = TestingSessionLocal()

    try:
        yield session
    finally:
        session.close()
        connection.close()
