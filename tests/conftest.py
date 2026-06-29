import os

import pytest

# Import the shared internal platform network routing controller
from observability.db import get_platform_database_url
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="session")
def finance_db_session():
    """Establishes a live, transactional session to the Finance database shard."""
    port = os.environ.get("FINANCE_DB_PORT", "5432")
    url = get_platform_database_url(port=port)
    engine = create_engine(url)
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    db = session_factory()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture(scope="session")
def shipping_db_session():
    """Establishes a live, transactional session to the Shipping database shard."""
    port = os.environ.get("SHIPPING_API_DB_PORT", "5432")
    url = get_platform_database_url(port=port)
    engine = create_engine(url)
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    db = session_factory()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture(scope="session")
def notifications_db_session():
    """Establishes a live, transactional session to the Notifications database shard."""
    port = os.environ.get("NOTIFICATIONS_DB_PORT", "5432")
    url = get_platform_database_url(port=port)
    engine = create_engine(url)
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    db = session_factory()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture(scope="session")
def orchestrator_db_session():
    """Establishes a live, transactional session to the Master Saga Orchestrator database shard."""
    port = os.environ.get("SALES_SAGA_DB_PORT", "5432")
    url = get_platform_database_url(port=port)
    engine = create_engine(url)
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    db = session_factory()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture(scope="session")
def gateway_api_url():
    """Returns the base local host URL pointing to your active FastAPI gateway port."""
    host = os.environ.get("SALES_GATEWAY_HOST", "http://localhost")
    port = os.environ.get("SALES_GATEWAY_PORT", "8000")
    return f"{host}:{port}"
