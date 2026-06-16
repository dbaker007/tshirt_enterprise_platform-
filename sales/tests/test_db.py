# Import your active production connection parameters and model trackers directly
from sales.db import DATABASE_URL, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine(DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_clean_test_db_session():
    """Drops and rebuilds all tables to guarantee complete data isolation

    across parallel test execution sweeps, and returns a clean session.
    """
    # Force a complete clean-slate cycle across all tables (including saga_states)
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    return TestingSessionLocal()
