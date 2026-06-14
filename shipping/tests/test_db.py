# Import your active production connection parameters and model trackers directly
from shipping.db import DATABASE_URL, Base
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

engine = create_engine(DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_clean_test_db_session():
    """Ensures tables exist, truncates row entries out-of-band to guarantee data

    isolation, and returns an isolated testing session handler.
    """
    # Force synchronization check
    Base.metadata.create_all(bind=engine)

    db = TestingSessionLocal()
    try:
        # Encapsulate the raw SQL purge script completely within the data namespace boundary
        db.execute(
            text(
                "TRUNCATE TABLE shipping_ledger, shipping_outbox RESTART IDENTITY CASCADE;"
            )
        )
        db.commit()
        return db
    except Exception as e:
        db.rollback()
        print(f"❌ [TEST DB CRITICAL]: Data purge verification sweep failed: {str(e)}")
        raise e
