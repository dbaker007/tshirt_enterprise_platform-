# Import your active production connection parameters and model trackers directly
from notifications.db import DATABASE_URL, Base
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

engine = create_engine(DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_clean_test_db_session():
    """Ensures notifications schemas exist, truncates rows out-of-band to guarantee data

    isolation, and returns an isolated testing session handler.
    """
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        db.execute(
            text(
                "TRUNCATE TABLE communication_ledger, notification_outbox RESTART IDENTITY CASCADE;"
            )
        )
        db.commit()
        return db
    except Exception as e:
        db.rollback()
        raise e
