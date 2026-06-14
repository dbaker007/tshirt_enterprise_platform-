from finance.db import DATABASE_URL, Base
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

engine = create_engine(DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_clean_test_db_session():
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    db.execute(
        text("TRUNCATE TABLE finance_ledger, finance_outbox RESTART IDENTITY CASCADE;")
    )
    db.commit()
    return db
