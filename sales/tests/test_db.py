from sales.db import Base, SessionLocal


def get_clean_test_db_session():
    """Clears records from the development tables to ensure test isolation

    without dropping schemas or disrupting live running background microservices.
    """
    db = SessionLocal()
    try:
        # Fast, non-destructive row truncation across your active sales tables
        db.execute(Base.metadata.tables["sales_outbox"].delete())
        db.execute(Base.metadata.tables["saga_states"].delete())
        db.execute(Base.metadata.tables["invoices"].delete())
        db.execute(Base.metadata.tables["customers"].delete())
        db.commit()
    except Exception:
        db.rollback()
    return db
