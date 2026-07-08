# finance/src/finance/web.py

import logging
import sys

from fastapi import Depends, FastAPI, HTTPException
from observability.db import get_platform_database_url
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

# Import your existing shard schema utilities and declarative helpers directly
from finance.db import get_all_finance_ledgers_by_status, init_finance_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("FINANCE_API_SHARD")

# 1. Dynamically retrieve the identical centralized database URL credentials string [1.1]
DATABASE_URL = get_platform_database_url()
# 🟢 SOLUTION: Keep the query plane engine pool clean, raw, and aligned with metadata boundaries
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Idempotently guarantee the schema table tracks match before mounting connections [1.1]
init_finance_db(engine)

app = FastAPI(
    title="Finance Shard Programmatic Web API",
    description="Isolated Data Query Plane for Enterprise Operations Automation [1.1]",
)


# Operational dependency provider to yield isolated database sessions cleanly to endpoints
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/finance/reviews/pending")
def list_active_fraud_hold_uuids(db: Session = Depends(get_db)):
    """
    Scans the private finance shard database ledger table to aggregate
    and return an array of all order IDs currently frozen inside a fraud review hold [1.1].
    """
    logger.info(
        "📡 [SHARD INTERROGATION]: Inbound lookup scanning for PENDING_HUMAN_REVIEW fields..."
    )

    try:
        # Leverage your existing, production-hardened query tool natively!
        hold_records = get_all_finance_ledgers_by_status(
            db, execution_status="PENDING_HUMAN_REVIEW"
        )

        # Extract and convert the SQLAlchemy model fields into raw string array primitives
        order_ids_list = [str(record.order_id) for record in hold_records]

        logger.info(
            f"   └── ✔ Found {len(order_ids_list)} orders matching active review state criteria."
        )
        return order_ids_list

    except Exception as query_err:
        logger.error(f"❌ Shard relational query block exception: {str(query_err)}")
        raise HTTPException(
            status_code=500,
            detail=f"Private finance ledger database access boundary failure: {str(query_err)}",
        )


@app.get("/health")
def health_check():
    """Kubernetes cluster operational probe checkpoint."""
    return {"status": "HEALTHY", "service": "FINANCE_WEB_API"}
