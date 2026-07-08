# sales/src/sales/orchestrator/db.py

from sales.shared_models import SagaState, SharedBase
from sqlalchemy.orm import Session


# =========================================================================
# 🟢 HYBRID SHARD SCHEMAS INITIALIZATION
# =========================================================================
def init_orchestrator_db(engine) -> None:
    """Binds and maps the master orchestration table schemas natively onto the sales_domain logical shard [1.1]."""
    # 🟢 SOLUTION: Wall off master saga orchestration data into the private sales schema workspace [1.1]
    TARGET_SCHEMA = "sales_domain"
    SharedBase.metadata.schema = TARGET_SCHEMA

    SharedBase.metadata.create_all(bind=engine)


# =========================================================================
# 🟢 STATELESS DATA ACCESS WORKERS
# =========================================================================
def get_saga_state_by_order_id(db: Session, order_id: str) -> SagaState | None:
    return db.query(SagaState).filter(SagaState.order_id == str(order_id)).first()


def get_all_saga_states_by_status(db: Session, saga_status: str) -> list[SagaState]:
    return db.query(SagaState).filter(SagaState.saga_status == str(saga_status)).all()
