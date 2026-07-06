import os

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from observability.db import get_platform_database_url

# Import your database model and session factories cleanly from your parent namespaces
from sales.shared_models import SagaState
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ui_router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Initialize an isolated database session builder for your UI queries
LOCAL_PORT = os.environ.get("SALES_GATEWAY_DB_PORT", "5432")
DATABASE_URL = get_platform_database_url(port=LOCAL_PORT)
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def mount_static_assets(app) -> None:
    """Mounts the physical disk asset tree onto the global FastAPI application instance."""
    static_abs_path = os.path.join(BASE_DIR, "static")
    app.mount("/static", StaticFiles(directory=static_abs_path), name="static")


@ui_router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def serve_control_panel_view(request: Request):
    """Renders and serves the synchronized administrative UI to the browser context."""
    return templates.TemplateResponse(
        request, name="control_panel.html", context={"request": request}
    )


@ui_router.get("/api/ui/pending-reviews", include_in_schema=False)
async def get_pending_fraud_reviews():
    """Queries the localized finance database shard directly to rehydrate live fraud review dashboard card elements [1.1]."""
    db = SessionLocal()
    try:
        # 1. Target the physical finance shard ledger rows to harvest active human holds [1.1]
        from sqlalchemy import text

        shard_rows = db.execute(
            text("""
            SELECT order_id FROM finance_ledger 
            WHERE execution_status = 'PENDING_HUMAN_REVIEW'
        """)
        ).fetchall()

        pending_order_ids = [str(row[0]) for row in shard_rows]

        if not pending_order_ids:
            return []

        # 2. Cross-reference those active order IDs back against your saga metadata logs to populate details [1.1]
        records = (
            db.query(SagaState)
            .filter(SagaState.order_id.in_(pending_order_ids))
            .order_by(SagaState.created_at.desc())
            .all()
        )

        payload_list = []
        for row in records:
            payload_list.append(
                {
                    "order_id": str(row.order_id),
                    "customer_name": getattr(row, "customer_name", "Anonymous Buyer"),
                    "customer_email": getattr(
                        row, "customer_email", "unknown@enterprise.io"
                    ),
                    "amount": float(getattr(row, "amount", 0.0)),
                    "item_id": getattr(row, "item_id", "UNKNOWN_ITEM"),
                    "shipping_address": {
                        "street": getattr(row, "shipping_street", "123 Default Way"),
                        "city": getattr(row, "shipping_city", "Default Ville"),
                        "state": getattr(row, "shipping_state", "OH"),
                        "postal_code": getattr(row, "shipping_postal", "00000"),
                    },
                }
            )

        return payload_list

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Relational cross-shard fetch failure: {str(e)}"
        )
    finally:
        db.close()
