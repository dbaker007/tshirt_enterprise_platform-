import logging
import os
import sys

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from observability.db import get_platform_database_url
from sales.shared_models import SagaState
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("SALES_UI_SERVER")

LOCAL_PORT = os.environ.get("SALES_GATEWAY_DB_PORT", "5432")
DATABASE_URL = get_platform_database_url(port=LOCAL_PORT)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

app = FastAPI(title="Saga Control Panel Hub")


# =========================================================================
# 📡 BACKEND APIS: Fetch Live Pending Fraud Threads From Local Shard DB
# =========================================================================
@app.get("/api/ui/pending-reviews")
async def get_pending_fraud_reviews():
    """Queries the centralized tracking log to find transactions locked inside the Finance review loop."""
    db = SessionLocal()
    try:
        # Search the shared models schema space for orders marked PENDING_HUMAN_REVIEW [1.1]
        records = (
            db.query(SagaState)
            .filter(SagaState.finance_status == "PENDING_HUMAN_REVIEW")
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
                    "created_at": row.created_at.isoformat() if row.created_at else "",
                }
            )
        return payload_list
    except Exception as e:
        logger.error(f"Failed to query local tracking records context: {str(e)}")
        raise HTTPException(status_code=500, detail="Relational shard fetch failure.")
    finally:
        db.close()


@app.get("/", response_class=HTMLResponse)
async def serve_control_panel_view():
    """Serves the synchronized responsive web interface to the operator's browser [1.1]."""
    html_content = """
    <!DOCTYPE html>
    <html lang="en" class="h-full bg-slate-900 text-slate-100">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Enterprise Saga Platform Operations Hub</title>
        <script src="https://tailwindcss.com"></script>
    </head>
    <body class="h-full flex flex-col font-sans antialiased selection:bg-indigo-500 selection:text-white">
        
        <!-- TOP BRANDING NAVIGATION HEADER BANNER -->
        <header class="border-b border-slate-800 bg-slate-900/80 backdrop-blur-md sticky top-0 z-50">
            <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
                <div class="flex items-center space-x-3">
                    <span class="text-2xl">☸️</span>
                    <h1 class="text-lg font-bold tracking-tight text-white">Saga Control Panel Hub</h1>
                </div>
                <div class="flex items-center space-x-2">
                    <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                        <span class="w-1.5 h-1.5 mr-1.5 rounded-full bg-emerald-400 animate-pulse"></span>
                        Cluster Mesh Online
                    </span>
                </div>
            </div>
        </header>

        <!-- TWO-COLUMN RESPONSIVE LAYOUT GRID SPACE -->
        <main class="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-8 grid grid-cols-1 lg:grid-cols-12 gap-8">
            
            <!-- COLUMN 1: FORM ROADWAY FOR FRESH ORDER ENTRY SUBMISSIONS (4-SPANS) -->
            <section class="lg:col-span-5 bg-slate-950/40 border border-slate-800/80 rounded-2xl p-6 h-fit backdrop-blur-sm">
                <div class="mb-6">
                    <h2 class="text-xl font-bold text-white flex items-center space-x-2">
                        <span>🛒</span> <span>New Sale Dispatcher</span>
                    </h2>
                    <p class="text-sm text-slate-400 mt-1">Submit checkout payloads straight to the FastAPI Gateway pipeline [1.1].</p>
                </div>

                <form id="orderForm" class="space-y-4">
                    <div>
                        <label class="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-1.5">Full Name</label>
                        <input type="text" id="custName" required placeholder="Bob Vance" class="w-full bg-slate-900/60 border border-slate-800 rounded-xl px-4 py-3 text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-colors">
                    </div>
                    <div>
                        <label class="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-1.5">Email Address</label>
                        <input type="email" id="custEmail" required placeholder="bob@vanceair.com" class="w-full bg-slate-900/60 border border-slate-800 rounded-xl px-4 py-3 text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-colors">
                    </div>
                    <div class="grid grid-cols-3 gap-3">
                        <div class="col-span-2">
                            <label class="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-1.5">Order Amount ($)</label>
                            <input type="number" id="orderAmount" required min="0.01" step="0.01" placeholder="250.00" class="w-full bg-slate-900/60 border border-slate-800 rounded-xl px-4 py-3 text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-colors font-mono">
                        </div>
                        <div>
                            <label class="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-1.5">US State</label>
                            <input type="text" id="shipState" required maxlength="2" placeholder="OH" class="w-full bg-slate-900/60 border border-slate-800 rounded-xl px-4 py-3 text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-colors font-mono uppercase text-center">
                        </div>
                    </div>
                    <div>
                        <label class="block text-xs font-semibold text-slate-400 uppercase tracking-wider mb-1.5">Item Stock SKU ID</label>
                        <input type="text" id="itemSku" required placeholder="SHIRT_ULTRA_LUXURY" class="w-full bg-slate-900/60 border border-slate-800 rounded-xl px-4 py-3 text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-colors font-mono">
                    </div>
                    <button type="submit" class="w-full mt-2 bg-indigo-600 hover:bg-indigo-500 active:bg-indigo-700 text-white font-semibold py-3.5 px-4 rounded-xl shadow-lg shadow-indigo-600/10 transition-colors flex items-center justify-center space-x-2">
                        <span>🚀</span> <span>Dispatch Order Payload</span>
                    </button>
                </form>
            </section>

            <!-- COLUMN 2: REAL-TIME OPERATOR FRAUD HOLDS REVIEW TERMINAL (7-SPANS) -->
            <section class="lg:col-span-7 flex flex-col bg-slate-950/40 border border-slate-800/80 rounded-2xl p-6 backdrop-blur-sm">
                <div class="mb-6 flex items-center justify-between">
                    <div>
                        <h2 class="text-xl font-bold text-white flex items-center space-x-2">
                            <span>⏸️</span> <span>Fraud Detection Hold Desk</span>
                        </h2>
                        <p class="text-sm text-slate-400 mt-1">Live threads currently frozen inside the LangGraph checkpoint layer [1.1].</p>
                    </div>
                    <button id="refreshBtn" class="bg-slate-900 hover:bg-slate-800 border border-slate-800 text-slate-300 font-semibold py-2 px-4 rounded-xl text-sm transition-colors flex items-center space-x-1.5">
                        <span>🔄</span> <span>Sync</span>
                    </button>
                </div>

                <!-- DYNAMIC LIST CONTAINER SPACE -->
                <div id="reviewTerminal" class="flex-1 space-y-4 overflow-y-auto max-h-[34rem] pr-1">
                    <div class="text-center py-12 text-slate-500 border border-dashed border-slate-800 rounded-xl">
                        Scanning partition state memory cache layers...
                    </div>
                </div>
            </section>
        </main>

        <!-- GLOBAL TOACTION NOTIFICATION FEED OVERLAY FOOTER -->
        <div id="toastFeed" class="fixed bottom-5 right-5 z-50 space-y-2 pointer-events-none max-w-sm w-full"></div>

        <!-- RUNTIME CLIENT WEB ENGINE SCRIPT LOGIC -->
        <script>
            // DISPATCH PAYLOAD AGENT: Handles forward order entries via the local sales endpoint route
            document.getElementById('orderForm').addEventListener('submit', async (e) => {
                e.preventDefault();
                const payload = {
                    item_id: document.getElementById('itemSku').value,
                    amount: parseFloat(document.getElementById('orderAmount').value),
                    customer: {
                        name: document.getElementById('custName').value,
                        email: document.getElementById('custEmail').value
                    },
                    shipping_address: {
                        street: "123 Operator Entry Way",
                        city: "Gateway Hub",
                        state: document.getElementById('shipState').value.toUpperCase(),
                        postal_code: "43210"
                    }
                };

                try {
                    const res = await fetch('/sales/', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(payload)
                    });
                    const data = await res.json();
                    if(res.ok) {
                        spawnToast(`Order submitted safely! ID: ${data.order_id.slice(0,8)}...`, 'success');
                        document.getElementById('orderForm').reset();
                        setTimeout(syncReviewTerminal, 800);
                    } else {
                        spawnToast(`Gateway Rejected Pass: ${data.detail || 'Error'}`, 'error');
                    }
                } catch (err) {
                    spawnToast(`Network pipeline connection drop: ${err.message}`, 'error');
                }
            });

            // RESOLUTION ACTION OVERRIDE AGENT: Forwards approval/rejection verdict commands straight to outbox logs [1.1]
            async function postOperatorOverride(orderId, decision) {
                try {
                    const res = await fetch('/sales/override', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ order_id: orderId, verdict: decision })
                    });
                    if(res.ok) {
                        spawnToast(`Verdict [${decision}] staged for thread override pass!`, 'success');
                        setTimeout(syncReviewTerminal, 600);
                    } else {
                        spawnToast('Failed to write override verdict envelope contract.', 'error');
                    }
                } catch (err) {
                    spawnToast(`Failed to register override endpoint socket: ${err.message}`, 'error');
                }
            }

            // DATAPLANE SYNCHRONIZATION CONTROLLER: Pulls active data rows straight from the FastAPI database cache API [1.1]
            async function syncReviewTerminal() {
                const terminal = document.getElementById('reviewTerminal');
                try {
                    const res = await fetch('/api/ui/pending-reviews');
                    const cards = await res.json();
                    
                    if(!cards || cards.length === 0) {
                        terminal.innerHTML = `
                            <div class="text-center py-16 border border-dashed border-slate-800 rounded-xl bg-slate-950/20">
                                <span class="text-3xl block mb-2">🛡️</span>
                                <span class="text-slate-400 font-semibold text-sm">Pristine Shard Memory Space</span>
                                <p class="text-xs text-slate-500 mt-1">No transaction locks currently detected inside the control namespace.</p>
                            </div>
                        `;
                        return;
                    }

                    terminal.innerHTML = cards.map(row => `
                        <div class="bg-slate-900/50 border border-slate-800 rounded-xl p-5 hover:border-slate-700 transition-all flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
                            <div class="space-y-1">
                                <div class="flex items-center space-x-2">
                                    <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-mono font-medium bg-amber-500/10 text-amber-400 border border-amber-500/20">HOLD</span>
                                    <span class="font-mono text-xs text-slate-400 font-semibold">UUID: ${row.order_id}</span>
                                </div>
                                <div class="text-sm font-bold text-white">${row.customer_name} (<span class="text-slate-400 font-normal">${row.customer_email}</span>)</div>
                                <p class="text-xs text-slate-400">Target Line: <span class="font-mono text-indigo-400 font-semibold">${row.item_id}</span> | Value Threshold: <span class="font-mono font-bold text-emerald-400">$${row.amount.toFixed(2)}</span></p>
                            </div>
                            <div class="flex sm:flex-col lg:flex-row gap-2 shrink-0">
                                <button onclick="postOperatorOverride('${row.order_id}', 'APPROVE')" class="flex-1 bg-emerald-600/10 hover:bg-emerald-600/20 active:bg-emerald-600/30 text-emerald-400 border border-emerald-500/20 font-semibold px-4 py-2 rounded-lg text-xs transition-colors tracking-wide uppercase">Approve</button>
                                <button onclick="postOperatorOverride('${row.order_id}', 'REJECT')" class="flex-1 bg-rose-600/10 hover:bg-rose-600/20 active:bg-rose-600/30 text-rose-400 border border-rose-500/20 font-semibold px-4 py-2 rounded-lg text-xs transition-colors tracking-wide uppercase">Let Order Die</button>
                            </div>
                        </div>
                    `).join('');

                } catch (err) {
                    terminal.innerHTML = `<div class="text-rose-400 text-xs py-4">Failed to rehydrate live thread registries: ${err.message}</div>`;
                }
            }

            // UI TOAST FEED NOTIFIER FACTORY
            function spawnToast(message, type) {
                const feed = document.getElementById('toastFeed');
                const toast = document.createElement('div');
                toast.className = `p-4 rounded-xl text-xs font-semibold shadow-2xl border transition-all duration-300 opacity-0 transform translate-y-2 pointer-events-auto flex items-center space-x-2 ${
                    type === 'success' ? 'bg-emerald-950/80 border-emerald-500/30 text-emerald-400' : 'bg-rose-950/80 border-rose-500/30 text-rose-400'
                }`;
                toast.innerHTML = `<span>${type === 'success' ? '✔' : '❌'}</span> <span>${message}</span>`;
                feed.appendChild(toast);
                
                setTimeout(() => { toast.classList.remove('opacity-0', 'translate-y-2'); }, 10);
                setTimeout(() => {
                    toast.classList.add('opacity-0', 'translate-y-2');
                    setTimeout(() => { toast.remove(); }, 300);
                }, 4000);
            }

            document.getElementById('refreshBtn').addEventListener('click', syncReviewTerminal);
            setInterval(syncReviewTerminal, 4000);
            window.addEventListener('DOMContentLoaded', syncReviewTerminal);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content, status_code=200)
