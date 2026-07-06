// =========================================================================
// 🌐 UNIFIED ADMINISTRATIVE SAGA DASHBOARD OPERATIONS LAYER
// =========================================================================
let activeFilterOrderIds = null; 

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

async function postOperatorOverride(orderId, decision) {
    const cards = document.querySelectorAll('#reviewTerminal > div');
    cards.forEach(card => {
        if (card.innerHTML.includes(orderId)) {
            card.style.opacity = '0.4';
            card.style.pointerEvents = 'none';
        }
    });
    try {
        const res = await fetch('/sales/override', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ order_id: orderId, verdict: decision })
        });
        if(res.ok) {
            spawnToast(`Verdict [${decision}] staged for thread override pass!`, 'success');
            if (activeFilterOrderIds) {
                activeFilterOrderIds = activeFilterOrderIds.filter(id => id !== orderId);
                if (activeFilterOrderIds.length === 0) activeFilterOrderIds = null;
            }
            setTimeout(syncReviewTerminal, 400);
        } else {
            spawnToast('Failed to write override verdict envelope contract.', 'error');
            syncReviewTerminal();
        }
    } catch (err) {
        spawnToast(`Failed to register override endpoint socket: ${err.message}`, 'error');
        syncReviewTerminal();
    }
}
function clearActiveFilter() {
    activeFilterOrderIds = null;
    document.getElementById('agentOutputTerminal').classList.add('hidden');
    syncReviewTerminal();
    spawnToast("Operations terminal filter cleared.", "success");
}

async function syncReviewTerminal() {
    const terminal = document.getElementById('reviewTerminal');
    const syncBtn = document.getElementById('refreshBtn');
    try {
        const res = await fetch('/api/ui/pending-reviews');
        let cards = await res.json();
        if(!cards || cards.length === 0) {
            terminal.innerHTML = `
                <div class="text-center py-16 border border-dashed border-slate-800 rounded-xl bg-slate-950/20">
                    <span class="text-3xl block mb-2">🛡️</span>
                    <span class="text-slate-400 font-semibold text-sm">Pristine Shard Memory Space</span>
                    <p class="text-xs text-slate-500 mt-1">No transactions locked in local finance shards.</p>
                </div>
            `;
            syncBtn.innerHTML = `<span>🔄</span> <span>Sync</span>`;
            return;
        }
        if (activeFilterOrderIds !== null) {
            cards = cards.filter(row => activeFilterOrderIds.includes(row.order_id));
            syncBtn.innerHTML = `<span>🧹</span> <span onclick="clearActiveFilter(); event.stopPropagation();">Clear Filter</span>`;
            if (cards.length === 0) {
                terminal.innerHTML = `
                    <div class="text-center py-12 border border-dashed border-slate-800 rounded-xl bg-slate-950/20">
                        <span class="text-slate-400 font-semibold text-sm">No items matching active filter criteria.</span>
                        <p class="text-xs text-indigo-400 mt-1 cursor-pointer underline" onclick="clearActiveFilter()">Reset view to list all orders</p>
                    </div>
                `;
                return;
            }
        } else {
            syncBtn.innerHTML = `<span>🔄</span> <span>Sync</span>`;
        }
        terminal.innerHTML = cards.map((row, index) => {
            const addr = row.shipping_address || {};
            const street = addr.street || "No Street Provided";
            const city = addr.city || "Unknown City";
            const state = addr.state || row.state_code || "??";
            const zip = addr.postal_code || "";
            const addressString = `${street}, ${city}, ${state} ${zip}`.trim();
            const visualSeparator = index < cards.length - 1 
                ? `<div class="py-2"><hr class="border-t border-dashed border-slate-800/80 my-1"></div>` 
                : '';
            return `
                <div class="bg-slate-900/50 border border-slate-800 rounded-xl p-5 hover:border-slate-700 transition-all flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
                    <div class="space-y-1 flex-1">
                        <div class="flex items-center space-x-2">
                            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-mono font-medium bg-amber-500/10 text-amber-400 border border-amber-500/20">HOLD</span>
                            <span class="font-mono text-xs text-slate-400 font-semibold">UUID: ${row.order_id}</span>
                        </div>
                        <div class="text-sm font-bold text-white">${row.customer_name} (<span class="text-slate-400 font-normal">${row.customer_email}</span>)</div>
                        <div class="text-xs text-slate-400 flex items-center space-x-1.5 pt-0.5">
                            <span>📍</span>
                            <span class="text-slate-300 select-all bg-slate-950/40 px-2 py-0.5 rounded border border-slate-800/60">${addressString}</span>
                        </div>
                        <p class="text-xs text-slate-400 pt-1">Target Line: <span class="font-mono text-indigo-400 font-semibold">${row.item_id}</span> | Value Threshold: <span class="font-mono font-bold text-emerald-400">$${row.amount.toFixed(2)}</span></p>
                    </div>
                    <div class="flex sm:flex-col lg:flex-row gap-2 shrink-0">
                        <button onclick="postOperatorOverride('${row.order_id}', 'APPROVE')" class="flex-1 bg-emerald-600/10 hover:bg-emerald-600/20 active:bg-emerald-600/30 text-emerald-400 border border-emerald-500/20 font-semibold px-4 py-2 rounded-lg text-xs transition-colors tracking-wide uppercase">Approve</button>
                        <button onclick="postOperatorOverride('${row.order_id}', 'REJECT')" class="flex-1 bg-rose-600/10 hover:bg-rose-600/20 active:bg-rose-600/30 text-rose-400 border border-rose-500/20 font-semibold px-4 py-2 rounded-lg text-xs transition-colors tracking-wide uppercase">Let Order Die</button>
                    </div>
                </div>
                ${visualSeparator}
            `;
        }).join('');
    } catch (err) {
        terminal.innerHTML = `<div class="text-rose-400 text-xs py-4">Failed to rehydrate live thread registries: ${err.message}</div>`;
    }
}
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

// =========================================================================
// 🧠 CO-PILOT AGENT NLP EXECUTOR INTERFACE SUBMIT BLOCK
// =========================================================================
document.getElementById('agentForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const promptInput = document.getElementById('agentPrompt');
    const submitBtn = document.getElementById('agentSubmitBtn');
    const outputTerminal = document.getElementById('agentOutputTerminal');
    const userDirective = promptInput.value.trim();
    if (!userDirective) return;
    promptInput.disabled = true;
    submitBtn.disabled = true;
    outputTerminal.classList.remove('hidden');
    outputTerminal.innerHTML = `
        <div class="text-indigo-400 animate-pulse flex items-center space-x-2">
            <span>🧠</span> <span>Model running autonomous reasoning cycles over cluster shards...</span>
        </div>
    `;
    try {
        const res = await fetch('http://localhost:8005/api/agent/command', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ prompt: userDirective })
        });
        const data = await res.json();
        if (res.ok && data.status === 'COMPLETED') {
            outputTerminal.innerHTML = `
                <div class="space-y-2">
                    <div class="flex items-center justify-between text-[10px] text-slate-500 uppercase tracking-wider border-b border-slate-800 pb-1.5 font-semibold">
                        <span>📊 Execution Completed</span>
                        <span>Reasoning Steps: ${data.reasoning_steps_executed}</span>
                    </div>
                    <div class="text-white font-medium leading-relaxed whitespace-pre-wrap">${data.agent_summary}</div>
                    <div class="pt-1 flex justify-end">
                         <button onclick="clearActiveFilter()" class="bg-slate-900 hover:bg-slate-800 border border-slate-800 text-slate-400 hover:text-white px-2.5 py-1 rounded text-[10px] font-semibold transition-colors uppercase tracking-wider">Reset View</button>
                    </div>
                </div>
            `;
            const uuidPattern = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
            const matchedIds = data.agent_summary.match(uuidPattern);
            if (matchedIds && matchedIds.length > 0) {
                activeFilterOrderIds = matchedIds.map(id => id.toLowerCase());
                spawnToast(`Dashboard view filtered to ${activeFilterOrderIds.length} matched holds!`, "success");
            } else {
                activeFilterOrderIds = [];
                spawnToast("AI agent completed but found no specific transaction ids.", "success");
            }
            promptInput.value = '';
            syncReviewTerminal();
        } else {
            const detailMsg = data.detail || "Operational mesh boundary constraint breach.";
            outputTerminal.innerHTML = `
                <div class="text-rose-400 font-semibold flex items-center space-x-1.5">
                    <span>❌</span> <span>Execution Rejected: ${detailMsg}</span>
                </div>
            `;
            spawnToast("AI Copilot process loop failed to converge.", "error");
        }
    } catch (err) {
        outputTerminal.innerHTML = `
            <div class="text-rose-400 font-semibold flex items-center space-x-1.5">
                <span>❌</span> <span>Network Connection Failure: ${err.message}</span>
            </div>
        `;
        spawnToast("Failed to reach the AI agent gateway endpoint.", "error");
    } finally {
        promptInput.disabled = false;
        submitBtn.disabled = false;
    }
});
