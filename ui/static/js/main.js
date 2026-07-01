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
    // Optimistically fade out and freeze user actions on click to avoid graph race states
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
            cards.forEach(card => {
                if (card.innerHTML.includes(orderId)) card.remove();
            });
            setTimeout(syncReviewTerminal, 1200);
        } else {
            spawnToast('Failed to write override verdict envelope contract.', 'error');
            syncReviewTerminal(); // Rehydrate original UI if server fails
        }
    } catch (err) {
        spawnToast(`Failed to register override endpoint socket: ${err.message}`, 'error');
        syncReviewTerminal();
    }
}

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
                    <p class="text-xs text-slate-500 mt-1">No transactions locked in local finance shards.</p>
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
