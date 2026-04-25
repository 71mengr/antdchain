// ANTDChain Explorer – Frontend Application
const API = {
    chainStatus: '/api/chain/status',
    chainStats: '/api/chain/stats',
    blocks: '/api/blocks',
    block: (h) => `/api/blocks/${h}`,
    transactions: '/api/transactions',
    transaction: (h) => `/api/transactions/${h}`,
    mempool: '/api/mempool',
    address: (a) => `/api/address/${a}`,
    validators: '/api/validators',
    rotatingKing: '/api/rotatingking',
    search: '/api/search'
};

let currentPage = '';
let searchTimeout;

// ---- Router ----
function router() {
    const path = window.location.pathname;
    const app = document.getElementById('app');
    app.innerHTML = '<div class="spinner-container"><div class="spinner"></div><p>Loading...</p></div>';

    if (path === '/') return loadDashboard();
    if (path === '/blocks' || path.startsWith('/blocks?')) return loadBlocks();
    if (path.startsWith('/block/')) return loadBlockDetail(path.split('/block/')[1]);
    if (path.startsWith('/tx/')) return loadTxDetail(path.split('/tx/')[1]);
    if (path.startsWith('/address/')) return loadAddress(decodeURIComponent(path.split('/address/')[1]));
    if (path === '/mempool') return loadMempool();
    if (path === '/validators') return loadValidators();
    loadDashboard();
}

// ---- Helpers ----
async function fetchJSON(url) {
    try {
        const r = await fetch(url);
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return await r.json();
    } catch (e) {
        showToast(`Error: ${e.message}`);
        return null;
    }
}

function showToast(msg) {
    const t = document.getElementById('toast');
    t.textContent = msg; t.classList.remove('hidden');
    setTimeout(() => t.classList.add('hidden'), 3000);
}

function formatTime(ts) {
    const d = new Date(ts * 1000);
    return d.toLocaleString();
}

function formatANTD(wei) {
    if (!wei || wei === '0') return '0';
    const antd = BigInt(wei) / 10n**18n;
    const frac = BigInt(wei) % 10n**18n;
    if (frac === 0n) return antd.toString();
    let s = frac.toString().padStart(18, '0').replace(/0+$/, '');
    return `${antd}.${s}`;
}

function truncate(s, len = 12) {
    if (!s) return '';
    return s.length <= len * 2 + 2 ? s : `${s.slice(0, len)}...${s.slice(-len)}`;
}

async function updateNavStatus() {
    const data = await fetchJSON(API.chainStatus);
    if (!data) return;
    document.getElementById('chainHeight').textContent = `Height: ${data.height.toLocaleString()}`;
    document.getElementById('peerCount').textContent = `Peers: ${data.peers || 0}`;
    document.getElementById('footerHeight').textContent = `Block Height: ${data.height.toLocaleString()}`;
    const dot = document.getElementById('syncStatus');
    if (data.syncing) { dot.classList.add('syncing'); dot.title = 'Syncing...'; }
    else { dot.classList.remove('syncing'); dot.title = 'Synced'; }
}

// ---- Search ----
async function doSearch() {
    const q = document.getElementById('searchInput').value.trim();
    if (!q) return;
    const data = await fetchJSON(`${API.search}?q=${encodeURIComponent(q)}`);
    if (!data || data.type === 'none' || data.type === 'not_found') {
        showToast('No results found');
        return;
    }
    if (data.type === 'block') {
        window.location.href = `/block/${data.data.height}`;
    } else if (data.type === 'transaction') {
        window.location.href = `/tx/${data.data.hash}`;
    } else if (data.type === 'address') {
        window.location.href = `/address/${data.data.address}`;
    }
}

async function liveSearch() {
    const q = document.getElementById('searchInput').value.trim();
    const dd = document.getElementById('searchResults');
    if (q.length < 3) { dd.classList.add('hidden'); return; }
    const data = await fetchJSON(`${API.search}?q=${encodeURIComponent(q)}`);
    if (!data || data.type === 'none' || data.type === 'not_found') {
        dd.innerHTML = '<div class="search-dropdown-item">No results</div>';
        dd.classList.remove('hidden');
        return;
    }
    let html = '';
    if (data.type === 'block') html = `<a href="/block/${data.data.height}">�� Block #${data.data.height} – ${truncate(data.data.hash)}</a>`;
    else if (data.type === 'transaction') html = `<a href="/tx/${data.data.hash}">�� TX ${truncate(data.data.hash)}</a>`;
    else if (data.type === 'address') html = `<a href="/address/${data.data.address}">�� ${truncate(data.data.address, 14)}</a>`;
    dd.innerHTML = html; dd.classList.remove('hidden');
}

// ---- Dashboard ----
async function loadDashboard() {
    const [status, stats, blocks] = await Promise.all([
        fetchJSON(API.chainStatus),
        fetchJSON(API.chainStats),
        fetchJSON(`${API.blocks}?limit=8`)
    ]);
    if (!status) return;

    const blkRows = (blocks && blocks.blocks) ? blocks.blocks.map(b => `
        <tr>
            <td class="mono"><a href="/block/${b.height}">${b.height.toLocaleString()}</a></td>
            <td class="mono hash"><a href="/block/${b.height}">${truncate(b.hash)}</a></td>
            <td><span class="badge badge-success">${b.txCount}</span></td>
            <td class="addr">${truncate(b.miner, 8)}</td>
            <td class="mono">${b.gasUsed.toLocaleString()}</td>
            <td>${formatTime(b.timestamp)}</td>
        </tr>`).join('') : '';

    document.getElementById('app').innerHTML = `
        <div class="stats-grid">
            <div class="stat-card"><div class="stat-value">${(status.height || 0).toLocaleString()}</div><div class="stat-label">Block Height</div></div>
            <div class="stat-card"><div class="stat-value">${(stats && stats.totalTransactions || 0).toLocaleString()}</div><div class="stat-label">Transactions</div></div>
            <div class="stat-card"><div class="stat-value">${status.mempoolSize || 0}</div><div class="stat-label">Mempool</div></div>
            <div class="stat-card"><div class="stat-value">${status.peers || 0}</div><div class="stat-label">Peers</div></div>
            <div class="stat-card"><div class="stat-value">${(stats && stats.activeValidators || 0)}</div><div class="stat-label">Active Stakers</div></div>
            <div class="stat-card"><div class="stat-value">${(stats && stats.avgBlockTime || 0).toFixed(1)}s</div><div class="stat-label">Avg Block Time</div></div>
        </div>
        <div class="card">
            <div class="card-header"><h2>�� Latest Blocks</h2><a href="/blocks" class="btn">View All →</a></div>
            <div class="table-container">
                <table>
                    <thead><tr><th>Height</th><th>Hash</th><th>Txs</th><th>Miner</th><th>Gas Used</th><th>Time</th></tr></thead>
                    <tbody>${blkRows}</tbody>
                </table>
            </div>
        </div>`;
}

// ---- Blocks Page ----
async function loadBlocks() {
    const params = new URLSearchParams(window.location.search);
    const offset = parseInt(params.get('offset') || '0');
    const limit = 20;
    const data = await fetchJSON(`${API.blocks}?limit=${limit}&offset=${offset}`);
    if (!data) return;

    const total = data.total || 0;
    const rows = (data.blocks || []).map(b => `
        <tr>
            <td class="mono"><a href="/block/${b.height}">${b.height.toLocaleString()}</a></td>
            <td class="mono hash"><a href="/block/${b.height}">${truncate(b.hash)}</a></td>
            <td><span class="badge badge-success">${b.txCount}</span></td>
            <td class="addr">${truncate(b.miner, 8)}</td>
            <td>${formatTime(b.timestamp)}</td>
        </tr>`).join('');

    const prevOffset = Math.max(0, offset - limit);
    const nextOffset = offset + limit;
    const hasPrev = offset > 0;
    const hasNext = (offset + limit) < total;

    document.getElementById('app').innerHTML = `
        <div class="card">
            <div class="card-header"><h2>�� Blocks</h2><span class="page-info">${total.toLocaleString()} total</span></div>
            <div class="table-container">
                <table>
                    <thead><tr><th>Height</th><th>Hash</th><th>Txs</th><th>Miner</th><th>Time</th></tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
            <div class="pagination">
                <button class="btn" onclick="navigate('/blocks?offset=${prevOffset}')" ${!hasPrev ? 'disabled' : ''}>← Previous</button>
                <span class="page-info">Blocks ${offset+1}–${Math.min(offset+limit, total)} of ${total.toLocaleString()}</span>
                <button class="btn" onclick="navigate('/blocks?offset=${nextOffset}')" ${!hasNext ? 'disabled' : ''}>Next →</button>
            </div>
        </div>`;
}

function navigate(url) { window.location.href = url; }

// ---- Block Detail ----
async function loadBlockDetail(identifier) {
    const data = await fetchJSON(API.block(identifier));
    if (!data) return;

    const txRows = (data.transactions || []).map(t => `
        <tr>
            <td class="mono hash"><a href="/tx/${t.hash}">${truncate(t.hash)}</a></td>
            <td class="addr mono">${truncate(t.from, 8)}</td>
            <td class="addr mono">${truncate(t.to || '', 8)}</td>
            <td class="mono">${formatANTD(t.value)} ANTD</td>
            <td class="mono">${t.gas.toLocaleString()}</td>
        </tr>`).join('');

    document.getElementById('app').innerHTML = `
        <div class="card">
            <div class="card-header"><h2>�� Block #${data.height.toLocaleString()}</h2></div>
            <div class="detail-grid">
                <div class="detail-label">Hash</div><div class="detail-value">${data.hash}</div>
                <div class="detail-label">Parent Hash</div><div class="detail-value"><a href="/block/${data.height-1}">${data.parentHash}</a></div>
                <div class="detail-label">Miner</div><div class="detail-value"><a href="/address/${data.miner}">${data.miner}</a></div>
                <div class="detail-label">Timestamp</div><div class="detail-value">${formatTime(data.timestamp)}</div>
                <div class="detail-label">Difficulty</div><div class="detail-value">${data.difficulty}</div>
                <div class="detail-label">Gas Limit</div><div class="detail-value">${(data.gasLimit || 0).toLocaleString()}</div>
                <div class="detail-label">Gas Used</div><div class="detail-value">${(data.gasUsed || 0).toLocaleString()}</div>
                <div class="detail-label">State Root</div><div class="detail-value">${data.stateRoot || ''}</div>
                <div class="detail-label">Tx Root</div><div class="detail-value">${data.txRoot || ''}</div>
                <div class="detail-label">Transactions</div><div class="detail-value">${(data.transactions || []).length}</div>
                <div class="detail-label">Size</div><div class="detail-value">${(data.size || 0).toLocaleString()} bytes</div>
            </div>
        </div>
        ${txRows ? `<div class="card">
            <div class="card-header"><h2>�� Transactions (${(data.transactions || []).length})</h2></div>
            <div class="table-container">
                <table>
                    <thead><tr><th>Hash</th><th>From</th><th>To</th><th>Value</th><th>Gas</th></tr></thead>
                    <tbody>${txRows}</tbody>
                </table>
            </div>
        </div>` : ''}`;
}

// ---- Transaction Detail ----
async function loadTxDetail(hash) {
    const data = await fetchJSON(API.transaction(hash));
    if (!data) return;

    document.getElementById('app').innerHTML = `
        <div class="card">
            <div class="card-header"><h2>�� Transaction</h2></div>
            <div class="detail-grid">
                <div class="detail-label">Hash</div><div class="detail-value">${data.hash}</div>
                <div class="detail-label">Status</div><div class="detail-value"><span class="badge badge-success">Confirmed</span></div>
                <div class="detail-label">Block</div><div class="detail-value"><a href="/block/${data.blockHeight}">#${data.blockHeight.toLocaleString()}</a></div>
                <div class="detail-label">Timestamp</div><div class="detail-value">${formatTime(data.timestamp)}</div>
                <div class="detail-label">From</div><div class="detail-value"><a href="/address/${data.from}">${data.from}</a></div>
                <div class="detail-label">To</div><div class="detail-value"><a href="/address/${data.to}">${data.to || 'Contract Creation'}</a></div>
                <div class="detail-label">Value</div><div class="detail-value">${formatANTD(data.value)} ANTD</div>
                <div class="detail-label">Gas Limit</div><div class="detail-value">${(data.gas || 0).toLocaleString()}</div>
                <div class="detail-label">Gas Price</div><div class="detail-value">${data.gasPrice || '0'}</div>
                <div class="detail-label">Nonce</div><div class="detail-value">${data.nonce}</div>
                <div class="detail-label">Data</div><div class="detail-value">${data.data === '00' ? 'None' : data.data}</div>
            </div>
        </div>`;
}

// ---- Address ----
async function loadAddress(address) {
    const data = await fetchJSON(API.address(address));
    if (!data) return;

    const txRows = (data.transactions || []).map(t => `
        <tr>
            <td class="mono hash"><a href="/tx/${t.hash}">${truncate(t.hash)}</a></td>
            <td><span class="badge badge-${t.direction}">${t.direction.toUpperCase()}</span></td>
            <td class="addr mono">${truncate(t.direction === 'in' ? t.from : t.to, 8)}</td>
            <td class="mono">${formatANTD(t.value)} ANTD</td>
            <td><a href="/block/${t.blockHeight}">#${t.blockHeight.toLocaleString()}</a></td>
        </tr>`).join('');

    document.getElementById('app').innerHTML = `
        <div class="card">
            <div class="card-header"><h2>�� Address</h2></div>
            <div class="detail-grid">
                <div class="detail-label">Address</div><div class="detail-value">${data.address}</div>
                <div class="detail-label">Balance</div><div class="detail-value">${formatANTD(data.balance)} ANTD</div>
                <div class="detail-label">Nonce</div><div class="detail-value">${data.nonce}</div>
                <div class="detail-label">Transactions</div><div class="detail-value">${data.txCount}</div>
            </div>
        </div>
        ${txRows ? `<div class="card">
            <div class="card-header"><h2>�� Recent Transactions</h2></div>
            <div class="table-container">
                <table>
                    <thead><tr><th>Hash</th><th>Direction</th><th>Counterparty</th><th>Value</th><th>Block</th></tr></thead>
                    <tbody>${txRows}</tbody>
                </table>
            </div>
        </div>` : ''}`;
}

// ---- Mempool ----
async function loadMempool() {
    const data = await fetchJSON(API.mempool);
    const rows = (data && data.transactions) ? data.transactions.map(t => `
        <tr>
            <td class="mono hash">${truncate(t.hash)}</td>
            <td class="addr mono">${truncate(t.from, 8)}</td>
            <td class="addr mono">${truncate(t.to || '', 8)}</td>
            <td class="mono">${formatANTD(t.value)} ANTD</td>
            <td class="mono">${t.nonce}</td>
            <td><span class="badge badge-pending">Pending</span></td>
        </tr>`).join('') : '';

    document.getElementById('app').innerHTML = `
        <div class="card">
            <div class="card-header"><h2>�� Mempool</h2><span class="page-info">${(data && data.count) || 0} pending transactions</span></div>
            <div class="table-container">
                <table>
                    <thead><tr><th>Hash</th><th>From</th><th>To</th><th>Value</th><th>Nonce</th><th>Status</th></tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
            ${!rows ? '<p style="text-align:center;padding:20px;color:var(--text-secondary)">No pending transactions</p>' : ''}
        </div>`;
}

// ---- Validators + Rotating King ----
async function loadValidators() {
    const [valData, rkData] = await Promise.all([
        fetchJSON(API.validators),
        fetchJSON(API.rotatingKing)
    ]);

    let html = '';
    if (!valData) return;

    const summary = (valData.validators && valData.validators[0]) ? valData.validators[0] : {};
    const kings = (valData.validators || []).slice(1).map(v => `
        <tr>
            <td class="addr mono">${truncate(v.address, 10)}</td>
            <td class="mono">${formatANTD(v.balance)} ANTD</td>
            <td><span class="badge ${v.isKing ? 'badge-success' : 'badge-pending'}">${v.isKing ? 'Active' : 'Inactive'}</span></td>
        </tr>`).join('');

    let rkHtml = '';
    if (rkData) {
        rkHtml = `
        <div class="card" style="margin-top:20px;">
            <div class="card-header"><h2>�� Rotating King</h2></div>
            <div class="detail-grid">
                <div class="detail-label">Current King</div><div class="detail-value"><a href="/address/${rkData.currentKing}">${rkData.currentKing}</a></div>
                <div class="detail-label">Next King</div><div class="detail-value"><a href="/address/${rkData.nextKing}">${rkData.nextKing}</a></div>
                <div class="detail-label">King Count</div><div class="detail-value">${rkData.kingCount}</div>
                <div class="detail-label">Blocks Until Rotation</div><div class="detail-value">${rkData.blocksUntilRotation.toLocaleString()}</div>
                <div class="detail-label">Rotation Height</div><div class="detail-value">${rkData.rotationHeight}</div>
                <div class="detail-label">Rotation Interval</div><div class="detail-value">${rkData.rotationInterval}</div>
            </div>
        </div>`;
    }

    document.getElementById('app').innerHTML = `
        <div class="stats-grid">
            <div class="stat-card"><div class="stat-value">${summary.activeStakers || 0}</div><div class="stat-label">Active Stakers</div></div>
            <div class="stat-card"><div class="stat-value">${summary.totalStakers || 0}</div><div class="stat-label">Total Stakers</div></div>
            <div class="stat-card"><div class="stat-value">${summary.totalStakedANTD || '0'}</div><div class="stat-label">Total Staked (ANTD)</div></div>
            <div class="stat-card"><div class="stat-value">${summary.rotations || 0}</div><div class="stat-label">Rotations</div></div>
            <div class="stat-card"><div class="stat-value">${summary.difficulty || '0'}</div><div class="stat-label">Difficulty</div></div>
            <div class="stat-card"><div class="stat-value">${Number(summary.avgBlockTime || 0).toFixed(1)}s</div><div class="stat-label">Avg Block Time</div></div>
        </div>
        <div class="card">
            <div class="card-header"><h2>�� Validators (Kings)</h2></div>
            <div class="table-container">
                <table>
                    <thead><tr><th>Address</th><th>Balance</th><th>Status</th></tr></thead>
                    <tbody>${kings}</tbody>
                </table>
            </div>
        </div>
        ${rkHtml}`;
}

// ---- Init ----
document.addEventListener('DOMContentLoaded', () => {
    router();
    updateNavStatus();
    setInterval(updateNavStatus, 15000);

    document.getElementById('searchBtn').addEventListener('click', doSearch);
    document.getElementById('searchInput').addEventListener('keydown', (e) => {
        if (e.key === 'Enter') doSearch();
    });
    document.getElementById('searchInput').addEventListener('input', () => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(liveSearch, 400);
    });
    document.addEventListener('click', (e) => {
        if (!e.target.closest('.nav-search')) {
            document.getElementById('searchResults').classList.add('hidden');
        }
    });
});

window.addEventListener('popstate', router);
