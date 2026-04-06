// ============================================================
// recovery.js — Tab Recovery Center
// Memanggil /api/v2/recovery
// ============================================================

let _recovPage = 1;
const _recovPageSize = 25;

async function muatRecovery() {
    const type = (document.getElementById('recovery-filter-type') || {}).value || '';
    const status = (document.getElementById('recovery-filter-status') || {}).value || '';
    setLoading('list-recovery', 'Memuat recovery items...');

    // Summary
    try {
        const sum = await _getV2('/recovery/summary');
        const grid = document.getElementById('recovery-summary-grid');
        if (grid) {
            grid.innerHTML = [
                ['Total', sum.total || 0, ''],
                ['Recoverable', sum.recoverable || 0, 'warn'],
                ['Recovered', sum.recovered || 0, 'green'],
                ['Partial', sum.partial || 0, 'info'],
                ['Ignored', sum.ignored || 0, ''],
            ].map(([l, v, c]) => `<div class="ringkasan-item ${c}"><div class="ringkasan-angka">${v}</div><div class="ringkasan-label">${l}</div></div>`).join('');
        }
    } catch {}

    try {
        const params = new URLSearchParams({ page: _recovPage, page_size: _recovPageSize });
        if (type) params.set('entity_type', type);
        if (status) params.set('status', status);

        const data = await _getV2(`/recovery/items?${params}`);
        const items = data.items || [];
        const total = (data._meta || {}).total || 0;

        if (!items.length) {
            document.getElementById('list-recovery').innerHTML =
                `<div class="empty-state"><div class="icon">🛠️</div><p>Tidak ada item recovery. Sistem berjalan normal.</p></div>`;
            document.getElementById('recovery-pagination').innerHTML = '';
            return;
        }

        document.getElementById('list-recovery').innerHTML = `
            <table class="tabel-data">
                <thead><tr>
                    <th>ID</th><th>Tipe Entitas</th><th>Entity ID</th>
                    <th>Status</th><th>Severity</th><th>Masalah</th><th>Aksi</th>
                </tr></thead>
                <tbody>
                ${items.map(item => `
                <tr>
                    <td>${item.id}</td>
                    <td><span class="badge-tipe">${item.entity_type}</span></td>
                    <td>${item.entity_id}</td>
                    <td>${_badgeRecovery(item.recovery_status)}</td>
                    <td>${_badgeSeverity(item.severity)}</td>
                    <td style="font-size:12px;max-width:200px;word-break:break-word">${item.issue_summary || item.note || '-'}</td>
                    <td>
                        <div style="display:flex;gap:4px;flex-wrap:wrap">
                            ${item.recovery_status === 'recoverable' ? `<button class="btn-success btn-xs" onclick="recoverItem(${item.id})">✅ Recover</button>` : ''}
                            <button class="btn-outline btn-xs" onclick="requeueItem(${item.id})">🔁 Requeue</button>
                            <button class="btn-outline btn-xs" onclick="partialItem(${item.id})">🟡 Partial</button>
                            <button class="btn-danger btn-xs" onclick="ignoreItem(${item.id})">🔇 Ignore</button>
                        </div>
                    </td>
                </tr>`).join('')}
                </tbody>
            </table>`;

        const totalPages = Math.ceil(total / _recovPageSize);
        document.getElementById('recovery-pagination').innerHTML = totalPages > 1
            ? `<button class="btn-outline btn-sm" ${_recovPage <= 1 ? 'disabled' : ''} onclick="_recovPage--;muatRecovery()">← Prev</button>
               <span style="font-size:13px">Hal ${_recovPage} / ${totalPages} (${total} total)</span>
               <button class="btn-outline btn-sm" ${_recovPage >= totalPages ? 'disabled' : ''} onclick="_recovPage++;muatRecovery()">Next →</button>`
            : `<span style="font-size:12px;color:#888">${total} item</span>`;
    } catch (e) {
        document.getElementById('list-recovery').innerHTML =
            `<div class="empty-state"><div class="icon">⚠️</div><p>Gagal muat: ${e.message}</p></div>`;
    }
}

async function jalankanScanRecovery() {
    try {
        await _postV2('/recovery/scan', {});
        alert('✅ Recovery scan dijalankan. Refresh untuk melihat hasil.');
        muatRecovery();
    } catch (e) { alert('Gagal: ' + e.message); }
}

async function recoverAllSafe() {
    if (!confirm('Recover semua item yang berstatus "recoverable"?')) return;
    try {
        const r = await _postV2('/recovery/recover-all-safe', {});
        alert(`✅ ${r.recovered_count || 0} item dipulihkan.`);
        muatRecovery();
    } catch (e) { alert('Gagal: ' + e.message); }
}

async function recoverItem(id) {
    try { await _postV2(`/recovery/items/${id}/recover`, {}); muatRecovery(); }
    catch (e) { alert('Gagal: ' + e.message); }
}

async function requeueItem(id) {
    try { await _postV2(`/recovery/items/${id}/requeue`, {}); muatRecovery(); }
    catch (e) { alert('Gagal: ' + e.message); }
}

async function partialItem(id) {
    const reason = prompt('Catatan recovery partial:');
    if (reason === null) return;
    try { await _postV2(`/recovery/items/${id}/mark-partial`, { reason }); muatRecovery(); }
    catch (e) { alert('Gagal: ' + e.message); }
}

async function ignoreItem(id) {
    const reason = prompt('Alasan diabaikan:');
    if (reason === null) return;
    try { await _postV2(`/recovery/items/${id}/ignore`, { reason }); muatRecovery(); }
    catch (e) { alert('Gagal: ' + e.message); }
}

// ── Helper badge recovery ─────────────────────────────────
function _badgeRecovery(status) {
    const map = {
        recoverable: 'badge-warn', recovered: 'badge-aktif', partial: 'badge-info',
        ignored: '', abandoned: 'badge-gagal',
    };
    return `<span class="${map[status] || 'badge-info'}">${status || '-'}</span>`;
}

function _badgeSeverity(severity) {
    const map = { critical: 'badge-gagal', high: 'badge-warn', medium: 'badge-info', low: '' };
    return severity ? `<span class="${map[severity] || 'badge-info'}">${severity}</span>` : '-';
}
