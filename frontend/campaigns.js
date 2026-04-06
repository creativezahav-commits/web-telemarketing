// ============================================================
// campaigns.js — Tab Campaign Manager + Broadcast Queue
// Memanggil /api/v2/campaigns dan /api/v2/broadcast-queue
// ============================================================

let _campPage = 1;
const _campPageSize = 20;

async function muatCampaigns() {
    const search = (document.getElementById('campaigns-search') || {}).value || '';
    const status = (document.getElementById('campaigns-filter-status') || {}).value || '';
    setLoading('list-campaigns', 'Memuat campaigns...');

    // Summary
    try {
        const sum = await _getV2('/campaigns/summary');
        const grid = document.getElementById('campaigns-summary-grid');
        if (grid) {
            grid.innerHTML = [
                ['Total', sum.total || 0, ''],
                ['Running', sum.running || 0, 'green'],
                ['Queued', sum.queued || 0, 'info'],
                ['Paused', sum.paused || 0, 'warn'],
                ['Done', sum.done || 0, ''],
            ].map(([l, v, c]) => `<div class="ringkasan-item ${c}"><div class="ringkasan-angka">${v}</div><div class="ringkasan-label">${l}</div></div>`).join('');
        }
    } catch {}

    try {
        const params = new URLSearchParams({ page: _campPage, page_size: _campPageSize });
        if (search) params.set('search', search);
        if (status) params.set('status', status);

        const data = await _getV2(`/campaigns?${params}`);
        const items = data.items || [];
        const total = (data._meta || {}).total || 0;

        if (!items.length) {
            document.getElementById('list-campaigns').innerHTML =
                `<div class="empty-state"><div class="icon">📣</div><p>Belum ada campaign. Klik "Buat Campaign" atau "Auto-Create".</p></div>`;
            document.getElementById('campaigns-pagination').innerHTML = '';
        } else {
            document.getElementById('list-campaigns').innerHTML = `
                <table class="tabel-data">
                    <thead><tr>
                        <th>ID</th><th>Nama</th><th>Status</th><th>Pool</th>
                        <th>Target</th><th>Eligible</th><th>Aksi</th>
                    </tr></thead>
                    <tbody>
                    ${items.map(c => `
                    <tr>
                        <td>${c.id}</td>
                        <td><strong>${c.name}</strong></td>
                        <td>${_badgeCampaign(c.status)}</td>
                        <td><span class="badge-tipe">${c.sender_pool || 'default'}</span></td>
                        <td>${c.total_targets || 0}</td>
                        <td>${c.eligible_targets || 0}</td>
                        <td>
                            <div style="display:flex;gap:4px;flex-wrap:wrap">
                                ${c.status === 'draft' || c.status === 'queued' ? `<button class="btn-success btn-xs" onclick="startCampaign(${c.id})">▶️ Start</button>` : ''}
                                ${c.status === 'running' ? `<button class="btn-outline btn-xs" onclick="pauseCampaign(${c.id})">⏸️ Pause</button>` : ''}
                                ${c.status === 'paused' ? `<button class="btn-success btn-xs" onclick="resumeCampaign(${c.id})">▶️ Resume</button>` : ''}
                                ${c.status !== 'done' && c.status !== 'stopped' ? `<button class="btn-danger btn-xs" onclick="stopCampaign(${c.id})">⏹️ Stop</button>` : ''}
                                <button class="btn-outline btn-xs" onclick="lihatQueueCampaign(${c.id})">📦 Queue</button>
                                <button class="btn-outline btn-xs" onclick="duplikatCampaign(${c.id})">📋 Copy</button>
                            </div>
                        </td>
                    </tr>`).join('')}
                    </tbody>
                </table>`;

            const totalPages = Math.ceil(total / _campPageSize);
            document.getElementById('campaigns-pagination').innerHTML = totalPages > 1
                ? `<button class="btn-outline btn-sm" ${_campPage <= 1 ? 'disabled' : ''} onclick="_campPage--;muatCampaigns()">← Prev</button>
                   <span style="font-size:13px">Hal ${_campPage} / ${totalPages} (${total} total)</span>
                   <button class="btn-outline btn-sm" ${_campPage >= totalPages ? 'disabled' : ''} onclick="_campPage++;muatCampaigns()">Next →</button>`
                : `<span style="font-size:12px;color:#888">${total} campaign</span>`;
        }
    } catch (e) {
        document.getElementById('list-campaigns').innerHTML =
            `<div class="empty-state"><div class="icon">⚠️</div><p>Gagal muat: ${e.message}</p></div>`;
    }

    muatBroadcastQueue();
}

function tampilFormCampaign() {
    const el = document.getElementById('form-buat-campaign');
    if (el) el.style.display = el.style.display === 'none' ? 'block' : 'none';
}

async function simpanCampaign() {
    const name = document.getElementById('campaign-name').value.trim();
    if (!name) return tampilPesan('pesan-campaign', '❌ Nama campaign wajib diisi', 'gagal');
    tampilPesan('pesan-campaign', '⏳ Menyimpan...', 'info');
    try {
        const r = await _postV2('/campaigns', {
            name,
            sender_pool: document.getElementById('campaign-sender-pool').value,
            required_permission_status: document.getElementById('campaign-req-permission').value,
            required_group_status: document.getElementById('campaign-req-group').value,
            auto_start_enabled: document.getElementById('campaign-auto-start').checked,
        });
        tampilPesan('pesan-campaign', `✅ Campaign dibuat! ID: ${r.campaign_id}`, 'berhasil');
        document.getElementById('form-buat-campaign').style.display = 'none';
        _campPage = 1;
        muatCampaigns();
    } catch (e) {
        tampilPesan('pesan-campaign', `❌ Gagal: ${e.message}`, 'gagal');
    }
}

async function autoCreateCampaign() {
    const name = prompt('Nama campaign otomatis:', 'Auto Campaign ' + new Date().toLocaleDateString('id-ID'));
    if (!name) return;
    try {
        const r = await _postV2('/campaigns/auto-create', { name, auto_start_enabled: true });
        alert(`✅ Campaign otomatis dibuat!\nID: ${r.campaign_id}\nTarget: ${r.target_count} grup`);
        muatCampaigns();
    } catch (e) {
        alert('Gagal: ' + e.message);
    }
}

async function startCampaign(id) {
    try { await _postV2(`/campaigns/${id}/start`, {}); muatCampaigns(); }
    catch (e) { alert('Gagal: ' + e.message); }
}
async function pauseCampaign(id) {
    try { await _postV2(`/campaigns/${id}/pause`, {}); muatCampaigns(); }
    catch (e) { alert('Gagal: ' + e.message); }
}
async function resumeCampaign(id) {
    try { await _postV2(`/campaigns/${id}/resume`, {}); muatCampaigns(); }
    catch (e) { alert('Gagal: ' + e.message); }
}
async function stopCampaign(id) {
    if (!confirm('Hentikan campaign ini?')) return;
    try { await _postV2(`/campaigns/${id}/stop`, {}); muatCampaigns(); }
    catch (e) { alert('Gagal: ' + e.message); }
}
async function duplikatCampaign(id) {
    try {
        const r = await _postV2(`/campaigns/${id}/duplicate`, {});
        alert(`✅ Campaign diduplikasi. ID baru: ${r.campaign_id}`);
        muatCampaigns();
    } catch (e) { alert('Gagal: ' + e.message); }
}

// ── Broadcast Queue ───────────────────────────────────────
let _activeCampaignFilter = null;

async function lihatQueueCampaign(campaignId) {
    _activeCampaignFilter = campaignId;
    muatBroadcastQueue();
    document.getElementById('list-broadcast-queue').scrollIntoView({ behavior: 'smooth' });
}

async function muatBroadcastQueue() {
    try {
        const sum = await _getV2('/broadcast-queue/summary');
        const grid = document.getElementById('broadcast-queue-summary');
        if (grid) {
            grid.innerHTML = [
                ['Total', sum.total || 0, ''],
                ['Queued', sum.queued || 0, 'info'],
                ['Sent', sum.sent || 0, 'green'],
                ['Failed', sum.failed || 0, 'danger'],
                ['Blocked', sum.blocked || 0, 'warn'],
            ].map(([l, v, c]) => `<div class="ringkasan-item ${c}"><div class="ringkasan-angka">${v}</div><div class="ringkasan-label">${l}</div></div>`).join('');
        }
    } catch {}

    try {
        const throttle = await _getV2('/broadcast-queue/throttle-status');
        const el = document.getElementById('broadcast-throttle-info');
        if (el) {
            if (throttle.siap !== false && !throttle.next_allowed_at) {
                el.innerHTML = `<div class="next-join-info next-join-ready"><span class="next-join-icon">🟢</span><div class="next-join-detail"><span class="next-join-label">Status Throttle Broadcast</span><span class="next-join-value">Siap kirim kapan saja</span></div></div>`;
            } else if (throttle.siap) {
                el.innerHTML = `<div class="next-join-info next-join-ready"><span class="next-join-icon">🟢</span><div class="next-join-detail"><span class="next-join-label">Status Throttle Broadcast</span><span class="next-join-value">Siap kirim sekarang</span></div></div>`;
            } else {
                const sisa = throttle.sisa_detik || 0;
                const jam = Math.floor(sisa / 3600);
                const menit = Math.floor((sisa % 3600) / 60);
                const detik = sisa % 60;
                const sisaStr = jam > 0 ? `${jam}j ${menit}m lagi` : menit > 0 ? `${menit}m ${detik}d lagi` : `${detik} detik lagi`;
                el.innerHTML = `<div class="next-join-info next-join-waiting"><span class="next-join-icon">⏳</span><div class="next-join-detail"><span class="next-join-label">Status Throttle Broadcast</span><span class="next-join-value">Tahan hingga ${throttle.next_allowed_at ? throttle.next_allowed_at.slice(11,16) : '?'} <em>(${sisaStr})</em></span></div></div>`;
            }
        }
    } catch {}

    try {
        const params = new URLSearchParams({ page: 1, page_size: 30 });
        if (_activeCampaignFilter) params.set('campaign_id', _activeCampaignFilter);

        const data = await _getV2(`/broadcast-queue?${params}`);
        const items = data.items || [];

        if (!items.length) {
            document.getElementById('list-broadcast-queue').innerHTML =
                `<div class="empty-state"><div class="icon">📦</div><p>Queue kosong.${_activeCampaignFilter ? ' Klik "Lihat Queue" pada campaign lain.' : ''}</p></div>`;
            return;
        }

        document.getElementById('list-broadcast-queue').innerHTML = `
            ${_activeCampaignFilter ? `<div style="margin-bottom:8px;font-size:12px;color:#888">Filter campaign ID: ${_activeCampaignFilter} <button class="btn-xs btn-outline" onclick="_activeCampaignFilter=null;muatBroadcastQueue()">× Hapus filter</button></div>` : ''}
            <table class="tabel-data">
                <thead><tr>
                    <th>ID</th><th>Grup</th><th>Sender</th><th>Campaign</th>
                    <th>Status</th><th>Aksi</th>
                </tr></thead>
                <tbody>
                ${items.map(t => `
                <tr>
                    <td>${t.id}</td>
                    <td>${t.group_name || t.group_id}</td>
                    <td>${t.sender_account_id || '-'}</td>
                    <td>${t.campaign_name || t.campaign_id}</td>
                    <td>${_badgeStatus(t.status)}</td>
                    <td>
                        <div style="display:flex;gap:4px;flex-wrap:wrap">
                            ${t.status === 'failed' ? `<button class="btn-success btn-xs" onclick="retryQueueTarget(${t.id})">🔁 Retry</button>` : ''}
                            <button class="btn-outline btn-xs" onclick="skipQueueTarget(${t.id})">⏭️ Skip</button>
                            <button class="btn-danger btn-xs" onclick="blockQueueTarget(${t.id})">🚫 Block</button>
                        </div>
                    </td>
                </tr>`).join('')}
                </tbody>
            </table>`;
    } catch (e) {
        document.getElementById('list-broadcast-queue').innerHTML =
            `<div class="empty-state"><div class="icon">⚠️</div><p>Gagal muat queue: ${e.message}</p></div>`;
    }
}

async function resetStuckQueue() {
    if (!confirm('Reset semua target yang stuck (queued tertahan + failed belum final + grup hold)? Ini akan langsung memasukkan semua ke antrian.')) return;
    try {
        const r = await _postV2('/broadcast-queue/reset-stuck', {});
        alert('✅ ' + (r.message || 'Reset berhasil') + '
' + JSON.stringify(r));
        muatBroadcastQueue();
    } catch(e) { alert('❌ Gagal: ' + e); }
}

async function retryFailedQueue() {
    if (!confirm('Retry semua target yang failed?')) return;
    try {
        const r = await _postV2('/broadcast-queue/retry-failed', { limit: 100 });
        alert(`✅ ${r.requeued_count || 0} target dimasukkan kembali ke queue.`);
        muatBroadcastQueue();
    } catch (e) { alert('Gagal: ' + e.message); }
}

async function pauseQueue() {
    try { await _postV2('/broadcast-queue/pause', {}); alert('✅ Queue dijeda.'); }
    catch (e) { alert('Gagal: ' + e.message); }
}

async function resumeQueue() {
    try { await _postV2('/broadcast-queue/resume', {}); alert('✅ Queue dilanjutkan.'); }
    catch (e) { alert('Gagal: ' + e.message); }
}

async function retryQueueTarget(id) {
    try { await _postV2(`/broadcast-queue/${id}/retry`, {}); muatBroadcastQueue(); }
    catch (e) { alert('Gagal: ' + e.message); }
}

async function skipQueueTarget(id) {
    try { await _postV2(`/broadcast-queue/${id}/skip`, { reason: 'manual_skip' }); muatBroadcastQueue(); }
    catch (e) { alert('Gagal: ' + e.message); }
}

async function blockQueueTarget(id) {
    const reason = prompt('Alasan diblok:');
    if (!reason) return;
    try { await _postV2(`/broadcast-queue/${id}/block`, { reason }); muatBroadcastQueue(); }
    catch (e) { alert('Gagal: ' + e.message); }
}

// ── Helper badge campaign ─────────────────────────────────
function _badgeCampaign(status) {
    const map = {
        draft: 'badge-info', queued: 'badge-info', running: 'badge-aktif',
        paused: 'badge-warn', stopped: 'badge-gagal', done: 'badge-aktif',
    };
    return `<span class="${map[status] || 'badge-info'}">${status || '-'}</span>`;
}
