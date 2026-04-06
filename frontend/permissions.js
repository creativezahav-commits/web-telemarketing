// ============================================================
// permissions.js — Tab Permissions Grup
// Memanggil /api/v2/permissions
// ============================================================

let _permPage = 1;
const _permPageSize = 25;

async function muatPermissions() {
    const search = (document.getElementById('permissions-search') || {}).value || '';
    const status = (document.getElementById('permissions-filter-status') || {}).value || '';
    setLoading('list-permissions', 'Memuat permissions...');

    // Muat summary
    try {
        const sum = await _getV2('/permissions/summary');
        const grid = document.getElementById('permissions-summary-grid');
        if (grid) {
            grid.innerHTML = [
                ['Total', sum.total || 0, ''],
                ['Valid', sum.valid || 0, 'green'],
                ['Expired', sum.expired || 0, 'warn'],
                ['Revoked', sum.revoked || 0, 'danger'],
            ].map(([l, v, c]) => `<div class="ringkasan-item ${c}"><div class="ringkasan-angka">${v}</div><div class="ringkasan-label">${l}</div></div>`).join('');
        }
    } catch {}

    try {
        const params = new URLSearchParams({ page: _permPage, page_size: _permPageSize });
        if (search) params.set('search', search);
        if (status) params.set('status', status);

        const data = await _getV2(`/permissions?${params}`);
        const items = data.items || [];
        const total = (data._meta || {}).total || 0;

        if (!items.length) {
            document.getElementById('list-permissions').innerHTML =
                `<div class="empty-state"><div class="icon">🔐</div><p>Belum ada permission. Klik "Tambah Permission" untuk mulai.</p></div>`;
            document.getElementById('permissions-pagination').innerHTML = '';
            return;
        }

        document.getElementById('list-permissions').innerHTML = `
            <table class="tabel-data">
                <thead><tr>
                    <th>ID</th><th>Grup</th><th>Basis</th><th>Status</th>
                    <th>Disetujui oleh</th><th>Berlaku sampai</th><th>Aksi</th>
                </tr></thead>
                <tbody>
                ${items.map(p => `
                <tr>
                    <td>${p.id}</td>
                    <td><strong>${p.group_name || p.group_id}</strong><br><span style="font-size:11px;color:#888">ID: ${p.group_id}</span></td>
                    <td><span class="badge-tipe">${p.permission_basis || '-'}</span></td>
                    <td>${_badgeStatus(p.status)}</td>
                    <td>${p.approved_by || '-'}</td>
                    <td style="font-size:12px">${p.expires_at ? p.expires_at.slice(0,16) : 'Tidak terbatas'}</td>
                    <td>
                        <div style="display:flex;gap:4px;flex-wrap:wrap">
                            ${p.status !== 'valid' ? `<button class="btn-outline btn-xs" onclick="approvePermission(${p.id})">✅ Approve</button>` : ''}
                            ${p.status === 'valid' ? `<button class="btn-danger btn-xs" onclick="revokePermission(${p.id})">🚫 Revoke</button>` : ''}
                        </div>
                    </td>
                </tr>`).join('')}
                </tbody>
            </table>`;

        // Pagination
        const totalPages = Math.ceil(total / _permPageSize);
        document.getElementById('permissions-pagination').innerHTML = totalPages > 1
            ? `<button class="btn-outline btn-sm" ${_permPage <= 1 ? 'disabled' : ''} onclick="_permPage--;muatPermissions()">← Prev</button>
               <span style="font-size:13px">Hal ${_permPage} / ${totalPages} (${total} total)</span>
               <button class="btn-outline btn-sm" ${_permPage >= totalPages ? 'disabled' : ''} onclick="_permPage++;muatPermissions()">Next →</button>`
            : `<span style="font-size:12px;color:#888">${total} permission</span>`;
    } catch (e) {
        document.getElementById('list-permissions').innerHTML =
            `<div class="empty-state"><div class="icon">⚠️</div><p>Gagal muat permissions: ${e.message}</p></div>`;
    }
}

function tampilFormTambahPermission() {
    const el = document.getElementById('form-tambah-permission');
    if (el) el.style.display = el.style.display === 'none' ? 'block' : 'none';
}

async function simpanPermission() {
    const group_id = document.getElementById('perm-group-id').value;
    const permission_basis = document.getElementById('perm-basis').value;
    const approved_by = document.getElementById('perm-approved-by').value;
    const expires_at = document.getElementById('perm-expires-at').value;
    const notes = document.getElementById('perm-notes').value;

    if (!group_id) return tampilPesan('pesan-permission', '❌ ID Grup wajib diisi', 'gagal');
    tampilPesan('pesan-permission', '⏳ Menyimpan...', 'info');

    try {
        await _postV2('/permissions', { group_id: parseInt(group_id), permission_basis, approved_by, expires_at: expires_at || null, notes });
        tampilPesan('pesan-permission', '✅ Permission berhasil disimpan!', 'berhasil');
        document.getElementById('form-tambah-permission').style.display = 'none';
        _permPage = 1;
        muatPermissions();
    } catch (e) {
        tampilPesan('pesan-permission', `❌ Gagal: ${e.message}`, 'gagal');
    }
}

async function approvePermission(id) {
    if (!confirm('Setujui permission ini?')) return;
    try {
        await _postV2(`/permissions/${id}/approve`, {});
        muatPermissions();
    } catch (e) {
        alert('Gagal approve: ' + e.message);
    }
}

async function revokePermission(id) {
    const reason = prompt('Alasan pencabutan (opsional):');
    if (reason === null) return;
    try {
        await _postV2(`/permissions/${id}/revoke`, { reason });
        muatPermissions();
    } catch (e) {
        alert('Gagal revoke: ' + e.message);
    }
}

async function recheckExpiredPermissions() {
    try {
        const r = await _postV2('/permissions/recheck-expired', {});
        alert(`✅ Selesai. ${r.changed || 0} permission diperbarui.`);
        muatPermissions();
    } catch (e) {
        alert('Gagal: ' + e.message);
    }
}
