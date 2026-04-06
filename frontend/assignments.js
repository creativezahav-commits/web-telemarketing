// ============================================================
// assignments.js — Tab Assignments Grup
// Fitur baru: toggle auto assign otomatis + status real-time
// ============================================================

let _assignPage = 1;
const _assignPageSize = 25;
let _autoAssignInterval = null;

// ── Load tab ─────────────────────────────────────────────
async function muatAssignments() {
    const search = (document.getElementById('assignments-search') || {}).value || '';
    const status = (document.getElementById('assignments-filter-status') || {}).value || '';

    // Muat status toggle auto assign
    await muatStatusAutoAssign();
    await muatKriteriaAutoAssign();

    // Muat summary
    try {
        const sum = await _getV2('/assignments/summary');
        const grid = document.getElementById('assignments-summary-grid');
        if (grid) {
            grid.innerHTML = [
                ['Total',      sum.total      || 0, ''],
                ['Assigned',   sum.assigned   || 0, 'green'],
                ['Failed',     sum.failed     || 0, 'danger'],
                ['Retry Wait', sum.retry_wait || 0, 'warn'],
                ['Released',   sum.released   || 0, ''],
            ].map(([l, v, c]) =>
                `<div class="ringkasan-item ${c}">
                    <div class="ringkasan-angka">${v}</div>
                    <div class="ringkasan-label">${l}</div>
                </div>`
            ).join('');
        }
    } catch {}

    // Muat daftar assignment
    setLoading('list-assignments', 'Memuat assignments...');
    try {
        const params = new URLSearchParams({ page: _assignPage, page_size: _assignPageSize });
        if (search) params.set('search', search);
        if (status) params.set('status', status);

        const data  = await _getV2(`/assignments?${params}`);
        const items = data.items || [];
        const total = (data._meta || {}).total || 0;

        if (!items.length) {
            document.getElementById('list-assignments').innerHTML = `
                <div class="empty-state">
                    <div class="icon">🎯</div>
                    <p>Belum ada assignment. Aktifkan Auto Assign di atas, atau klik "Run Sekali Sekarang".</p>
                </div>`;
            document.getElementById('assignments-pagination').innerHTML = '';
            return;
        }

        document.getElementById('list-assignments').innerHTML = `
            <table class="tabel-data">
                <thead><tr>
                    <th>ID</th><th>Grup</th><th>Akun Owner</th><th>Tipe</th>
                    <th>Status</th><th>Retry</th><th>Alasan</th><th>Aksi</th>
                </tr></thead>
                <tbody>
                ${items.map(a => `
                <tr>
                    <td style="color:#888;font-size:11px">${a.id}</td>
                    <td>
                        <strong>${a.group_name || a.group_id}</strong>
                        <div style="font-size:11px;color:#aaa">ID: ${a.group_id}</div>
                    </td>
                    <td style="font-size:12px">${a.assigned_account_id || '<span style="color:#aaa">-</span>'}</td>
                    <td><span class="badge-tipe">${a.assignment_type || 'sync_owner'}</span></td>
                    <td>${_badgeStatus(a.status)}</td>
                    <td style="font-size:12px;color:#888">${a.retry_count || 0}x</td>
                    <td style="font-size:11px;color:#888;max-width:150px;word-break:break-word">
                        ${a.assign_reason === 'auto_assign_background'
                            ? '🤖 Otomatis'
                            : a.assign_reason === 'auto_assign_v2'
                            ? '⚡ Manual run'
                            : a.assign_reason || '-'}
                    </td>
                    <td>
                        <div style="display:flex;gap:4px;flex-wrap:wrap">
                            <button class="btn-outline btn-xs" onclick="retryAssignment(${a.id})" title="Retry">🔁</button>
                            <button class="btn-outline btn-xs" onclick="reassignAssignment(${a.id})" title="Reassign ke akun lain">🔀</button>
                            <button class="btn-danger btn-xs" onclick="releaseAssignment(${a.id})" title="Lepas — grup jadi tidak punya owner">🔓</button>
                        </div>
                    </td>
                </tr>`).join('')}
                </tbody>
            </table>`;

        const totalPages = Math.ceil(total / _assignPageSize);
        document.getElementById('assignments-pagination').innerHTML = totalPages > 1
            ? `<button class="btn-outline btn-sm" ${_assignPage <= 1 ? 'disabled' : ''}
                   onclick="_assignPage--;muatAssignments()">← Prev</button>
               <span style="font-size:13px;color:#888">Hal ${_assignPage} / ${totalPages} &nbsp;·&nbsp; ${total} total</span>
               <button class="btn-outline btn-sm" ${_assignPage >= totalPages ? 'disabled' : ''}
                   onclick="_assignPage++;muatAssignments()">Next →</button>`
            : `<span style="font-size:12px;color:#888">${total} assignment</span>`;

    } catch (e) {
        document.getElementById('list-assignments').innerHTML = `
            <div class="empty-state"><div class="icon">⚠️</div><p>Gagal muat: ${e.message}</p></div>`;
    }
}



async function muatKriteriaAutoAssign() {
    const box = document.getElementById('assignments-criteria-box');
    if (!box) return;
    try {
        const data = await _getV2('/assignments/criteria');
        const f = data.filters || {};
        const r = data.ranking || {};
        const rt = data.retries || {};
        box.innerHTML = `
            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px">
                <div class="akun-detail-box">
                    <div class="adb-label">Filter kandidat</div>
                    <div class="adb-sisa">Status akun: ${(f.status_allowed || []).join(', ') || '-'}</div>
                    <div class="adb-sisa">Auto assign akun wajib aktif: ${f.auto_assign_enabled_required ? 'Ya' : 'Tidak'}</div>
                    <div class="adb-sisa">Health score minimum: <strong>${f.min_health_score ?? '-'}</strong></div>
                    <div class="adb-sisa">Warming level minimum: <strong>${f.min_warming_level ?? '-'}</strong></div>
                    <div class="adb-sisa">Cooldown harus clear: ${f.cooldown_must_be_clear ? 'Ya' : 'Tidak'}</div>
                </div>
                <div class="akun-detail-box">
                    <div class="adb-label">Kapasitas & ranking</div>
                    <div class="adb-sisa">Batas kapasitas: ${f.capacity_rule || '-'}</div>
                    <div class="adb-sisa">Prefer akun yang sudah join grup: ${r.prefer_joined_owner ? 'Ya' : 'Tidak'}</div>
                    <div class="adb-sisa">Urutan ranking: ${(r.order || []).join(' → ') || '-'}</div>
                    <div class="adb-sisa">Formula: <code style="font-size:11px">${r.formula || '-'}</code></div>
                </div>
                <div class="akun-detail-box">
                    <div class="adb-label">Retry & pengaturan</div>
                    <div class="adb-sisa">Retry assignment: <strong>${rt.assignment_retry_count ?? '-'}</strong>x</div>
                    <div class="adb-sisa">Reassign maksimum: <strong>${rt.assignment_reassign_count ?? '-'}</strong>x</div>
                    <div class="adb-sisa">Lokasi ubah: <strong>${(data.where_to_change || {}).tab || 'Settings'}</strong> → ${(data.where_to_change || {}).scope || 'assignment-rules'}</div>
                    <div class="adb-sisa">Keys: ${((data.where_to_change || {}).keys || []).join(', ')}</div>
                </div>
            </div>`;
    } catch (e) {
        box.innerHTML = `<div class="empty-state"><div class="icon">⚠️</div><p>Gagal memuat kriteria auto assign: ${e.message}</p></div>`;
    }
}

// ── Auto Assign Toggle ────────────────────────────────────

async function muatStatusAutoAssign() {
    try {
        const r = await fetch(`${API}/auto-assign/status`);
        const data = await r.json();
        _renderAutoAssignStatus(data.enabled);
    } catch {
        _renderAutoAssignStatus(false);
    }
}

function _renderAutoAssignStatus(enabled) {
    const badge = document.getElementById('auto-assign-status-badge');
    const btn   = document.getElementById('btn-toggle-auto-assign');
    const info  = document.getElementById('auto-assign-info');
    const panel = document.getElementById('auto-assign-panel');

    if (!badge || !btn) return;

    if (enabled) {
        badge.textContent       = '🟢 AKTIF';
        badge.style.background  = '#dcfce7';
        badge.style.color       = '#16a34a';
        btn.textContent         = '⏹️ Matikan Auto Assign';
        btn.className           = 'btn-danger';
        btn.style.minWidth      = '180px';
        if (info)  info.style.display  = 'block';
        if (panel) panel.style.borderColor = '#86efac';
    } else {
        badge.textContent       = '⭕ NONAKTIF';
        badge.style.background  = '#f1f5f9';
        badge.style.color       = '#64748b';
        btn.textContent         = '▶️ Aktifkan Auto Assign';
        btn.className           = 'btn-success';
        btn.style.minWidth      = '180px';
        if (info)  info.style.display  = 'none';
        if (panel) panel.style.borderColor = '#e5e7eb';
    }
}

async function toggleAutoAssign() {
    const btn = document.getElementById('btn-toggle-auto-assign');
    if (btn) { btn.disabled = true; btn.textContent = '⏳ Memproses...'; }

    try {
        const r    = await fetch(`${API}/auto-assign/toggle`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({}),
        });
        const data = await r.json();
        _renderAutoAssignStatus(data.enabled);

        // Tampilkan konfirmasi
        const panel = document.getElementById('auto-assign-panel');
        if (panel) {
            const konfirmasi = document.createElement('div');
            konfirmasi.style.cssText = `
                margin-top:10px;padding:8px 12px;border-radius:6px;font-size:12px;font-weight:600;
                background:${data.enabled ? '#dcfce7' : '#fff7ed'};
                color:${data.enabled ? '#16a34a' : '#9a3412'};
            `;
            konfirmasi.textContent = data.enabled
                ? '✅ Auto assign diaktifkan — sistem akan mulai assign otomatis dalam 60 detik.'
                : '⏹️ Auto assign dimatikan — tidak ada assignment otomatis sampai diaktifkan kembali.';
            panel.appendChild(konfirmasi);
            setTimeout(() => konfirmasi.remove(), 5000);
        }
    } catch (e) {
        alert('Gagal mengubah status auto assign: ' + e.message);
        muatStatusAutoAssign(); // restore status yang benar
    } finally {
        if (btn) btn.disabled = false;
    }
}

// ── Manual Run ────────────────────────────────────────────

async function jalankanAutoAssign() {
    if (!confirm('Jalankan auto-assign sekali sekarang untuk semua grup yang belum punya owner?')) return;
    try {
        const r = await _postV2('/assignments/run-auto', { limit: 200 });
        const dibuat   = (r.created  || []).length;
        const dilewati = (r.skipped  || []).length;

        let pesan = `✅ Auto assign selesai.\n\nDibuat: ${dibuat} assignment baru`;
        if (dilewati > 0) {
            pesan += `\nDilewati: ${dilewati} grup (tidak ada akun tersedia atau semua akun penuh)`;
        }
        if (dibuat === 0 && dilewati === 0) {
            pesan = '✅ Semua grup sudah punya assignment. Tidak ada yang perlu diproses.';
        }
        alert(pesan);
        muatAssignments();
    } catch (e) {
        alert('Gagal: ' + e.message);
    }
}

async function reassignFailed() {
    if (!confirm('Reassign semua assignment yang failed ke kandidat akun terbaik?')) return;
    try {
        const r = await _postV2('/assignments/reassign-failed', { limit: 100 });
        alert(`✅ Reassign selesai.\n${r.reassigned_count || 0} assignment berhasil dipindahkan ke akun lain.`);
        muatAssignments();
    } catch (e) {
        alert('Gagal: ' + e.message);
    }
}

// ── Aksi per baris ────────────────────────────────────────

async function retryAssignment(id) {
    try {
        await _postV2(`/assignments/${id}/retry`, {});
        muatAssignments();
    } catch (e) {
        alert('Gagal retry: ' + e.message);
    }
}

async function reassignAssignment(id) {
    const targetAccount = prompt('ID akun tujuan (kosongkan = pilih otomatis berdasarkan score):');
    if (targetAccount === null) return;
    try {
        const r = await _postV2(`/assignments/${id}/reassign`, {
            target_account_id: targetAccount.trim() || undefined,
        });
        alert(`✅ Assignment berhasil dipindahkan ke akun: ${r.assigned_account_id}`);
        muatAssignments();
    } catch (e) {
        alert('Gagal reassign: ' + e.message);
    }
}

async function releaseAssignment(id) {
    if (!confirm('Lepaskan assignment ini?\nGrup akan kembali ke status "belum punya owner" dan bisa di-assign ulang.')) return;
    try {
        await _postV2(`/assignments/${id}/release`, {});
        muatAssignments();
    } catch (e) {
        alert('Gagal release: ' + e.message);
    }
}
