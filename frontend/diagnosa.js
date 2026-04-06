// ── DIAGNOSA MACET & STUCK ────────────────────────────────────────────────

let _diagnosaData = null;
let _diagnosaTimer = null;
let _diagnosaAutoRefresh = false;

async function muatDiagnosa() {
    const el = document.getElementById('diagnosa-konten');
    if (!el) return;
    el.innerHTML = `<div class="loading"><span class="spinner"></span> Menganalisis sistem...</div>`;
    try {
        const res = await _get('/v2/diagnosa');
        const d = res.data || res;
        _diagnosaData = d;
        el.innerHTML = _renderDiagnosa(d);
        _bindDiagnosaEvents();
    } catch (err) {
        el.innerHTML = `<div class="pesan-status error">❌ Gagal memuat diagnosa: ${err.message}</div>`;
    }
}

function _renderDiagnosa(d) {
    const stat = d.statistik || {};
    const gs = stat.grup || {};
    const as = stat.akun || {};

    const totalMacet =
        (d.waiting_join?.jumlah || 0) +
        (d.akun_bermasalah?.jumlah || 0) +
        (d.campaign_target_stuck?.jumlah || 0) +
        (d.recovery_stuck?.jumlah || 0);

    const statusWarna = totalMacet === 0 ? 'green' : totalMacet < 5 ? 'orange' : 'red';
    const statusTeks = totalMacet === 0 ? '✅ Sistem berjalan normal' : `⚠️ ${totalMacet} item membutuhkan perhatian`;

    return `
    <div style="display:flex;gap:12px;align-items:center;margin-bottom:20px;flex-wrap:wrap;">
        <div style="background:${statusWarna};color:#fff;padding:8px 18px;border-radius:8px;font-weight:bold;font-size:14px;">
            ${statusTeks}
        </div>
        <button onclick="muatDiagnosa()" class="btn-outline btn-sm">🔄 Refresh Sekarang</button>
        <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer;">
            <input type="checkbox" id="diagnosa-autorefresh" onchange="_toggleDiagnosaAutoRefresh(this.checked)"
                ${_diagnosaAutoRefresh ? 'checked' : ''}>
            Auto-refresh 30 detik
        </label>
        <span style="color:#888;font-size:12px;">Terakhir: ${stat.digenerate_pada || '-'}</span>
    </div>

    <!-- RINGKASAN STATISTIK -->
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:24px;">
        ${_statBox('Total Grup Aktif', gs.grup_aktif, '#2196F3')}
        ${_statBox('Grup Managed', gs.grup_managed, '#4CAF50')}
        ${_statBox('Menunggu Join', d.waiting_join?.jumlah || 0, d.waiting_join?.jumlah > 0 ? '#FF9800' : '#4CAF50')}
        ${_statBox('Grup Blocked', gs.grup_blocked, gs.grup_blocked > 0 ? '#f44336' : '#4CAF50')}
        ${_statBox('Grup Cooldown', gs.grup_cooldown, gs.grup_cooldown > 0 ? '#FF9800' : '#4CAF50')}
        ${_statBox('Approval Pending', gs.menunggu_approval, gs.menunggu_approval > 0 ? '#FF9800' : '#4CAF50')}
        ${_statBox('Akun Aktif', as.akun_aktif, '#2196F3')}
        ${_statBox('Akun Bermasalah', d.akun_bermasalah?.jumlah || 0, d.akun_bermasalah?.jumlah > 0 ? '#f44336' : '#4CAF50')}
    </div>

    <!-- TABS DETAIL -->
    <div class="diagnosa-tabs">
        <button class="dtab aktif" onclick="_switchDtab(this,'dtab-join')">
            🕐 Waiting Join <span class="badge-num ${(d.waiting_join?.jumlah||0)>0?'warn':''}">${d.waiting_join?.jumlah||0}</span>
        </button>
        <button class="dtab" onclick="_switchDtab(this,'dtab-broadcast')">
            📡 Broadcast Macet <span class="badge-num ${(d.broadcast_macet?.jumlah||0)>0?'warn':''}">${d.broadcast_macet?.jumlah||0}</span>
        </button>
        <button class="dtab" onclick="_switchDtab(this,'dtab-akun')">
            👤 Akun Bermasalah <span class="badge-num ${(d.akun_bermasalah?.jumlah||0)>0?'err':''}">${d.akun_bermasalah?.jumlah||0}</span>
        </button>
        <button class="dtab" onclick="_switchDtab(this,'dtab-target')">
            🎯 Target Stuck <span class="badge-num ${(d.campaign_target_stuck?.jumlah||0)>0?'warn':''}">${d.campaign_target_stuck?.jumlah||0}</span>
        </button>
        <button class="dtab" onclick="_switchDtab(this,'dtab-recovery')">
            🔧 Recovery Partial <span class="badge-num ${(d.recovery_stuck?.jumlah||0)>0?'warn':''}">${d.recovery_stuck?.jumlah||0}</span>
        </button>
    </div>

    <div id="dtab-join" class="dtab-panel">
        ${_renderWaitingJoin(d.waiting_join)}
    </div>
    <div id="dtab-broadcast" class="dtab-panel" style="display:none">
        ${_renderBroadcastMacet(d.broadcast_macet)}
    </div>
    <div id="dtab-akun" class="dtab-panel" style="display:none">
        ${_renderAkunBermasalah(d.akun_bermasalah)}
    </div>
    <div id="dtab-target" class="dtab-panel" style="display:none">
        ${_renderTargetStuck(d.campaign_target_stuck)}
    </div>
    <div id="dtab-recovery" class="dtab-panel" style="display:none">
        ${_renderRecoveryStuck(d.recovery_stuck)}
    </div>

    <!-- DETAIL MODAL -->
    <div id="diagnosa-detail-modal" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;
        background:rgba(0,0,0,0.6);z-index:9999;overflow-y:auto;" onclick="_tutupDetailModal(event)">
        <div style="background:var(--card-bg,#1e1e2e);margin:40px auto;max-width:800px;border-radius:12px;
            padding:24px;position:relative;" onclick="event.stopPropagation()">
            <button onclick="_tutupDetailModal()" style="position:absolute;top:12px;right:16px;background:none;
                border:none;color:#aaa;font-size:20px;cursor:pointer;">✕</button>
            <div id="diagnosa-detail-isi"></div>
        </div>
    </div>

    <style>
    .diagnosa-tabs { display:flex;gap:6px;flex-wrap:wrap;margin-bottom:0;border-bottom:2px solid #333;padding-bottom:0; }
    .dtab { background:transparent;border:none;border-bottom:3px solid transparent;padding:8px 14px;
        cursor:pointer;font-size:13px;color:#aaa;margin-bottom:-2px;transition:all .2s; }
    .dtab.aktif { color:#fff;border-bottom-color:#4a9eff;font-weight:600; }
    .dtab:hover { color:#ddd; }
    .dtab-panel { background:var(--card-bg,#1a1a2e);border-radius:0 0 8px 8px;padding:16px; }
    .badge-num { display:inline-block;background:#444;color:#fff;border-radius:10px;
        padding:1px 7px;font-size:11px;margin-left:4px; }
    .badge-num.warn { background:#FF9800; }
    .badge-num.err { background:#f44336; }
    .diag-table { width:100%;border-collapse:collapse;font-size:13px; }
    .diag-table th { text-align:left;padding:8px 10px;background:#252540;color:#aaa;
        font-weight:600;font-size:12px;border-bottom:1px solid #333; }
    .diag-table td { padding:7px 10px;border-bottom:1px solid #2a2a3e;vertical-align:middle; }
    .diag-table tr:hover td { background:#1f1f35; }
    .penyebab-chip { display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600; }
    .chip-orange { background:#FF980022;color:#FF9800;border:1px solid #FF980055; }
    .chip-red { background:#f4433622;color:#f44336;border:1px solid #f4433655; }
    .chip-blue { background:#2196F322;color:#2196F3;border:1px solid #2196F355; }
    .chip-gray { background:#44444422;color:#aaa;border:1px solid #55555555; }
    .btn-detail { background:#2196F322;border:1px solid #2196F355;color:#4a9eff;
        padding:3px 10px;border-radius:6px;cursor:pointer;font-size:12px; }
    .btn-detail:hover { background:#2196F344; }
    .stat-box { background:var(--card-bg,#1a1a2e);border-radius:8px;padding:14px;text-align:center;border:1px solid #333; }
    .stat-box .val { font-size:26px;font-weight:bold; }
    .stat-box .lbl { font-size:11px;color:#888;margin-top:2px; }
    .empty-msg { text-align:center;padding:40px;color:#666;font-size:14px; }
    .detail-section { margin-bottom:20px; }
    .detail-section h4 { color:#aaa;font-size:13px;font-weight:600;margin-bottom:8px;border-bottom:1px solid #333;padding-bottom:6px; }
    .log-item { background:#14141e;border-radius:6px;padding:8px 12px;margin-bottom:6px;font-size:12px; }
    .log-item .log-time { color:#666;font-size:11px; }
    .log-item .log-msg { color:#ddd;margin-top:2px; }
    .log-level-warning { border-left:3px solid #FF9800; }
    .log-level-error { border-left:3px solid #f44336; }
    .log-level-info { border-left:3px solid #2196F3; }
    </style>
    `;
}

function _statBox(label, val, warna) {
    return `<div class="stat-box">
        <div class="val" style="color:${warna}">${val ?? '-'}</div>
        <div class="lbl">${label}</div>
    </div>`;
}

function _penyebabChip(text) {
    if (!text) return '<span class="penyebab-chip chip-gray">-</span>';
    const map = {
        'menunggu_giliran': ['chip-blue','⏳ Menunggu giliran'],
        'dalam_cooldown': ['chip-orange','🕐 Cooldown'],
        'menunggu_approval_admin': ['chip-orange','📨 Nunggu approval admin'],
        'floodwait_join': ['chip-orange','⏱ FloodWait join'],
        'username_invalid': ['chip-red','❌ Username invalid'],
        'grup_diblokir': ['chip-red','🚫 Grup diblokir'],
        'akun_nonaktif': ['chip-red','⛔ Akun nonaktif'],
        'akun_tidak_ada': ['chip-red','❓ Akun tidak ada'],
        'tidak_ada_username': ['chip-orange','🔗 Tidak ada username'],
        'sudah_join_tapi_belum_diupdate': ['chip-blue','🔄 Perlu sync'],
        'akun_banned': ['chip-red','🚫 Banned'],
        'session_expired_login_ulang': ['chip-red','🔑 Session expired'],
        'dalam_cooldown_floodwait': ['chip-orange','⏱ FloodWait'],
        'level_warming_terlalu_rendah': ['chip-orange','🌡 Warming rendah'],
        'gagal_kirim_permanen': ['chip-red','❌ Gagal permanen'],
        'diblokir_di_grup_ini': ['chip-red','🚫 Diblokir di grup'],
        'sudah_3x_coba_gagal': ['chip-red','⚠️ 3x gagal'],
        'grup_diblokir_permanen': ['chip-red','🚫 Blocked permanen'],
        'dalam_masa_istirahat': ['chip-orange','😴 Cooldown'],
        'belum_ada_owner': ['chip-blue','👤 Belum ada owner'],
        'owner_belum_join': ['chip-orange','⏳ Owner belum join'],
    };
    const [cls, label] = map[text] || ['chip-gray', text.replace(/_/g,' ')];
    return `<span class="penyebab-chip ${cls}">${label}</span>`;
}

function _renderWaitingJoin(data) {
    if (!data || data.jumlah === 0) return `<div class="empty-msg">✅ Tidak ada grup yang stuck di tahap join.</div>`;
    const rows = (data.items || []).map(g => `
        <tr>
            <td><b>${g.nama || '-'}</b><br><small style="color:#666">@${g.username || '-'} · ID:${g.id}</small></td>
            <td>${g.owner_phone || '-'}</td>
            <td>${_penyebabChip(g.penyebab_macet)}</td>
            <td style="color:#aaa;font-size:12px">${g.join_ready_at ? '⏰ ' + g.join_ready_at : '-'}</td>
            <td>${g.akun_status ? `<span style="color:${g.akun_status==='active'?'#4CAF50':'#f44336'}">${g.akun_status}</span>` : '-'}</td>
            <td><button class="btn-detail" onclick="_lihatDetailGrup(${g.id}, '${(g.nama||'').replace(/'/g,"\\'")}')">Detail</button></td>
        </tr>`).join('');
    return `
    <p style="color:#888;font-size:13px;margin:0 0 12px">
        Grup ini sudah punya owner tapi akun belum berhasil join. Sistem akan terus mencoba secara otomatis.
    </p>
    <table class="diag-table">
        <thead><tr><th>Grup</th><th>Owner Akun</th><th>Penyebab</th><th>Coba Lagi</th><th>Status Akun</th><th></th></tr></thead>
        <tbody>${rows}</tbody>
    </table>`;
}

function _renderBroadcastMacet(data) {
    if (!data || data.jumlah === 0) return `<div class="empty-msg">✅ Tidak ada grup yang stuck di broadcast.</div>`;
    const rows = (data.items || []).map(g => `
        <tr>
            <td><b>${g.nama || '-'}</b><br><small style="color:#666">@${g.username || '-'} · ID:${g.id}</small></td>
            <td>${g.owner_phone || '-'}</td>
            <td>${_penyebabChip(g.penyebab_macet)}</td>
            <td style="color:#aaa;font-size:12px">${g.broadcast_ready_at ? '⏰ ' + g.broadcast_ready_at : '-'}</td>
            <td><span style="color:#aaa;font-size:12px">${g.last_kirim ? g.last_kirim.slice(0,16) : 'Belum pernah'}</span></td>
            <td><button class="btn-detail" onclick="_lihatDetailGrup(${g.id}, '${(g.nama||'').replace(/'/g,"\\'")}')">Detail</button></td>
        </tr>`).join('');
    return `
    <p style="color:#888;font-size:13px;margin:0 0 12px">
        Grup yang sudah managed tapi belum bisa menerima pesan karena cooldown, blocked, atau masalah lain.
    </p>
    <table class="diag-table">
        <thead><tr><th>Grup</th><th>Owner</th><th>Penyebab</th><th>Siap Lagi</th><th>Terakhir Kirim</th><th></th></tr></thead>
        <tbody>${rows}</tbody>
    </table>`;
}

function _renderAkunBermasalah(data) {
    if (!data || data.jumlah === 0) return `<div class="empty-msg">✅ Semua akun dalam kondisi baik.</div>`;
    const rows = (data.items || []).map(a => `
        <tr>
            <td><b>${a.nama || '-'}</b><br><small style="color:#666">${a.phone}</small></td>
            <td><span style="color:${a.status==='active'?'#4CAF50':'#f44336'}">${a.status}</span></td>
            <td>${_penyebabChip(a.penyebab_masalah)}</td>
            <td style="color:#aaa;font-size:12px">${a.cooldown_until || '-'}</td>
            <td style="color:#aaa">${a.level_warming ?? '-'}</td>
            <td style="color:#aaa">${a.jumlah_grup ?? 0} grup</td>
            <td><button class="btn-detail" onclick="_lihatDetailAkun('${a.phone}', '${(a.nama||a.phone).replace(/'/g,"\\'")}')">Detail</button></td>
        </tr>`).join('');
    return `
    <p style="color:#888;font-size:13px;margin:0 0 12px">
        Akun yang tidak aktif, kena banned, session expired, atau dalam cooldown FloodWait.
    </p>
    <table class="diag-table">
        <thead><tr><th>Akun</th><th>Status</th><th>Penyebab</th><th>Cooldown s/d</th><th>Level Warming</th><th>Grup</th><th></th></tr></thead>
        <tbody>${rows}</tbody>
    </table>`;
}

function _renderTargetStuck(data) {
    if (!data || data.jumlah === 0) return `<div class="empty-msg">✅ Tidak ada target campaign yang stuck.</div>`;
    const rows = (data.items || []).map(t => `
        <tr>
            <td>${t.nama_grup || '-'}<br><small style="color:#666">@${t.username || '-'} · ID:${t.group_id}</small></td>
            <td style="color:#aaa;font-size:12px">Campaign #${t.campaign_id}</td>
            <td>${t.akun || '-'}</td>
            <td><span style="color:${t.status==='failed'||t.status==='blocked'?'#f44336':'#FF9800'}">${t.status}</span></td>
            <td>${_penyebabChip(t.penyebab_macet)}</td>
            <td style="color:#aaa;font-size:12px">${t.attempt_count || 0}x</td>
            <td><button class="btn-detail" onclick="_lihatDetailGrup(${t.group_id}, '${(t.nama_grup||'').replace(/'/g,"\\'")}')">Detail</button></td>
        </tr>`).join('');
    return `
    <p style="color:#888;font-size:13px;margin:0 0 12px">
        Target pengiriman yang gagal, diblokir, atau sudah dicoba berkali-kali tapi belum berhasil.
    </p>
    <table class="diag-table">
        <thead><tr><th>Grup</th><th>Campaign</th><th>Akun</th><th>Status</th><th>Penyebab</th><th>Percobaan</th><th></th></tr></thead>
        <tbody>${rows}</tbody>
    </table>`;
}

function _renderRecoveryStuck(data) {
    if (!data || data.jumlah === 0) return `<div class="empty-msg">✅ Tidak ada item recovery yang partial/gagal.</div>`;
    const sevColor = { high: '#f44336', medium: '#FF9800', low: '#888' };
    const rows = (data.items || []).map(r => `
        <tr>
            <td>${r.entity_name || r.entity_id || '-'}</td>
            <td style="color:#aaa;font-size:12px">${r.item_type || '-'}</td>
            <td><span style="color:${sevColor[r.severity]||'#888'}">${r.severity || '-'}</span></td>
            <td><span style="color:#FF9800">${r.recovery_status}</span></td>
            <td style="color:#aaa;font-size:12px;max-width:200px;word-break:break-word">${r.reason || '-'}</td>
            <td style="color:#aaa;font-size:12px">${r.attempt_count || 0}x</td>
            <td style="color:#aaa;font-size:12px">${r.last_attempt_at ? r.last_attempt_at.slice(0,16) : '-'}</td>
        </tr>`).join('');
    return `
    <p style="color:#888;font-size:13px;margin:0 0 12px">
        Item yang sistem sudah coba perbaiki otomatis tapi belum selesai. Recovery berjalan tiap siklus orchestrator.
    </p>
    <table class="diag-table">
        <thead><tr><th>Entitas</th><th>Tipe</th><th>Prioritas</th><th>Status</th><th>Alasan</th><th>Percobaan</th><th>Terakhir</th></tr></thead>
        <tbody>${rows}</tbody>
    </table>`;
}

function _switchDtab(btn, panelId) {
    document.querySelectorAll('.dtab').forEach(b => b.classList.remove('aktif'));
    document.querySelectorAll('.dtab-panel').forEach(p => p.style.display = 'none');
    btn.classList.add('aktif');
    const panel = document.getElementById(panelId);
    if (panel) panel.style.display = 'block';
}

async function _lihatDetailGrup(grupId, nama) {
    const modal = document.getElementById('diagnosa-detail-modal');
    const isi = document.getElementById('diagnosa-detail-isi');
    if (!modal || !isi) return;
    isi.innerHTML = `<div class="loading"><span class="spinner"></span> Memuat detail grup...</div>`;
    modal.style.display = 'block';
    try {
        const res = await _get(`/v2/diagnosa/grup/${grupId}`);
        const d = res.data || res;
        const g = d.grup || {};
        isi.innerHTML = `
            <h3 style="margin:0 0 16px;color:#fff">🔬 Detail Grup: ${nama}</h3>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:20px;">
                ${_detailRow('Status', g.status)} ${_detailRow('Assignment', g.assignment_status)}
                ${_detailRow('Broadcast Status', g.broadcast_status)} ${_detailRow('Join Status', g.join_status)}
                ${_detailRow('Owner', g.owner_phone)} ${_detailRow('Username', '@' + (g.username || '-'))}
                ${_detailRow('Join Hold Reason', g.join_hold_reason || '-')} ${_detailRow('Join Ready At', g.join_ready_at || '-')}
                ${_detailRow('Broadcast Hold', g.broadcast_hold_reason || '-')} ${_detailRow('Score', g.score)}
            </div>

            <div class="detail-section">
                <h4>👥 Akun yang Sudah Join (${d.akun_join?.length || 0})</h4>
                ${(d.akun_join||[]).length === 0 ? '<p style="color:#666;font-size:13px">Belum ada akun yang join grup ini.</p>' :
                    d.akun_join.map(a => `<div class="log-item">
                        <b>${a.phone}</b> ${a.nama ? '('+a.nama+')' : ''} — Status: <b style="color:${a.status==='active'?'#4CAF50':'#f44336'}">${a.status}</b>
                        · Warming: ${a.level_warming} · Score: ${a.score}
                        ${a.cooldown_until ? '<br><span style="color:#FF9800">⏱ Cooldown s/d: '+a.cooldown_until+'</span>' : ''}
                    </div>`).join('')}
            </div>

            <div class="detail-section">
                <h4>📋 Riwayat Aksi Terbaru (${d.riwayat?.length || 0})</h4>
                ${_renderLogItems(d.riwayat, r => `${r.phone} → <b>${r.status}</b>${r.pesan_error ? ': <span style="color:#FF9800">'+r.pesan_error+'</span>' : ''}`, r => r.waktu)}
            </div>

            <div class="detail-section">
                <h4>🧾 Audit Log (${d.audit_log?.length || 0})</h4>
                ${_renderLogItems(d.audit_log, r => `[${r.module}] ${r.action} — ${r.message}`, r => r.created_at, r => r.level)}
            </div>

            <div class="detail-section">
                <h4>🎯 Campaign Targets (${d.campaign_targets?.length || 0})</h4>
                ${(d.campaign_targets||[]).length === 0 ? '<p style="color:#666;font-size:13px">Belum ada target campaign untuk grup ini.</p>' :
                    d.campaign_targets.map(t => `<div class="log-item">
                        Campaign #${t.campaign_id} · Akun: ${t.assigned_account_id || '-'} ·
                        Status: <b style="color:${t.status==='sent'?'#4CAF50':t.status==='failed'||t.status==='blocked'?'#f44336':'#FF9800'}">${t.status}</b>
                        · ${t.attempt_count || 0}x percobaan
                        ${t.hold_reason ? '<br><span style="color:#FF9800">Hold: '+t.hold_reason+'</span>' : ''}
                    </div>`).join('')}
            </div>
        `;
    } catch (err) {
        isi.innerHTML = `<div class="pesan-status error">❌ ${err.message}</div>`;
    }
}

async function _lihatDetailAkun(phone, nama) {
    const modal = document.getElementById('diagnosa-detail-modal');
    const isi = document.getElementById('diagnosa-detail-isi');
    if (!modal || !isi) return;
    isi.innerHTML = `<div class="loading"><span class="spinner"></span> Memuat detail akun...</div>`;
    modal.style.display = 'block';
    try {
        const res = await _get(`/v2/diagnosa/akun/${encodeURIComponent(phone)}`);
        const d = res.data || res;
        const a = d.akun || {};
        isi.innerHTML = `
            <h3 style="margin:0 0 16px;color:#fff">🔬 Detail Akun: ${nama}</h3>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:20px;">
                ${_detailRow('Phone', a.phone)} ${_detailRow('Nama', a.nama || '-')}
                ${_detailRow('Status', a.status)} ${_detailRow('Level Warming', a.level_warming)}
                ${_detailRow('Score', a.score)} ${_detailRow('Cooldown s/d', a.cooldown_until || '-')}
                ${_detailRow('Total Flood', a.total_flood || 0)} ${_detailRow('Total Banned', a.total_banned || 0)}
            </div>

            <div class="detail-section">
                <h4>📁 Grup yang Dipegang (${d.grup_dipegang?.length || 0})</h4>
                ${(d.grup_dipegang||[]).length === 0 ? '<p style="color:#666;font-size:13px">Tidak ada grup.</p>' :
                    d.grup_dipegang.slice(0,10).map(g => `<div class="log-item">
                        <b>${g.nama || '-'}</b> @${g.username || '-'} · 
                        Assignment: <b>${g.assignment_status}</b> · 
                        Broadcast: <b style="color:${g.broadcast_status==='blocked'?'#f44336':g.broadcast_status==='managed'?'#4CAF50':'#FF9800'}">${g.broadcast_status||'hold'}</b>
                    </div>`).join('')}
                ${(d.grup_dipegang||[]).length > 10 ? `<p style="color:#666;font-size:12px">...dan ${d.grup_dipegang.length-10} grup lainnya</p>` : ''}
            </div>

            <div class="detail-section">
                <h4>📋 Riwayat Aksi Terbaru (${d.riwayat?.length || 0})</h4>
                ${_renderLogItems(d.riwayat, r => `${r.nama_grup} → <b>${r.status}</b>${r.pesan_error ? ': <span style="color:#FF9800">'+r.pesan_error+'</span>' : ''}`, r => r.waktu)}
            </div>

            <div class="detail-section">
                <h4>🧾 Audit Log (${d.audit_log?.length || 0})</h4>
                ${_renderLogItems(d.audit_log, r => `[${r.module}] ${r.action} — ${r.message}`, r => r.created_at, r => r.level)}
            </div>
        `;
    } catch (err) {
        isi.innerHTML = `<div class="pesan-status error">❌ ${err.message}</div>`;
    }
}

function _detailRow(label, val) {
    return `<div style="background:#14141e;border-radius:6px;padding:8px 12px;">
        <div style="font-size:11px;color:#666;margin-bottom:2px">${label}</div>
        <div style="font-size:13px;color:#ddd;font-weight:500">${val ?? '-'}</div>
    </div>`;
}

function _renderLogItems(items, msgFn, timeFn, levelFn) {
    if (!items || items.length === 0) return '<p style="color:#666;font-size:13px">Tidak ada log.</p>';
    return items.slice(0, 20).map(r => {
        const level = levelFn ? levelFn(r) : 'info';
        return `<div class="log-item log-level-${level}">
            <div class="log-time">${timeFn(r) ? timeFn(r).slice(0,19) : '-'}</div>
            <div class="log-msg">${msgFn(r)}</div>
        </div>`;
    }).join('');
}

function _tutupDetailModal(event) {
    if (event && event.target !== document.getElementById('diagnosa-detail-modal')) return;
    const modal = document.getElementById('diagnosa-detail-modal');
    if (modal) modal.style.display = 'none';
}

function _toggleDiagnosaAutoRefresh(aktif) {
    _diagnosaAutoRefresh = aktif;
    if (_diagnosaTimer) { clearInterval(_diagnosaTimer); _diagnosaTimer = null; }
    if (aktif) {
        _diagnosaTimer = setInterval(() => {
            if (window.tabAktif === 'diagnosa') muatDiagnosa();
        }, 30000);
    }
}

function _bindDiagnosaEvents() {
    // Checkbox state preservation
    const cb = document.getElementById('diagnosa-autorefresh');
    if (cb) cb.checked = _diagnosaAutoRefresh;
}
