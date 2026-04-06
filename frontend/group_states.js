// group_states.js — Dashboard visual state grup per tahap pipeline

const GROUP_STATE_META = {
    stabilization: { label: 'Stabilization', icon: '⏳', tone: 'amber' },
    eligible:      { label: 'Eligible',      icon: '✅', tone: 'green' },
    queued:        { label: 'Queued',        icon: '📥', tone: 'blue' },
    cooldown:      { label: 'Cooldown',      icon: '🧊', tone: 'purple' },
    hold:          { label: 'Hold',          icon: '🛑', tone: 'slate' },
    failed:        { label: 'Failed',        icon: '❌', tone: 'red' },
    recovery:      { label: 'Recovery',      icon: '🛠️', tone: 'orange' },
};

function _stateToneClass(key) {
    return (GROUP_STATE_META[key] || {}).tone || 'slate';
}

function _safeText(value, fallback='-') {
    return value === null || value === undefined || value === '' ? fallback : value;
}

function _renderStateSummaryCard(key, count, totalVisible) {
    const meta = GROUP_STATE_META[key] || { label: key, icon: '•' };
    const pct = totalVisible > 0 ? Math.round((count / totalVisible) * 100) : 0;
    return `
        <button class="state-summary-card ${_stateToneClass(key)}" onclick="setGroupStateFocus('${key}')">
            <div class="state-summary-head">
                <span class="state-summary-icon">${meta.icon}</span>
                <span class="state-summary-title">${meta.label}</span>
            </div>
            <div class="state-summary-count">${count}</div>
            <div class="state-summary-foot">${pct}% dari grup terlihat</div>
        </button>`;
}

function _renderSessionStrip(data) {
    const el = document.getElementById('group-state-session-strip');
    const flowEl = document.getElementById('group-state-flow-notes');
    if (!el || !flowEl) return;

    const sessions = data.active_sessions || [];
    const pipeline = data.pipeline || {};
    const stages = pipeline.stages || {};

    const cards = [
        `<div class="session-mini-card"><div class="smc-label">Sesi aktif</div><strong>${data.summary?.active_sessions || 0}</strong><span>${data.summary?.queued_targets || 0} target menunggu</span></div>`,
        `<div class="session-mini-card"><div class="smc-label">Recovery perlu tindakan</div><strong>${data.summary?.recovery_needed || 0}</strong><span>${(data.summary?.state_counts || {}).recovery || 0} grup di lane recovery</span></div>`,
        `<div class="session-mini-card"><div class="smc-label">Eligible → Queued</div><strong>${stages.groups_broadcast_eligible || 0} → ${stages.campaign_targets_waiting || 0}</strong><span>Pindah saat sesi dibentuk</span></div>`,
    ];

    sessions.slice(0, 5).forEach((s) => {
        const status = _safeText(s.status || s.session_status, 'idle');
        cards.push(`
            <div class="session-mini-card ${status}">
                <div class="smc-label">${_safeText(s.name, 'Session')}</div>
                <strong>${status}</strong>
                <span>${_safeText(s.total_targets, 0)} target · sent ${_safeText(s.sent_count, 0)}</span>
            </div>`);
    });

    el.innerHTML = cards.join('');

    const notes = (pipeline.flow_notes || []).map((note) => `<li>${note}</li>`).join('');
    flowEl.innerHTML = notes
        ? `<div class="flow-note-box"><strong>Catatan pipeline</strong><ul>${notes}</ul></div>`
        : '';
}


function _renderDiagnostics(data) {
    const el = document.getElementById('group-state-diagnostics');
    if (!el) return;
    const diag = data.diagnostics || {};
    const automation = diag.automation || {};
    const settings = diag.state_settings || {};
    const laneReasons = data.lane_reason_counts || {};
    const statusPill = (label, enabled) => `<span class="mini-pill ${enabled ? 'ok' : 'danger'}">${label}: ${enabled ? 'ON' : 'OFF'}</span>`;
    const reasonList = (items, emptyText='-') => {
        if (!items || !items.length) return `<div class="diag-reason-empty">${emptyText}</div>`;
        return `<ul class="diag-reason-list">${items.map(it => `<li><span>${_safeText(it.reason)}</span><strong>${_safeText(it.count, 0)}</strong></li>`).join('')}</ul>`;
    };
    const messages = (diag.messages || []).map(msg => `<div class="bottleneck-pill warn">⚠️ ${msg}</div>`).join('') || `<div class="bottleneck-pill ok">✅ Tidak ada pengunci otomatis utama yang terdeteksi.</div>`;
    el.innerHTML = `
        <div class="section-card">
            <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap">
                <div>
                    <h3 style="margin:0">🩺 Diagnostik Otomasi</h3>
                    <div class="hint">Menjelaskan kenapa assign atau broadcast belum bergerak otomatis.</div>
                </div>
                <div class="state-diag-pills">
                    ${statusPill('Auto assign', !!automation.auto_assign_enabled)}
                    ${statusPill('Auto campaign', !!automation.auto_campaign_enabled)}
                    ${statusPill('Auto recovery', !!automation.auto_recovery_enabled)}
                    ${statusPill('Pause all', !!automation.pause_all_automation)}
                    ${statusPill('Maintenance', !!automation.maintenance_mode)}
                    ${statusPill('Broadcast jadwal', !!automation.broadcast_jadwal_aktif)}
                    ${statusPill('Draft aktif', !!diag.active_draft)}
                </div>
            </div>
            <div class="state-bottleneck-strip" style="margin-top:12px">${messages}</div>
            <div class="diag-grid" style="margin-top:14px">
                <div class="diag-card">
                    <div class="diag-card-title">⚙️ Setting state grup</div>
                    <div class="diag-kv"><span>Stabilization delay</span><strong>${_safeText(settings.stabilization_delay_minutes, 0)} menit</strong></div>
                    <div class="diag-kv"><span>Eligible perlu permission valid</span><strong>${settings.eligible_require_valid_permission ? 'Ya' : 'Tidak'}</strong></div>
                    <div class="diag-kv"><span>Eligible perlu managed</span><strong>${settings.eligible_require_managed ? 'Ya' : 'Tidak'}</strong></div>
                    <div class="diag-kv"><span>Queued / session limit</span><strong>${_safeText(settings.queued_session_target_limit, 0)} target</strong></div>
                    <div class="diag-kv"><span>Queued / limit per sender</span><strong>${_safeText(settings.queued_per_sender_limit, 0)} target</strong></div>
                    <div class="diag-kv"><span>Mid-session enqueue</span><strong>${settings.queued_allow_mid_session_enqueue ? 'Aktif' : 'Nonaktif'}</strong></div>
                    <div class="diag-kv"><span>Cooldown</span><strong>${_safeText(settings.cooldown_hours, 0)} jam</strong></div>
                </div>
                <div class="diag-card">
                    <div class="diag-card-title">⏳ Dominan di stabilization</div>
                    ${reasonList(laneReasons.stabilization, 'Belum ada grup di lane stabilization.')}
                </div>
                <div class="diag-card">
                    <div class="diag-card-title">🛑 Dominan di hold</div>
                    ${reasonList(laneReasons.hold, 'Belum ada grup di lane hold.')}
                </div>
                <div class="diag-card">
                    <div class="diag-card-title">🧊 Dominan di cooldown</div>
                    ${reasonList(laneReasons.cooldown, 'Belum ada grup di lane cooldown.')}
                </div>
            </div>
        </div>`;
}

function _renderBottlenecks(items) {
    const el = document.getElementById('group-state-bottlenecks');
    if (!el) return;
    if (!items || !items.length) {
        el.innerHTML = `<div class="bottleneck-pill ok">✅ Tidak ada bottleneck utama yang terdeteksi saat ini.</div>`;
        return;
    }
    el.innerHTML = items.map((item) => `<div class="bottleneck-pill warn">⚠️ ${item}</div>`).join('');
}

function _renderLaneCard(item) {
    const username = item.group_username ? `@${item.group_username}` : '-';
    const targetInfo = item.target_status ? `${item.target_status}${item.campaign_name ? ` · ${item.campaign_name}` : ''}` : 'Belum ada target aktif';
    const recoveryInfo = item.recovery_status
        ? `${item.recovery_status}${item.recovery_problem_type ? ` · ${item.recovery_problem_type}` : ''}`
        : 'Tidak ada recovery aktif';
    return `
        <div class="group-state-card ${_stateToneClass(item.state)}">
            <div class="group-state-card-head">
                <div>
                    <div class="group-state-name">${_safeText(item.group_name)}</div>
                    <div class="group-state-sub">${username} · owner ${_safeText(item.owner_phone)}</div>
                </div>
                <div class="group-state-score">Score ${_safeText(item.score, 0)}</div>
            </div>
            <div class="group-state-badges">
                <span class="mini-pill neutral">assign ${_safeText(item.assignment_status)}</span>
                <span class="mini-pill neutral">broadcast ${_safeText(item.broadcast_status)}</span>
                <span class="mini-pill neutral">guard ${_safeText(item.send_guard_status)}</span>
            </div>
            <div class="group-state-reason">${_safeText(item.state_reason)}</div>
            <div class="group-state-meta-grid">
                <div><span>Ready at</span><strong>${_formatDateTimeText(item.broadcast_ready_at)}</strong></div>
                <div><span>Last chat</span><strong>${_formatDateTimeText(item.last_chat)}</strong></div>
                <div><span>Last kirim</span><strong>${_formatDateTimeText(item.last_kirim)}</strong></div>
                <div><span>Idle days</span><strong>${_safeText(item.idle_days, 0)}</strong></div>
            </div>
            <div class="group-state-line"><span>Target</span><strong>${targetInfo}</strong></div>
            <div class="group-state-line"><span>Recovery</span><strong>${recoveryInfo}</strong></div>
            <div class="group-state-line"><span>Keyword</span><strong>${_safeText(item.source_keyword)}</strong></div>
            <div class="group-state-actions">
                <button class="btn-outline btn-xs" onclick="tampilTab('analisis')">📊 Analisis</button>
                <button class="btn-outline btn-xs" onclick="tampilTab('assignments')">🎯 Assign</button>
                <button class="btn-outline btn-xs" onclick="tampilTab('campaigns')">📣 Campaign</button>
                ${item.recovery_item_id ? `<button class="btn-outline btn-xs" onclick="tampilTab('recovery')">🛠️ Recovery</button>` : ''}
            </div>
        </div>`;
}

function _renderStateBoard(lanes, totalVisible) {
    const el = document.getElementById('group-state-board');
    if (!el) return;
    const order = ['stabilization', 'eligible', 'queued', 'cooldown', 'hold', 'failed', 'recovery'];
    el.innerHTML = order.map((key) => {
        const lane = lanes[key] || { count: 0, items: [] };
        const meta = GROUP_STATE_META[key] || { label: key, icon: '•' };
        const items = (lane.items || []).map(_renderLaneCard).join('') || `<div class="group-state-empty">Belum ada grup pada lane ini.</div>`;
        const count = lane.count || 0;
        const pct = totalVisible > 0 ? Math.round((count / totalVisible) * 100) : 0;
        return `
            <section class="group-state-lane ${_stateToneClass(key)}">
                <div class="group-state-lane-head">
                    <div>
                        <div class="group-state-lane-title">${meta.icon} ${lane.label || meta.label}</div>
                        <div class="group-state-lane-desc">${lane.description || ''}</div>
                    </div>
                    <div class="group-state-lane-count">${count}<span>${pct}%</span></div>
                </div>
                <div class="group-state-lane-items">${items}</div>
                ${lane.has_more ? `<div class="group-state-more">Masih ada ${count - (lane.items || []).length} grup lagi. Perbesar limit untuk melihat lebih banyak.</div>` : ''}
            </section>`;
    }).join('');
}

async function muatGroupStateDashboard() {
    const summaryEl = document.getElementById('group-state-summary');
    const boardEl = document.getElementById('group-state-board');
    if (summaryEl) summaryEl.innerHTML = `<div class="loading"><span class="spinner"></span> Memuat ringkasan state...</div>`;
    if (boardEl) boardEl.innerHTML = `<div class="loading"><span class="spinner"></span> Memuat lane state grup...</div>`;
    try {
        const search = (document.getElementById('group-state-search') || {}).value || '';
        const focus = (document.getElementById('group-state-focus') || {}).value || '';
        const limit = parseInt((document.getElementById('group-state-limit') || {}).value || '20', 10) || 20;
        const includeArchived = !!((document.getElementById('group-state-include-archived') || {}).checked);
        const params = new URLSearchParams();
        if (search.trim()) params.set('search', search.trim());
        if (focus) params.set('focus_state', focus);
        params.set('limit_per_state', String(limit));
        if (includeArchived) params.set('include_archived', '1');

        const data = await _getV2(`/overview/group-states?${params.toString()}`);
        const totalVisible = data.summary?.total_visible_groups || 0;
        const counts = data.summary?.state_counts || {};
        const order = ['stabilization', 'eligible', 'queued', 'cooldown', 'hold', 'failed', 'recovery'];
        if (summaryEl) {
            summaryEl.innerHTML = order.map((key) => _renderStateSummaryCard(key, counts[key] || 0, totalVisible)).join('')
                + `<div class="state-summary-card neutral static"><div class="state-summary-head"><span class="state-summary-icon">📦</span><span class="state-summary-title">Total terlihat</span></div><div class="state-summary-count">${totalVisible}</div><div class="state-summary-foot">Dari total ${data.summary?.total_groups || totalVisible} grup</div></div>`;
        }
        _renderDiagnostics(data);
        _renderBottlenecks(data.bottlenecks || []);
        _renderSessionStrip(data);
        _renderStateBoard(data.lanes || {}, totalVisible);
    } catch (err) {
        if (summaryEl) summaryEl.innerHTML = '';
        const diagEl = document.getElementById('group-state-diagnostics'); if (diagEl) diagEl.innerHTML = '';
        if (boardEl) boardEl.innerHTML = `<div class="empty-state"><div class="icon">⚠️</div><p>${err.message || 'Gagal memuat dashboard state grup.'}</p></div>`;
        _renderBottlenecks([err.message || 'Gagal memuat dashboard state grup.']);
    }
}

function setGroupStateFocus(stateKey) {
    const focus = document.getElementById('group-state-focus');
    if (!focus) return;
    focus.value = stateKey || '';
    muatGroupStateDashboard();
}

async function jalankanOrchestratorManual() {
    try {
        await _postV2('/orchestrator/run', { trigger: 'dashboard_groupstates_manual' });
        await muatGroupStateDashboard();
    } catch (err) {
        alert(`Gagal menjalankan orchestrator: ${err.message}`);
    }
}

window.addEventListener('DOMContentLoaded', () => {
    const search = document.getElementById('group-state-search');
    const focus = document.getElementById('group-state-focus');
    const limit = document.getElementById('group-state-limit');
    const archived = document.getElementById('group-state-include-archived');
    if (search) search.addEventListener('input', () => {
        clearTimeout(window.__groupStateDebounce);
        window.__groupStateDebounce = setTimeout(() => {
            if (window.tabAktif === 'groupstates') muatGroupStateDashboard();
        }, 300);
    });
    [focus, limit, archived].forEach((el) => {
        if (el) el.addEventListener('change', () => {
            if (window.tabAktif === 'groupstates') muatGroupStateDashboard();
        });
    });
});
