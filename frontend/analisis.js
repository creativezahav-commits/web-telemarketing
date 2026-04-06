// ============================================================
// analisis.js v2 — Tabel dengan warna bergantian, filter, massal
// ============================================================

let _filterStatus = "semua";
let _filterAkun   = "semua";
let _dataAnalisis  = [];

async function muatAnalisis() {
    setLoading("tabel-analisis");
    try {
        _dataAnalisis = await _get("/grup/analisis");
        await _muatFilterAkun();
        renderTabelAnalisis();
    } catch {
        document.getElementById("tabel-analisis").innerHTML =
            `<div class="empty-state"><div class="icon">⚠️</div><p>Gagal muat data.</p></div>`;
    }
}

async function _muatFilterAkun() {
    try {
        const akun = await _get("/akun");
        const sel  = document.getElementById("filter-akun-analisis");
        if (!sel) return;
        sel.innerHTML = `<option value="semua">Semua Akun</option>` +
            akun.filter(a => a.online).map(a =>
                `<option value="${a.phone}">${a.nama||a.phone}</option>`
            ).join("");
    } catch {}
}

function renderTabelAnalisis() {
    const el = document.getElementById("tabel-analisis");

    // Filter data
    let data = _dataAnalisis;
    if (_filterStatus !== "semua") {
        data = data.filter(g => g.label === _filterStatus);
    }

    // Filter pencarian
    const q = (document.getElementById("cari-analisis")?.value || "").trim().toLowerCase();
    if (q) {
        data = data.filter(g =>
            (g.nama || "").toLowerCase().includes(q) ||
            (g.username || "").toLowerCase().includes(q)
        );
    }

    if (!data.length) {
        el.innerHTML = `<div class="empty-state"><div class="icon">📊</div>
            <p>Tidak ada grup dengan filter ini.</p></div>`;
        return;
    }

    el.innerHTML = `
        <div style="margin-bottom:8px;font-size:13px;color:#888">
            Menampilkan <strong>${data.length}</strong> dari ${_dataAnalisis.length} grup
        </div>
        <div style="overflow-x:auto">
        <table class="tabel-analisis">
            <thead>
                <tr>
                    <th style="width:32px">
                        <input type="checkbox" id="centang-semua-analisis"
                            onchange="centangSemuaAnalisis(this)">
                    </th>
                    <th>Nama Grup</th>
                    <th>Member</th>
                    <th>Akun Join</th>
                    <th>Aktivitas</th>
                    <th>Total Kirim</th>
                    <th>Score</th>
                    <th>Status</th>
                    <th>Aksi</th>
                </tr>
            </thead>
            <tbody>
                ${data.map((g, i) => {
                    const labelWarna = {Hot:"#ef4444",Normal:"#2563eb",Skip:"#94a3b8"}[g.label]||"#888";
                    const labelBg    = {Hot:"#fee2e2",Normal:"#eff6ff",Skip:"#f1f5f9"}[g.label]||"#f8fafc";
                    const rowBg      = i % 2 === 0 ? "#ffffff" : "#f8fafc";

                    // Indikator aktif
                    const indikator = g.aktif_indikator;
                    const indDot    = indikator === "aktif" ? "🟢" :
                                     indikator === "sepi"   ? "🟡" :
                                     indikator === "tidak_aktif" ? "🔴" : "⚪";

                    return `<tr style="background:${rowBg}" class="analisis-row">
                        <td><input type="checkbox" class="analisis-checkbox" value="${g.id}"></td>
                        <td>
                            <div style="font-weight:600;font-size:13px">${g.nama}</div>
                            <div style="font-size:11px;color:#aaa">
                                ${g.username ? '@'+g.username : 'Private'} · ${g.tipe||''}
                            </div>
                        </td>
                        <td style="font-size:13px">${g.jumlah_member ? g.jumlah_member.toLocaleString() : '-'}</td>
                        <td>
                            <div id="akun-join-${g.id}" style="font-size:11px;color:#666">
                                <button class="btn-sm btn-outline" style="padding:2px 6px;font-size:11px"
                                    onclick="muatAkunJoin(${g.id})">Lihat</button>
                            </div>
                        </td>
                        <td style="font-size:11px;color:#666;min-width:170px">
                            <div><strong>Last chat:</strong> ${g.last_chat ? g.last_chat.slice(0,16) : '-'}</div>
                            <div><strong>Last kirim:</strong> ${g.last_kirim ? g.last_kirim.slice(0,16) : '-'}</div>
                            <div style="margin-top:4px">
                                <span class="badge ${g.send_eligible ? 'badge-aktif' : 'badge-warn'}">${g.send_guard_status || (g.send_eligible ? 'sendable' : 'hold')}</span>
                            </div>
                            <div style="margin-top:2px;color:#999">${g.send_guard_reason || '-'}</div>
                        </td>
                        <td style="font-size:13px;text-align:center">${g.total_kirim||0}x</td>
                        <td style="text-align:center">
                            <div style="display:flex;flex-direction:column;align-items:center;gap:3px">
                                <span style="font-weight:700;font-size:15px;color:#1e2a3a">${g.score}</span>
                                <div style="display:flex;gap:3px">
                                    <input type="number" min="0" max="100" value="${g.score}"
                                        style="width:46px;padding:2px 4px;font-size:11px;border:1px solid #d1d5db;border-radius:4px;text-align:center"
                                        id="score-input-${g.id}">
                                    <button class="btn-xs btn-outline" onclick="simpanScoreGrup(${g.id})" title="Simpan score">💾</button>
                                </div>
                            </div>
                        </td>
                        <td>
                            <div style="display:flex;align-items:center;gap:4px">
                                ${indDot}
                                <select style="padding:3px 6px;border-radius:6px;border:1px solid #ddd;
                                    background:${labelBg};color:${labelWarna};font-weight:600;font-size:11px"
                                    onchange="ubahLabelGrup(${g.id}, this.value)">
                                    <option value="Hot" ${g.label==='Hot'?'selected':''}>🔥 Hot</option>
                                    <option value="Normal" ${g.label==='Normal'?'selected':''}>✅ Normal</option>
                                    <option value="Skip" ${g.label==='Skip'?'selected':''}>⏭️ Skip</option>
                                </select>
                            </div>
                        </td>
                        <td>
                            <div style="display:flex;gap:4px">
                                <button class="btn-sm btn-outline" style="padding:3px 6px;font-size:11px"
                                    onclick="fetchLastChat(${g.id})" title="Fetch last chat">↻</button>
                            </div>
                        </td>
                    </tr>`;
                }).join("")}
            </tbody>
        </table>
        </div>
        <div id="aksi-massal" style="display:none;margin-top:12px;padding:12px;
            background:#eff6ff;border-radius:8px;display:flex;gap:8px;align-items:center">
            <span id="jumlah-dipilih-analisis" style="font-size:13px;font-weight:600;color:#2563eb"></span>
            <button class="btn-sm btn-success" onclick="ubahStatusMassal('Hot')">🔥 Jadikan Hot</button>
            <button class="btn-sm btn-outline" onclick="ubahStatusMassal('Normal')">✅ Jadikan Normal</button>
            <button class="btn-sm btn-danger" onclick="ubahStatusMassal('Skip')">⏭️ Jadikan Skip</button>
        </div>`;

    // Monitor checkbox untuk aksi massal
    document.querySelectorAll(".analisis-checkbox").forEach(cb => {
        cb.addEventListener("change", updateAksiMassal);
    });
}

function filterAnalisis(status) {
    _filterStatus = status;
    // Update tombol aktif
    document.querySelectorAll(".filter-btn-analisis").forEach(b => b.classList.remove("aktif-filter"));
    document.getElementById(`filter-${status}`).classList.add("aktif-filter");
    renderTabelAnalisis();
}

function centangSemuaAnalisis(cb) {
    document.querySelectorAll(".analisis-checkbox").forEach(c => c.checked = cb.checked);
    updateAksiMassal();
}

function updateAksiMassal() {
    const dipilih = document.querySelectorAll(".analisis-checkbox:checked").length;
    const el      = document.getElementById("aksi-massal");
    const jEl     = document.getElementById("jumlah-dipilih-analisis");
    if (el) el.style.display = dipilih > 0 ? "flex" : "none";
    if (jEl) jEl.textContent = `${dipilih} grup dipilih`;
}

async function ubahStatusMassal(label) {
    const ids = [...document.querySelectorAll(".analisis-checkbox:checked")]
        .map(cb => parseInt(cb.value));
    if (!ids.length) return;

    const scoreMap = {Hot:75, Normal:50, Skip:20};
    for (const id of ids) {
        await _post(`/grup/${id}/score/manual`, { score: scoreMap[label] });
    }
    await _post("/grup/status/massal", { grup_ids: ids, status: "active" });
    muatAnalisis();
}

async function muatAkunJoin(grupId) {
    const el = document.getElementById(`akun-join-${grupId}`);
    try {
        const data = await _get(`/grup/${grupId}/akun`);
        if (!data.length) { el.innerHTML = `<span style="color:#aaa">-</span>`; return; }
        el.innerHTML = data.map(a =>
            `<span style="background:#eff6ff;color:#2563eb;font-size:10px;
             padding:1px 5px;border-radius:10px;margin:1px;display:inline-block">
             ${a.nama||a.phone.slice(-8)}</span>`
        ).join("");
    } catch { el.innerHTML = `<span style="color:#aaa">-</span>`; }
}

async function fetchLastChat(grupId) {
    try {
        const r = await _post(`/grup/${grupId}/last-chat`, {});
        if (r.last_chat) muatAnalisis();
    } catch {}
}

async function simpanScoreGrup(grupId) {
    const score = parseInt(document.getElementById(`score-input-${grupId}`).value);
    await _post(`/grup/${grupId}/score/manual`, { score });
    muatAnalisis();
}

async function ubahLabelGrup(grupId, label) {
    const scoreMap = {Hot:75, Normal:50, Skip:20};
    await _post(`/grup/${grupId}/score/manual`, { score: scoreMap[label] });
    muatAnalisis();
}

async function updateSemuaScore() {
    await _post("/grup/score/update-semua", {});
    muatAnalisis();
}
