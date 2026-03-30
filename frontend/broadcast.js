// ============================================================
// broadcast.js
// Logika tab Broadcast — kirim ke banyak grup dengan jeda
// ============================================================

let _sesiAktif   = null;   // session_id yang sedang berjalan
let _intervalCek = null;   // interval untuk cek progress

// ── INISIALISASI TAB BROADCAST ────────────────────────────
async function muatTabBroadcast() {
    await _isiSelectAkunBroadcast();
    await muatListGrupBroadcast();
    muatRiwayatBroadcast();
}

// ── LOAD DAFTAR GRUP (dengan checkbox) ───────────────────
async function muatListGrupBroadcast() {
    const el = document.getElementById("broadcast-grup-list");
    el.innerHTML = `<div class="loading"><span class="spinner"></span> Memuat grup...</div>`;
    try {
        const data = await _get("/grup/aktif");
        if (!data.length) {
            el.innerHTML = `<div class="empty-state"><div class="icon">👥</div>
                <p>Belum ada grup. Fetch grup dulu di tab Grup.</p></div>`;
            return;
        }
        el.innerHTML = `
            <div class="broadcast-grup-header">
                <label style="font-size:13px;font-weight:600;cursor:pointer">
                    <input type="checkbox" id="centang-semua" onchange="centangSemuaGrup(this)">
                    Pilih Semua (${data.length} grup)
                </label>
                <span id="jumlah-dipilih" style="font-size:12px;color:#2563eb">0 dipilih</span>
            </div>
            <div class="broadcast-grup-scroll">
                ${data.map(g => `
                    <label class="broadcast-grup-item">
                        <input type="checkbox" class="grup-checkbox"
                            value="${g.id}" data-nama="${g.nama}"
                            onchange="updateJumlahDipilih()">
                        <div class="grup-item-info">
                            <span class="grup-item-nama">${g.nama}</span>
                            <span class="grup-item-meta">${g.tipe} · ${g.jumlah_member||'?'} member</span>
                        </div>
                    </label>`).join("")}
            </div>`;
    } catch {
        el.innerHTML = `<div class="empty-state"><div class="icon">⚠️</div><p>Gagal muat grup.</p></div>`;
    }
}

function centangSemuaGrup(cb) {
    document.querySelectorAll(".grup-checkbox").forEach(c => c.checked = cb.checked);
    updateJumlahDipilih();
}

function updateJumlahDipilih() {
    const dipilih = document.querySelectorAll(".grup-checkbox:checked").length;
    document.getElementById("jumlah-dipilih").textContent = `${dipilih} dipilih`;

    // Warning kalau terlalu banyak
    const warn = document.getElementById("broadcast-warn");
    if (dipilih > 30) {
        warn.textContent = "⚠️ Maksimal 30 grup per sesi.";
        warn.style.display = "block";
    } else {
        warn.style.display = "none";
    }
}

// ── MULAI BROADCAST ───────────────────────────────────────
async function mulaiBroadcast() {
    const phone = document.getElementById("broadcast-akun").value;
    const pesan = document.getElementById("broadcast-pesan").value.trim();
    const jeda  = parseInt(document.getElementById("broadcast-jeda").value) || 30;

    // Kumpulkan grup yang dicentang
    const grup_list = [];
    document.querySelectorAll(".grup-checkbox:checked").forEach(cb => {
        grup_list.push({
            id  : parseInt(cb.value),
            nama: cb.dataset.nama
        });
    });

    // Validasi
    if (!phone)          { tampilPesan("broadcast-status-msg","Pilih akun dulu.","gagal"); return; }
    if (!pesan)          { tampilPesan("broadcast-status-msg","Isi pesan wajib.","gagal"); return; }
    if (!grup_list.length) { tampilPesan("broadcast-status-msg","Pilih minimal 1 grup.","gagal"); return; }
    if (grup_list.length > 30) { tampilPesan("broadcast-status-msg","Maksimal 30 grup.","gagal"); return; }
    if (jeda < 10)       { tampilPesan("broadcast-status-msg","Jeda minimal 10 detik.","gagal"); return; }

    tampilPesan("broadcast-status-msg",`Memulai broadcast ke ${grup_list.length} grup...`,"info");

    try {
        const data = await _post("/broadcast/mulai", { phone, pesan, jeda, grup_list });

        if (data.error) {
            tampilPesan("broadcast-status-msg", data.error, "gagal");
            return;
        }

        _sesiAktif = data.session_id;

        // Tampilkan panel progress
        document.getElementById("broadcast-form-panel").style.display  = "none";
        document.getElementById("broadcast-progress-panel").style.display = "block";

        // Mulai polling progress tiap 2 detik
        _intervalCek = setInterval(() => cekProgress(_sesiAktif), 2000);
        cekProgress(_sesiAktif);

    } catch {
        tampilPesan("broadcast-status-msg","Gagal memulai. Cek backend.","gagal");
    }
}

// ── CEK PROGRESS ─────────────────────────────────────────
async function cekProgress(session_id) {
    try {
        const sesi = await _get(`/broadcast/status/${session_id}`);
        tampilProgress(sesi);

        // Kalau sudah selesai/dihentikan → stop polling
        if (sesi.status === "selesai" || sesi.status === "dihentikan") {
            clearInterval(_intervalCek);
            _intervalCek = null;
            muatRiwayatBroadcast();
        }
    } catch {}
}

function tampilProgress(sesi) {
    const persen = sesi.total > 0
        ? Math.round((sesi.selesai / sesi.total) * 100)
        : 0;

    // Status badge
    const statusWarna = {
        berjalan  : "#2563eb",
        selesai   : "#16a34a",
        dihentikan: "#dc2626",
        menunggu  : "#888"
    }[sesi.status] || "#888";

    document.getElementById("broadcast-progress-info").innerHTML = `
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
            <span style="font-weight:600;font-size:15px">
                ${sesi.selesai} / ${sesi.total} grup
            </span>
            <span style="background:${statusWarna};color:#fff;padding:3px 10px;
                border-radius:20px;font-size:12px;font-weight:600">
                ${sesi.status}
            </span>
        </div>
        <div class="limit-bar-track" style="margin-bottom:8px">
            <div class="limit-bar-fill ok" style="width:${persen}%;transition:width 0.5s"></div>
        </div>
        <div style="font-size:12px;color:#888">
            ${persen}% selesai
            ${sesi.countdown > 0 ? ` · Jeda ${sesi.countdown} detik...` : ''}
        </div>`;

    // Log per grup
    const logEl = document.getElementById("broadcast-log");
    if (sesi.hasil && sesi.hasil.length) {
        logEl.innerHTML = sesi.hasil.map(h => {
            const icon = {
                berhasil  : "✅",
                gagal     : "❌",
                mengirim  : "⏳",
                dihentikan: "⏹️",
                menunggu  : "○"
            }[h.status] || "○";
            const warna = {
                berhasil: "#dcfce7",
                gagal   : "#fee2e2",
                mengirim: "#eff6ff"
            }[h.status] || "#f8fafc";
            return `
                <div style="display:flex;justify-content:space-between;align-items:center;
                    padding:8px 12px;border-radius:8px;margin-bottom:6px;background:${warna}">
                    <span style="font-size:13px">${icon} ${h.nama_grup}</span>
                    <span style="font-size:11px;color:#888">${h.waktu||''}</span>
                </div>`;
        }).join("");
    }
}

// ── STOP BROADCAST ────────────────────────────────────────
async function stopBroadcast() {
    if (!_sesiAktif) return;
    if (!confirm("Yakin ingin menghentikan broadcast?")) return;

    await _post(`/broadcast/stop/${_sesiAktif}`, {});
    tampilPesan("broadcast-status-msg","Menghentikan...","info");
}

function kembaliKeForm() {
    document.getElementById("broadcast-form-panel").style.display    = "block";
    document.getElementById("broadcast-progress-panel").style.display = "none";
    if (_intervalCek) { clearInterval(_intervalCek); _intervalCek = null; }
    _sesiAktif = null;
    // Reset form
    document.querySelectorAll(".grup-checkbox").forEach(c => c.checked = false);
    document.getElementById("centang-semua").checked = false;
    updateJumlahDipilih();
    document.getElementById("broadcast-pesan").value = "";
}

// ── RIWAYAT BROADCAST ────────────────────────────────────
async function muatRiwayatBroadcast() {
    try {
        const data = await _get("/broadcast/semua");
        const el   = document.getElementById("broadcast-riwayat");
        if (!data.length) {
            el.innerHTML = `<p style="font-size:13px;color:#aaa">Belum ada riwayat broadcast.</p>`;
            return;
        }
        el.innerHTML = data.map(s => {
            const warna = {selesai:"#dcfce7",dihentikan:"#fee2e2",berjalan:"#eff6ff"}[s.status]||"#f8fafc";
            return `
                <div style="background:${warna};border-radius:8px;padding:10px 14px;
                    margin-bottom:8px;display:flex;justify-content:space-between;align-items:center">
                    <div>
                        <strong style="font-size:13px">${s.phone}</strong>
                        <div style="font-size:11px;color:#888">${s.mulai} · ${s.selesai}/${s.total} grup</div>
                    </div>
                    <div style="display:flex;gap:6px;align-items:center">
                        <span style="font-size:11px;font-weight:600">${s.status}</span>
                        <button class="btn-danger btn-sm"
                            onclick="hapusSesiBroadcast('${s.session_id}')">🗑️</button>
                    </div>
                </div>`;
        }).join("");
    } catch {}
}

async function hapusSesiBroadcast(session_id) {
    await _del(`/broadcast/hapus/${session_id}`);
    muatRiwayatBroadcast();
}

// ── SELECT AKUN ───────────────────────────────────────────
async function _isiSelectAkunBroadcast() {
    try {
        const data  = await _get("/akun");
        const aktif = data.filter(a => a.online);
        const sel   = document.getElementById("broadcast-akun");
        sel.innerHTML = aktif.length
            ? aktif.map(a=>`<option value="${a.phone}">${a.nama} (${a.phone})</option>`).join("")
            : `<option value="">-- Tidak ada akun online --</option>`;
    } catch {}
}