// ============================================================
// broadcast.js — Tab Broadcast
// Fix: field mismatch grup_per_akun, muatListGrupBroadcast,
//      jumlah-dipilih-broadcast, feedback yang jelas
// ============================================================

let _sesiAktif   = null;
let _intervalCek = null;

// ── Load tab ─────────────────────────────────────────────
async function muatTabBroadcast() {
    await muatInfoAkunBroadcast();
    await muatRiwayatBroadcast();
    await syncPesanAktif();
    _muatBroadcastThrottleInfo();
}

async function _muatBroadcastThrottleInfo() {
    const el = document.getElementById('broadcast-throttle-info');
    if (!el) return;
    try {
        const throttle = await _getV2('/broadcast-queue/throttle-status');
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
    } catch { el.innerHTML = ''; }
}

// Alias — tombol Refresh di HTML memanggil ini
function muatListGrupBroadcast() {
    muatInfoAkunBroadcast();
}

// ── Muat akun + grup per akun ─────────────────────────────
async function muatInfoAkunBroadcast() {
    const el = document.getElementById('broadcast-akun-info');
    el.innerHTML = `<div class="loading"><span class="spinner"></span> Memuat akun dan grup...</div>`;

    try {
        const akun   = await _get('/akun');
        const online = akun.filter(a => a.online);

        if (!online.length) {
            el.innerHTML = `
                <div style="background:#fee2e2;color:#991b1b;padding:12px 16px;border-radius:8px;font-size:13px">
                    ❌ Tidak ada akun yang sedang online. Login akun dulu di tab 👤 Akun.
                </div>`;
            return;
        }

        let html = `<div class="abp-grid">`;

        for (const a of online) {
            const grup    = await _get(`/grup/by-akun/${a.phone}`);
            const hot     = grup.filter(g => g.label === 'Hot').length;
            const norm    = grup.filter(g => g.label === 'Normal').length;
            const phoneId = a.phone.replace(/\+/g, '');

            html += `
                <div class="abp-kolom">
                    <div class="abp-header">
                        <div style="display:flex;align-items:center;gap:6px;min-width:0">
                            <div class="abp-dot"></div>
                            <div style="min-width:0">
                                <div style="font-weight:700;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">
                                    ${a.nama || 'Akun'}
                                </div>
                                <div style="font-size:10px;color:#888">${a.phone}</div>
                            </div>
                        </div>
                        <div style="display:flex;gap:4px;align-items:center;flex-shrink:0">
                            <span style="font-size:10px;background:#fee2e2;color:#ef4444;padding:2px 6px;border-radius:10px">🔥${hot}</span>
                            <span style="font-size:10px;background:#eff6ff;color:#2563eb;padding:2px 6px;border-radius:10px">✅${norm}</span>
                            <label style="font-size:10px;cursor:pointer;display:flex;align-items:center;gap:3px;white-space:nowrap">
                                <input type="checkbox" class="centang-semua-akun" data-phone="${a.phone}"
                                    onchange="centangSemuaGrupAkun(this,'${a.phone}')"> Semua
                            </label>
                        </div>
                    </div>
                    <div class="abp-grup-scroll" id="grup-list-${phoneId}">
                        ${!grup.length
                            ? `<div style="font-size:11px;color:#aaa;padding:8px">Belum ada grup. Lakukan Discovery atau Fetch dulu.</div>`
                            : grup.map(g => {
                                const warna = { Hot: '#ef4444', Normal: '#2563eb', Skip: '#94a3b8' }[g.label] || '#888';
                                const icon  = { Hot: '🔥', Normal: '✅', Skip: '⏭️' }[g.label] || '';
                                const nama  = (g.nama || '').replace(/"/g, '&quot;');
                                return `
                                <label class="abp-item">
                                    <input type="checkbox" class="grup-checkbox"
                                        value="${g.id}"
                                        data-nama="${nama}"
                                        data-phone="${a.phone}"
                                        onchange="updateJumlahDipilih()">
                                    <div style="min-width:0">
                                        <div style="font-size:12px;font-weight:600;white-space:nowrap;
                                            overflow:hidden;text-overflow:ellipsis">${g.nama}</div>
                                        <div style="font-size:10px;color:#aaa">
                                            ${g.jumlah_member ? Number(g.jumlah_member).toLocaleString() : '?'} member
                                            <span style="color:${warna};margin-left:3px">${icon}</span>
                                        </div>
                                    </div>
                                </label>`;
                            }).join('')}
                    </div>
                </div>`;
        }

        html += `</div>`;
        el.innerHTML = html;
        updateJumlahDipilih();

    } catch (e) {
        el.innerHTML = `<div style="color:#aaa;font-size:13px">❌ Gagal memuat: ${e.message || e}</div>`;
    }
}

// ── Helper centang ────────────────────────────────────────
function centangSemuaGrupAkun(cb, phone) {
    const phoneId = phone.replace(/\+/g, '');
    const el = document.getElementById(`grup-list-${phoneId}`);
    if (el) el.querySelectorAll('.grup-checkbox').forEach(c => c.checked = cb.checked);
    updateJumlahDipilih();
}

function pilihSemuaGrupBroadcast(centang) {
    document.querySelectorAll('.grup-checkbox').forEach(c => c.checked = centang);
    document.querySelectorAll('.centang-semua-akun').forEach(c => c.checked = centang);
    updateJumlahDipilih();
}

function updateJumlahDipilih() {
    const dipilih = document.querySelectorAll('.grup-checkbox:checked').length;
    const total   = document.querySelectorAll('.grup-checkbox').length;
    const el = document.getElementById('jumlah-dipilih-broadcast');
    if (el) {
        el.textContent = dipilih > 0
            ? `${dipilih} dari ${total} grup dipilih`
            : `${total} grup tersedia — belum ada yang dipilih`;
        el.style.color = dipilih > 0 ? '#16a34a' : '#888';
    }
}

// ── Mulai broadcast ───────────────────────────────────────
async function mulaiBroadcast() {
    const pesan = (document.getElementById('broadcast-pesan').value || '').trim();
    const jeda  = parseInt(document.getElementById('broadcast-jeda').value) || 30;

    if (!pesan) {
        tampilPesan('broadcast-status-msg', '⚠️ Isi pesan wajib diisi sebelum broadcast.', 'gagal');
        document.getElementById('broadcast-pesan').focus();
        return;
    }

    // Kumpulkan grup per akun
    const grup_per_akun = {};
    document.querySelectorAll('.grup-checkbox:checked').forEach(cb => {
        const phone = cb.dataset.phone;
        if (!grup_per_akun[phone]) grup_per_akun[phone] = [];
        grup_per_akun[phone].push({
            id:   parseInt(cb.value),
            nama: cb.dataset.nama || String(cb.value)
        });
    });

    const totalDipilih = Object.values(grup_per_akun).reduce((s, arr) => s + arr.length, 0);
    if (totalDipilih === 0) {
        tampilPesan('broadcast-status-msg', '⚠️ Pilih minimal 1 grup tujuan terlebih dahulu.', 'gagal');
        return;
    }

    // grup_list flat untuk kompatibilitas backend lama
    const grup_list = Object.values(grup_per_akun).flat();

    tampilPesan('broadcast-status-msg', `⏳ Memulai broadcast ke ${totalDipilih} grup...`, 'info');

    try {
        const r = await _post('/broadcast/mulai', {
            pesan,
            jeda,
            grup_list,      // backend app.py baca field ini
            grup_per_akun,  // broadcast_session.py baca ini untuk mode per-akun paralel
        });

        if (r.error) {
            tampilPesan('broadcast-status-msg', `❌ ${r.error}`, 'gagal');
            return;
        }

        _sesiAktif = r.session_id;
        document.getElementById('broadcast-form-panel').style.display     = 'none';
        document.getElementById('broadcast-progress-panel').style.display = 'block';
        _intervalCek = setInterval(() => cekProgress(_sesiAktif), 1500);
        cekProgress(_sesiAktif);

    } catch (e) {
        tampilPesan('broadcast-status-msg', `❌ Gagal: ${e.message || e}`, 'gagal');
    }
}

// ── Cek progress ──────────────────────────────────────────
async function cekProgress(sid) {
    try {
        const sesi = await _get(`/broadcast/status/${sid}`);
        tampilProgress(sesi);
        if (sesi.status === 'selesai' || sesi.status === 'dihentikan') {
            clearInterval(_intervalCek);
            _intervalCek = null;
            muatRiwayatBroadcast();
        }
    } catch { /* server sedang sibuk, abaikan */ }
}

function tampilProgress(sesi) {
    const persen = sesi.total > 0 ? Math.round((sesi.selesai / sesi.total) * 100) : 0;
    const warnaStatus = {
        berjalan: '#2563eb', selesai: '#16a34a',
        dihentikan: '#dc2626', menunggu: '#888',
    }[sesi.status] || '#888';

    document.getElementById('broadcast-progress-info').innerHTML = `
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
            <span style="font-size:20px;font-weight:700">${sesi.selesai} / ${sesi.total} grup</span>
            <span style="background:${warnaStatus};color:#fff;padding:4px 12px;border-radius:20px;font-size:12px;font-weight:600">
                ${sesi.status}
            </span>
        </div>
        <div style="background:#f0f0f0;border-radius:6px;height:10px;margin-bottom:8px;overflow:hidden">
            <div style="background:#2563eb;width:${persen}%;height:10px;border-radius:6px;transition:width 0.3s"></div>
        </div>
        <div style="font-size:12px;color:#888">
            ${persen}% selesai
            ${sesi.countdown > 0 ? `· ⏳ Jeda ${sesi.countdown}s` : ''}
        </div>`;

    const logEl = document.getElementById('broadcast-log');
    if (sesi.hasil && sesi.hasil.length) {
        const perAkun = {};
        sesi.hasil.forEach(h => {
            if (!perAkun[h.phone]) perAkun[h.phone] = [];
            perAkun[h.phone].push(h);
        });
        logEl.innerHTML = `
            <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:10px">
                ${Object.entries(perAkun).map(([phone, items]) => `
                <div style="border:1px solid #e5e7eb;border-radius:8px;overflow:hidden">
                    <div style="background:#f8fafc;padding:6px 10px;font-size:11px;font-weight:700;color:#2563eb">
                        📱 ${phone}
                    </div>
                    <div style="max-height:220px;overflow-y:auto">
                        ${items.map(h => {
                            const icon = { berhasil:'✅', gagal:'❌', mengirim:'⏳', skip:'⏭️' }[h.status] || '○';
                            const bg   = { berhasil:'#f0fdf4', gagal:'#fff5f5', mengirim:'#eff6ff' }[h.status] || '#fff';
                            return `<div style="padding:5px 10px;font-size:11px;background:${bg};
                                border-bottom:1px solid #f0f0f0;display:flex;justify-content:space-between;gap:6px">
                                <span style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis">
                                    ${icon} ${h.nama_grup || h.grup_id}
                                </span>
                                <span style="color:#aaa;flex-shrink:0">${h.waktu || ''}</span>
                            </div>`;
                        }).join('')}
                    </div>
                </div>`).join('')}
            </div>`;
    } else {
        logEl.innerHTML = `<div style="color:#aaa;font-size:13px;padding:8px">Menunggu hasil pengiriman pertama...</div>`;
    }
}

// ── Kontrol sesi ──────────────────────────────────────────
async function stopBroadcast() {
    if (!_sesiAktif) return;
    if (!confirm('Hentikan broadcast yang sedang berjalan?')) return;
    try {
        await _post(`/broadcast/stop/${_sesiAktif}`, {});
    } catch (e) {
        alert('Gagal stop: ' + e.message);
    }
}

function kembaliKeForm() {
    document.getElementById('broadcast-form-panel').style.display     = 'block';
    document.getElementById('broadcast-progress-panel').style.display = 'none';
    if (_intervalCek) { clearInterval(_intervalCek); _intervalCek = null; }
    _sesiAktif = null;
    document.getElementById('broadcast-pesan').value = '';
    tampilPesan('broadcast-status-msg', '', 'info');
    muatInfoAkunBroadcast();
}

// ── Riwayat ───────────────────────────────────────────────
async function muatRiwayatBroadcast() {
    try {
        const data = await _get('/broadcast/semua');
        const el   = document.getElementById('broadcast-riwayat');
        if (!data.length) {
            el.innerHTML = `<div style="font-size:13px;color:#aaa">Belum ada riwayat broadcast.</div>`;
            return;
        }
        el.innerHTML = data.map(s => {
            const bg   = { selesai:'#f0fdf4', dihentikan:'#fff5f5', berjalan:'#eff6ff' }[s.status] || '#f8fafc';
            const icon = { selesai:'✅', dihentikan:'⏹️', berjalan:'⏳' }[s.status] || '○';
            return `
            <div style="background:${bg};border-radius:8px;padding:10px 14px;margin-bottom:8px;
                display:flex;justify-content:space-between;align-items:center;gap:10px">
                <div>
                    <div style="font-size:13px;font-weight:600">
                        ${icon} ${s.selesai}/${s.total} grup &middot; <span style="font-weight:400">${s.status}</span>
                    </div>
                    <div style="font-size:11px;color:#888;margin-top:2px">${s.mulai}</div>
                </div>
                <button class="btn-danger btn-sm" onclick="hapusSesiBroadcast('${s.session_id}')">🗑️ Hapus</button>
            </div>`;
        }).join('');
    } catch {
        document.getElementById('broadcast-riwayat').innerHTML =
            `<div style="font-size:13px;color:#aaa">Gagal muat riwayat.</div>`;
    }
}

async function hapusSesiBroadcast(sid) {
    if (!confirm('Hapus riwayat sesi ini?')) return;
    try {
        await _del(`/broadcast/hapus/${sid}`);
        muatRiwayatBroadcast();
    } catch (e) {
        alert('Gagal hapus: ' + e.message);
    }
}

// ── Sync draft aktif ke textarea ──────────────────────────
async function syncPesanAktif() {
    try {
        const draft     = await _get('/draft/aktif');
        const indikator = document.getElementById('indikator-pesan-broadcast');
        const textarea  = document.getElementById('broadcast-pesan');
        if (draft && draft.isi) {
            if (indikator) indikator.innerHTML =
                `<span style="color:#16a34a;font-size:12px">✅ Draft aktif: <strong>${draft.judul}</strong></span>`;
            if (textarea && !textarea.value.trim()) {
                textarea.value = draft.isi;
            }
        } else {
            if (indikator) indikator.innerHTML =
                `<span style="color:#888;font-size:12px">Tidak ada draft aktif — tulis pesan langsung di bawah.</span>`;
        }
    } catch { /* abaikan */ }
}
