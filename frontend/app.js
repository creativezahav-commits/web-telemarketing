const API = window.location.protocol.startsWith("http") ? `${window.location.origin}/api` : "http://127.0.0.1:5000/api";
const API_V2 = window.location.protocol.startsWith("http") ? `${window.location.origin}/api/v2` : "http://127.0.0.1:5000/api/v2";

const TAB_META = {
    akun:["Manajemen Akun","Login, pantau, dan rawat akun Telegram yang dipakai sistem."],
    analisis:["Analisis Grup","Lihat kualitas grup, score, dan indikator yang dipakai sender."],
    discovery:["Discovery Grup","Ambil grup dari akun aktif atau tambahkan grup manual ke sistem."],
    scraper:["Scraper Keyword","Generate keyword otomatis, jalankan job scraper, dan impor hasil terbaik."],
    draft:["Draft Pesan","Simpan template pesan agar bisa dipakai ulang di kirim, antrian, dan broadcast."],
    antrian:["Antrian Pengiriman","Kelola pesan yang menunggu dikirim satu per satu."],
    kirim:["Kirim Manual","Kirim pesan langsung ke grup aktif dengan akun yang tersedia."],
    broadcast:["Broadcast Session","Sebar pesan ke banyak grup dengan kontrol sesi."],
    sync:["Sinkronisasi Grup","Join grup pilihan dan sinkronkan relasi akun-grup secara bertahap."],
    permissions:["Permissions Grup","Kelola izin broadcast untuk setiap grup — basis, persetujuan, masa berlaku."],
    assignments:["Assignments Grup","Atur kepemilikan akun per grup, auto-assign, dan reassign yang gagal."],
    campaigns:["Campaign Manager","Buat dan pantau campaign pengiriman massal beserta broadcast queue-nya."],
    automation:["Automation Rules","Buat aturan otomatis untuk permission, assignment, delivery, dan recovery."],
    groupstates:["State Grup Pipeline","Pantau grup per tahap broadcast: stabilization, eligible, queued, cooldown, hold, failed, dan recovery."],
    recovery:["Recovery Center","Lihat dan pulihkan entitas yang macet atau bermasalah di sistem."],
    riwayat:["Riwayat Hari Ini","Pantau hasil kirim, error, dan ringkasan aktivitas hari ini."],
    settings:["Pengaturan Umum & Lanjutan","Atur batas akun, scoring, dan pengaturan lanjutan yang tidak ada di pop-up card otomasi."],
};

function tampilTab(nama) {
    document.querySelectorAll(".tab").forEach(t => t.style.display = "none");
    document.querySelectorAll(".sidebar nav a").forEach(a => a.classList.remove("aktif-menu"));
    document.getElementById(`tab-${nama}`).style.display = "block";
    const m = document.getElementById(`menu-${nama}`);
    if (m) m.classList.add("aktif-menu");
    const meta = TAB_META[nama] || ["Telegram Dashboard", "Panel kontrol otomatisasi Telegram."];
    const titleEl = document.getElementById("app-title");
    const subEl = document.getElementById("app-subtitle");
    if (titleEl) titleEl.textContent = meta[0];
    if (subEl) subEl.textContent = meta[1];
    window.tabAktif = nama;
    const fn = {
        akun:"muatAkun", analisis:"muatAnalisis", discovery:"muatTabDiscovery", scraper:"muatTabScraper",
        draft:"muatTabDraft", antrian:"muatAntrian", kirim:"muatTabKirim",
        broadcast:"muatTabBroadcast", sync:"muatTabSync",
        permissions:"muatPermissions", assignments:"muatAssignments",
        campaigns:"muatCampaigns", automation:"muatAutomation", groupstates:"muatGroupStateDashboard", recovery:"muatRecovery",
        riwayat:"muatRiwayat", settings:"muatTabSettings", diagnosa:"muatDiagnosa"
    };
    if (fn[nama]) window[fn[nama]]();
}

function tampilPesan(id, teks, tipe="info") {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = teks;
    el.className = `pesan-status ${tipe}`;
}
function setLoading(id, teks="Memuat...") {
    const el = document.getElementById(id);
    if (el) el.innerHTML = `<div class="loading"><span class="spinner"></span> ${teks}</div>`;
}

async function _readResponse(r) {
    const text = await r.text();
    let data = {};
    try { data = text ? JSON.parse(text) : {}; } catch { data = { raw: text }; }
    if (!r.ok) {
        const message = data.error || data.pesan || data.message || `HTTP ${r.status}`;
        throw new Error(message);
    }
    return data;
}
async function _get(path) {
    try {
        const r = await fetch(`${API}${path}`);
        return _readResponse(r);
    } catch (err) {
        throw new Error(err?.message || 'Backend tidak bisa dihubungi');
    }
}
async function _post(path, body) {
    try {
        const r = await fetch(`${API}${path}`, {
            method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)
        });
        return _readResponse(r);
    } catch (err) {
        throw new Error(err?.message || 'Backend tidak bisa dihubungi');
    }
}
async function _del(path) {
    try {
        const r = await fetch(`${API}${path}`, {method:"DELETE"});
        return _readResponse(r);
    } catch (err) {
        throw new Error(err?.message || 'Backend tidak bisa dihubungi');
    }
}

function _spamBadge(a) {
    const level = a.spam_level || 'tidak_diketahui';
    const label = a.spam_label || '⚪ ?';
    const ket = a.spam_keterangan || '';
    if (level === 'curiga') {
        return `<span class="mini-pill danger" title="${ket}">${label}</span>`;
    } else if (level === 'waspada') {
        return `<span class="mini-pill warn" title="${ket}">${label}</span>`;
    } else if (level === 'sehat') {
        return '';  // tidak perlu badge kalau sehat
    }
    return '';
}

function _spamDetail(a) {
    if (!a.spam_level || a.spam_level === 'sehat' || a.spam_level === 'tidak_diketahui') return '';
    return `<div class="spam-indicator-bar spam-${a.spam_level}">
        <span class="spam-icon">${a.spam_label}</span>
        <span class="spam-detail">${a.spam_keterangan || ''}</span>
        ${a.cooldown_aktif ? `<span class="spam-cooldown">⏱ Cooldown s/d ${a.cooldown_until}</span>` : ''}
    </div>`;
}

function buatLimitBar(sudah, batas) {
    const persen = batas > 0 ? Math.min(100, Math.round((sudah/batas)*100)) : 0;
    const sisa   = batas - sudah;
    const kelas  = persen < 60 ? "ok" : persen < 90 ? "warn" : "full";
    return `<div class="limit-bar-wrapper">
        <div class="limit-bar-label">
            <span>${sudah}/${batas} pesan hari ini</span>
            <span class="sisa-${kelas}">sisa ${sisa}</span>
        </div>
        <div class="limit-bar-track">
            <div class="limit-bar-fill ${kelas}" style="width:${persen}%"></div>
        </div>
    </div>`;
}

async function muatSidebarSummary() {
    try {
        const r = await _get("/riwayat/ringkasan");
        const t = await _get("/akun/tersedia");
        document.getElementById("sidebar-summary").innerHTML = `
            <div class="sf-item"><span>✅ Terkirim</span><span class="sf-val">${r.berhasil}</span></div>
            <div class="sf-item"><span>❌ Gagal</span><span class="sf-val">${r.gagal}</span></div>
            <div class="sf-item"><span>🟢 Akun siap</span><span class="sf-val">${t.length}</span></div>`;
    } catch {}
}

// ── AKUN ──────────────────────────────────────────────────
const AKUN_ASSIGN_DEFAULTS = {
    auto_assign_enabled: true,
    priority_weight: 100,
    daily_new_group_cap: 10,
    manual_health_override_enabled: false,
    manual_health_override_score: 80,
    manual_warming_override_enabled: false,
    manual_warming_override_level: 2,
    fresh_login_grace_enabled: true,
    fresh_login_grace_minutes: 180,
    fresh_login_health_floor: 80,
    fresh_login_warming_floor: 2,
    assignment_notes: ''
};

let akunAssignmentDefaultsCache = null;

function _formatDateTimeText(value) {
    if (!value) return '-';
    const d = new Date(value.replace(' ', 'T'));
    if (Number.isNaN(d.getTime())) return value;
    return d.toLocaleString('id-ID', { dateStyle: 'medium', timeStyle: 'short' });
}

function _accountStatusClass(a) {
    const status = (a.status || '').toLowerCase();
    if (status === 'banned' || status === 'suspended') return 'offline';
    if (status === 'restricted') return 'offline';  // Diblokir moderator — tampilkan merah
    if (!a.online) return status === 'active' ? 'standby' : 'offline';
    // Online — cek kondisi lebih detail
    const sisaKirim = Math.max(0, (a.maks_kirim || 0) - (a.sudah_kirim || 0));
    if (sisaKirim <= 0 && (a.maks_kirim || 0) > 0) return 'full';   // Kuota habis
    if (sisaKirim <= 2 && (a.maks_kirim || 0) > 0) return 'warn';   // Hampir habis
    return 'online';
}

function _accountStatusText(a) {
    const status = (a.status || '').toLowerCase();
    if (status === 'banned') return 'Banned';
    if (status === 'suspended') return 'Suspended';
    if (status === 'restricted') return 'Diblokir Telegram';  // Label jelas di kartu
    if (!a.online) return status === 'active' ? 'Standby' : (a.status || 'Offline');
    const sisaKirim = Math.max(0, (a.maks_kirim || 0) - (a.sudah_kirim || 0));
    if (sisaKirim <= 0 && (a.maks_kirim || 0) > 0) return 'Limit';
    if (sisaKirim <= 2 && (a.maks_kirim || 0) > 0) return 'Hampir Limit';
    return 'Aktif';
}

function _renderAkunCard(a) {
    const sisaKirim  = Math.max(0, (a.maks_kirim || 0) - (a.sudah_kirim || 0));
    const sisaJoin   = Math.max(0, (a.maks_join  || 0) - (a.sudah_join  || 0));
    const pctKirim   = a.maks_kirim > 0 ? Math.min(100, Math.round(((a.sudah_kirim || 0) / a.maks_kirim) * 100)) : 0;
    const pctJoin    = a.maks_join  > 0 ? Math.min(100, Math.round(((a.sudah_join || 0)  / a.maks_join)  * 100)) : 0;
    const barKirim   = pctKirim >= 100 ? 'full' : pctKirim >= 80 ? 'warn' : 'ok';
    const barJoin    = pctJoin  >= 100 ? 'full' : pctJoin  >= 80 ? 'warn' : 'ok';
    const statusClass = _accountStatusClass(a);
    const statusText = _accountStatusText(a);
    const tanggal = a.tanggal_buat ? a.tanggal_buat.slice(0,10) : '-';
    return `
        <div class="akun-card-tile ${statusClass}">
            <div class="account-card-head">
                <div>
                    <div class="account-card-name-row">
                        <span class="account-card-name">${a.nama || 'Tanpa Nama'}</span>
                        <span class="badge badge-${(a.status === 'restricted' || a.status === 'banned') ? 'offline' : (a.online ? 'aktif' : 'offline')}">${a.status === 'restricted' ? 'Diblokir' : a.status === 'banned' ? 'Banned' : (a.online ? 'Online' : 'Offline')}</span>
                    </div>
                    <div class="account-card-sub">@${a.username || '-'} · ${a.phone}</div>
                </div>
                <div class="account-card-badges">
                    <span class="mini-pill ${statusClass}">${statusText}</span>
                    <span class="mini-pill neutral">${a.label_level || '🌱 Baru'}</span>
                    <span class="mini-pill neutral">Score ${a.score || 0}</span>
                    ${a.fresh_login_grace_enabled ? `<span class="mini-pill info">Grace login</span>` : ''}
                    ${a.manual_health_override_enabled || a.manual_warming_override_enabled ? `<span class="mini-pill purple">Override</span>` : ''}
                    ${_spamBadge(a)}
                </div>
            </div>

            <div class="account-card-meta-grid">
                <div class="account-metric"><span class="metric-label">Status akun</span><strong>${a.status || 'active'}</strong></div>
                <div class="account-metric"><span class="metric-label">Bergabung</span><strong>${tanggal}</strong></div>
                <div class="account-metric"><span class="metric-label">Umur akun</span><strong>${a.umur_hari || 0} hari</strong></div>
                <div class="account-metric"><span class="metric-label">Grup dipegang</span><strong>${a.jumlah_grup || 0}</strong></div>
                <div class="account-metric"><span class="metric-label">Level warming</span><strong>${a.level_warming || 1}</strong></div>
                <div class="account-metric"><span class="metric-label">Daily cap assign</span><strong>${a.daily_new_group_cap ?? 10}</strong></div>
            </div>

            <div class="account-progress-group">
                <div class="account-progress-item">
                    <div class="limit-bar-label"><span>📤 Kirim hari ini</span><span class="sisa-${barKirim}">${a.sudah_kirim || 0}/${a.maks_kirim || 0} · sisa ${sisaKirim}</span></div>
                    <div class="limit-bar-track"><div class="limit-bar-fill ${barKirim}" style="width:${pctKirim}%"></div></div>
                </div>
                <div class="account-progress-item">
                    <div class="limit-bar-label"><span>🔗 Join hari ini</span><span class="sisa-${barJoin}">${a.sudah_join || 0}/${a.maks_join || 0} · sisa ${sisaJoin}</span></div>
                    <div class="limit-bar-track"><div class="limit-bar-fill ${barJoin}" style="width:${pctJoin}%"></div></div>
                </div>
            </div>

            ${(() => {
                if (!a.next_join_at) return `
                    <div class="next-join-info next-join-ready">
                        <span class="next-join-icon">🟢</span>
                        <div class="next-join-detail">
                            <span class="next-join-label">Jadwal Join Berikutnya</span>
                            <span class="next-join-value">Siap join kapan saja</span>
                        </div>
                    </div>`;
                const nextDt = new Date(a.next_join_at);
                const now = new Date();
                const diffMs = nextDt - now;
                if (diffMs <= 0) return `
                    <div class="next-join-info next-join-ready">
                        <span class="next-join-icon">🟢</span>
                        <div class="next-join-detail">
                            <span class="next-join-label">Jadwal Join Berikutnya</span>
                            <span class="next-join-value">Siap join sekarang</span>
                        </div>
                    </div>`;
                const diffMenit = Math.floor(diffMs / 60000);
                const jam = Math.floor(diffMenit / 60);
                const menit = diffMenit % 60;
                const sisaStr = jam > 0 ? `${jam}j ${menit}m lagi` : `${menit} menit lagi`;
                const waktuStr = nextDt.toLocaleTimeString('id-ID', {hour:'2-digit', minute:'2-digit'});
                const tglStr = nextDt.toLocaleDateString('id-ID', {day:'numeric', month:'short'});
                return `
                    <div class="next-join-info next-join-waiting">
                        <span class="next-join-icon">⏳</span>
                        <div class="next-join-detail">
                            <span class="next-join-label">Jadwal Join Berikutnya</span>
                            <span class="next-join-value">${tglStr} pukul ${waktuStr} <em>(${sisaStr})</em></span>
                        </div>
                    </div>`;
            })()}

            ${_spamDetail(a)}
            <div class="account-config-snapshot">
                <div class="snapshot-title">Konfigurasi assign saat ini</div>
                <div class="snapshot-grid">
                    <div>Auto assign: <strong>${a.auto_assign_enabled ? 'Aktif' : 'Nonaktif'}</strong></div>
                    <div>Priority: <strong>${a.priority_weight ?? 100}</strong></div>
                    <div>Fresh grace: <strong>${a.fresh_login_grace_enabled ? `${a.fresh_login_grace_minutes ?? 180}m` : 'Off'}</strong></div>
                    <div>Override health: <strong>${a.manual_health_override_enabled ? (a.manual_health_override_score ?? 80) : 'Off'}</strong></div>
                </div>
            </div>

            <div class="account-card-actions">
                ${a.online
                    ? `<button class="btn-danger btn-sm" onclick="logoutAkun('${a.phone}')">🔌 Logout</button>`
                    : `<button class="btn-success btn-sm" onclick="loginUlang('${a.phone}')">🔗 Login</button>`
                }
                <button class="btn-outline btn-sm" onclick="bukaKonfigAkun('${a.phone}')" title="Konfigurasi assign per akun">⚙️ Assign</button>
                <button class="btn-outline btn-sm" onclick="hitungScoreAkun('${a.phone}')" title="Hitung ulang score">🧮 Score</button>
                ${a.status !== 'active' ? `<button class="btn-outline btn-sm" onclick="pulihkanAkun('${a.phone}')">♻️ Pulihkan</button>` : ''}
                ${a.next_join_at ? `<button class="btn-outline btn-sm btn-reset-throttle" onclick="resetJoinThrottle('${a.phone}')" title="Reset throttle join">⚡ Reset Join</button>` : ''}
                <button class="btn-danger btn-sm" onclick="hapusAkunPermanen('${a.phone}')" title="Hapus akun dari database dan session lokal">🗑️ Hapus</button>
            </div>
        </div>`;
}

async function _getAkunAssignmentDefaults() {
    if (akunAssignmentDefaultsCache) return akunAssignmentDefaultsCache;
    try {
        const grouped = await _getV2('/settings/grouped');
        const rules = grouped.assignment_rules || {};
        akunAssignmentDefaultsCache = {
            ...AKUN_ASSIGN_DEFAULTS,
            assignment_min_health_score: Number(rules.assignment_min_health_score?.value ?? 50),
            assignment_min_warming_level: Number(rules.assignment_min_warming_level?.value ?? 1),
            assignment_retry_count: Number(rules.assignment_retry_count?.value ?? 2),
            assignment_reassign_count: Number(rules.assignment_reassign_count?.value ?? 1)
        };
    } catch {
        akunAssignmentDefaultsCache = {
            ...AKUN_ASSIGN_DEFAULTS,
            assignment_min_health_score: 50,
            assignment_min_warming_level: 1,
            assignment_retry_count: 2,
            assignment_reassign_count: 1
        };
    }
    return akunAssignmentDefaultsCache;
}

function _applyAkunConfigValues(cfg) {
    document.getElementById('cfg-auto-assign-enabled').checked = !!cfg.auto_assign_enabled;
    document.getElementById('cfg-priority-weight').value = cfg.priority_weight ?? 100;
    document.getElementById('cfg-daily-new-group-cap').value = cfg.daily_new_group_cap ?? 10;
    document.getElementById('cfg-manual-health-enabled').checked = !!cfg.manual_health_override_enabled;
    document.getElementById('cfg-manual-health-score').value = cfg.manual_health_override_score ?? 80;
    document.getElementById('cfg-manual-warming-enabled').checked = !!cfg.manual_warming_override_enabled;
    document.getElementById('cfg-manual-warming-level').value = cfg.manual_warming_override_level ?? 2;
    document.getElementById('cfg-fresh-login-grace-enabled').checked = !!cfg.fresh_login_grace_enabled;
    document.getElementById('cfg-fresh-login-grace-minutes').value = cfg.fresh_login_grace_minutes ?? 180;
    document.getElementById('cfg-fresh-login-health-floor').value = cfg.fresh_login_health_floor ?? 80;
    document.getElementById('cfg-fresh-login-warming-floor').value = cfg.fresh_login_warming_floor ?? 2;
    document.getElementById('cfg-assignment-notes').value = cfg.assignment_notes || '';
}

function _renderAkunDefaultInfo(defaults) {
    const setText = (id, value) => {
        const el = document.getElementById(id);
        if (el) el.textContent = `(default: ${value})`;
    };
    setText('default-priority-weight', defaults.priority_weight);
    setText('default-daily-new-group-cap', defaults.daily_new_group_cap);
    setText('default-manual-health-score', defaults.manual_health_override_score);
    setText('default-manual-warming-level', defaults.manual_warming_override_level);
    setText('default-fresh-login-grace-minutes', `${defaults.fresh_login_grace_minutes} menit`);
    setText('default-fresh-login-health-floor', defaults.fresh_login_health_floor);
    setText('default-fresh-login-warming-floor', defaults.fresh_login_warming_floor);
    const el = document.getElementById('akun-default-info');
    if (!el) return;
    el.innerHTML = `
        <div class="default-pill">Global min health <strong>${defaults.assignment_min_health_score}</strong></div>
        <div class="default-pill">Global min warming <strong>${defaults.assignment_min_warming_level}</strong></div>
        <div class="default-pill">Retry <strong>${defaults.assignment_retry_count}x</strong></div>
        <div class="default-pill">Reassign <strong>${defaults.assignment_reassign_count}x</strong></div>
        <div class="default-pill">Priority bawaan <strong>${defaults.priority_weight}</strong></div>
        <div class="default-pill">Cap assign bawaan <strong>${defaults.daily_new_group_cap}</strong></div>
        <div class="default-pill">Grace login <strong>${defaults.fresh_login_grace_minutes} menit</strong></div>
        <div class="default-pill">Floor grace <strong>H${defaults.fresh_login_health_floor} / W${defaults.fresh_login_warming_floor}</strong></div>`;
}

async function muatAkun() {
    muatSidebarSummary();
    try {
        const defaults = await _getAkunAssignmentDefaults();
        _renderAkunDefaultInfo(defaults);
    } catch {}

    // Kartu ringkasan mini akun — disembunyikan

    setLoading('list-akun');
    try {
        const data = await _get('/akun');
        const el = document.getElementById('list-akun');
        if (!data.length) {
            el.innerHTML = `<div class="empty-state"><div class="icon">👤</div><p>Belum ada akun. Tambah akun di panel atas.</p></div>`;
            return;
        }
        // Urutkan: online & aktif dulu, lalu standby, lalu restricted/banned
        const _skorAkun = (a) => {
            const st = (a.status || '').toLowerCase();
            if (st === 'banned' || st === 'suspended') return 0;
            if (st === 'restricted') return 1;
            if (!a.online) return 2;
            return 3; // online & aktif = paling atas
        };
        const sorted = [...data].sort((a, b) => _skorAkun(b) - _skorAkun(a));
        el.innerHTML = sorted.map(_renderAkunCard).join('');
    } catch {
        document.getElementById('list-akun').innerHTML =
            `<div class="empty-state"><div class="icon">⚠️</div><p>Backend belum jalan.</p></div>`;
    }
}

async function loginAkun() {
    const phone = document.getElementById("input-phone").value.trim();
    if (!phone) { tampilPesan("pesan-akun","⚠️ Nomor HP kosong.","gagal"); return; }
    tampilPesan("pesan-akun","⏳ Menghubungi Telegram...","info");
    try {
        const data = await _post("/akun/login", { phone });
        if (data.status==="aktif") {
            tampilPesan("pesan-akun",`✅ Login berhasil: ${data.nama}`,"berhasil");
            document.getElementById("input-phone").value = "";
            setTimeout(muatAkun, 1000);
        } else if (data.status==="perlu_otp") {
            document.getElementById("form-login").style.display = "none";
            document.getElementById("form-otp").style.display   = "block";
            document.getElementById("otp-phone").value = phone;
            document.getElementById("input-otp").value = "";
            document.getElementById("input-otp").focus();
            tampilPesan("pesan-otp","📱 Cek Telegram kamu untuk kode OTP.","info");
        } else {
            tampilPesan("pesan-akun",`❌ ${data.pesan}`,"gagal");
        }
    } catch { tampilPesan("pesan-akun","❌ Backend belum jalan.","gagal"); }
}

async function loginUlang(phone) {
    // Isi form login dan trigger login
    document.getElementById("input-phone").value = phone;
    await loginAkun();
}

async function submitOtp() {
    const phone    = document.getElementById("otp-phone").value;
    const kode     = document.getElementById("input-otp").value.trim();
    const password = document.getElementById("input-2fa").value.trim();
    if (!kode) { tampilPesan("pesan-otp","⚠️ Masukkan kode OTP.","gagal"); return; }
    tampilPesan("pesan-otp","⏳ Memverifikasi...","info");
    try {
        const data = await _post("/akun/otp",{phone,kode,password:password||null});
        if (data.status==="aktif") {
            tampilPesan("pesan-otp",`✅ Login berhasil: ${data.nama}`,"berhasil");
            setTimeout(()=>{ batalOtp(); muatAkun(); }, 1500);
        } else if (data.status==="perlu_2fa") {
            document.getElementById("form-2fa").style.display="block";
            tampilPesan("pesan-otp","🔐 Masukkan password 2FA Telegram kamu.","info");
        } else {
            tampilPesan("pesan-otp",`❌ ${data.pesan}`,"gagal");
        }
    } catch { tampilPesan("pesan-otp","❌ Gagal verifikasi.","gagal"); }
}

function batalOtp() {
    document.getElementById("form-otp").style.display   = "none";
    document.getElementById("form-login").style.display = "block";
    document.getElementById("form-2fa").style.display   = "none";
    ["input-phone","input-otp","input-2fa"].forEach(id =>
        document.getElementById(id).value = "");
}

async function logoutAkun(phone) {
    if (!confirm(`Logout akun ${phone}?`)) return;
    await _post("/akun/logout", { phone });
    muatAkun();
}
async function hapusAkunPermanen(phone) {
    const ok = confirm(`Hapus permanen akun ${phone}?

Ini akan:
- logout akun bila sedang online
- menghapus akun dari database
- menghapus session lokal akun

Tindakan ini tidak bisa dibatalkan.`);
    if (!ok) return;
    await _post("/akun/hapus", { phone });
    muatAkun();
}
async function pulihkanAkun(phone) { await _post("/akun/pulihkan",{phone}); muatAkun(); }
async function hitungScoreAkun(phone) { await _post(`/akun/${phone}/score`,{}); muatAkun(); }

async function resetJoinThrottle(phone) {
    if (!confirm(`Reset throttle join untuk ${phone}?\nAkun ini akan bisa join sekarang.`)) return;
    try {
        const r = await fetch(`/api/v2/akun/${encodeURIComponent(phone)}/reset-join-throttle`, { method: 'POST' });
        const data = await r.json();
        if (data.success) { showToast('✅ Throttle join berhasil direset', 'success'); muatAkun(); }
        else showToast('❌ Gagal: ' + (data.message || data.error), 'error');
    } catch (e) { showToast('❌ Error: ' + e.message, 'error'); }
}

async function resetBroadcastThrottle() {
    if (!confirm('Reset throttle broadcast?\nSistem akan bisa kirim pesan sekarang.')) return;
    try {
        const r = await fetch('/api/v2/broadcast-queue/reset-throttle', { method: 'POST' });
        const data = await r.json();
        if (data.success) showToast('✅ Throttle broadcast berhasil direset', 'success');
        else showToast('❌ Gagal: ' + (data.message || data.error), 'error');
    } catch (e) { showToast('❌ Error: ' + e.message, 'error'); }
}

function showToast(msg, type = 'info') {
    const el = document.createElement('div');
    el.className = `toast-notif toast-${type}`;
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => el.classList.add('toast-show'), 10);
    setTimeout(() => { el.classList.remove('toast-show'); setTimeout(() => el.remove(), 300); }, 3000);
}

// ── ANTRIAN ───────────────────────────────────────────────
async function muatAntrian() {
    await _isiSelectAkunTersedia("antrian-akun");
    await _isiSelectGrup("antrian-grup");
    await syncPesanAktif();
    setLoading("list-antrian");
    try {
        const data = await _get("/antrian");
        const el   = document.getElementById("list-antrian");
        if (!data.length) {
            el.innerHTML = `<div class="empty-state"><div class="icon">📋</div><p>Antrian kosong.</p></div>`;
            return;
        }
        el.innerHTML = data.map(a => `
            <div class="antrian-card ${a.status}">
                <div class="antrian-header">
                    <strong>Grup ID: ${a.grup_id}</strong>
                    <span class="badge badge-${a.status}">${a.status}</span>
                </div>
                <div class="antrian-info">Akun: ${a.phone} · ${a.dibuat}</div>
                <div class="antrian-pesan-preview">${a.pesan}</div>
                <div class="antrian-footer">
                    ${a.status==='menunggu'
                        ? `<button class="btn-success btn-sm" onclick="kirimDariAntrian(${a.id},this)">📤 Kirim</button>`
                        : ''}
                    <button class="btn-danger btn-sm" onclick="hapusAntrian(${a.id})">🗑️ Hapus</button>
                </div>
            </div>`).join("");
    } catch {}
}

async function tambahAntrian() {
    const phone  = document.getElementById("antrian-akun").value;
    const grup_id = document.getElementById("antrian-grup").value;
    const pesan  = document.getElementById("antrian-pesan").value.trim();
    if (!phone||!grup_id||!pesan) {
        tampilPesan("pesan-antrian","⚠️ Semua field wajib.","gagal"); return;
    }
    await _post("/antrian",{phone,grup_id:parseInt(grup_id),pesan});
    tampilPesan("pesan-antrian","✅ Ditambahkan!","berhasil");
    document.getElementById("antrian-pesan").value = "";
    muatAntrian();
}

async function kirimDariAntrian(id, btn) {
    btn.textContent = "⏳..."; btn.disabled = true;
    try {
        const d = await _post(`/antrian/${id}/kirim`,{});
        btn.textContent = d.status==="berhasil" ? "✅ Terkirim" : "❌ Gagal";
        if (d.status !== "berhasil") { btn.disabled = false; alert(d.pesan); }
        setTimeout(muatAntrian, 1000);
    } catch { btn.textContent="❌ Error"; btn.disabled=false; }
}
async function hapusAntrian(id) { if (!confirm("Hapus?")) return; await _del(`/antrian/${id}`); muatAntrian(); }

// ── KIRIM ─────────────────────────────────────────────────
async function muatTabKirim() {
    await _isiSelectAkunTersedia("pilih-akun-kirim");
    await _isiSelectGrup("pilih-grup-kirim");
    await syncPesanAktif();
    muatIndikatorAkun();
}

async function muatIndikatorAkun() {
    try {
        const t = await _get("/akun/tersedia");
        const s = await _get("/akun");
        const online = s.filter(a => a.online).length;
        document.getElementById("indikator-akun").innerHTML = `
            <div class="indikator-box">
                <div class="ind-item"><div class="ind-dot ok"></div>
                    <span><strong>${t.length}</strong> akun siap kirim</span></div>
                <div class="ind-item"><div class="ind-dot warn"></div>
                    <span><strong>${online - t.length}</strong> limit/offline</span></div>
            </div>`;
    } catch {}
}

async function cekStatusGrup() {
    const gid = document.getElementById("pilih-grup-kirim").value;
    const el  = document.getElementById("status-grup-tujuan");
    if (!gid) { el.textContent = ""; return; }
    try {
        const data = await _get("/grup");
        const g    = data.find(x => String(x.id) === String(gid));
        if (!g) return;
        const warna = g.label==='Hot'?'#ef4444':g.label==='Normal'?'#2563eb':'#94a3b8';
        el.textContent = `${g.label==='Hot'?'🔥':g.label==='Normal'?'✅':'⏭️'} ${g.label} · Score: ${g.score}`;
        el.style.color = warna;
    } catch {}
}

async function kirimPesan() {
    const phone  = document.getElementById("pilih-akun-kirim").value;
    const grup_id = document.getElementById("pilih-grup-kirim").value;
    const pesan  = document.getElementById("isi-pesan").value.trim();
    if (!phone)  { tampilPesan("hasil-kirim","⚠️ Pilih akun.","gagal"); return; }
    if (!grup_id){ tampilPesan("hasil-kirim","⚠️ Pilih grup.","gagal"); return; }
    if (!pesan)  { tampilPesan("hasil-kirim","⚠️ Pesan kosong.","gagal"); return; }
    tampilPesan("hasil-kirim","⏳ Mengirim...","info");
    try {
        const d = await _post("/pesan/kirim",{phone,grup_id:parseInt(grup_id),pesan});
        if (d.status==="berhasil") {
            tampilPesan("hasil-kirim",`✅ Terkirim ke: ${d.grup}`,"berhasil");
            document.getElementById("isi-pesan").value = "";
            muatSidebarSummary();
        } else {
            tampilPesan("hasil-kirim",`❌ ${d.pesan}`,"gagal");
        }
    } catch { tampilPesan("hasil-kirim","❌ Gagal.","gagal"); }
}

// ── RIWAYAT ───────────────────────────────────────────────
async function muatRiwayat() {
    try {
        const r = await _get("/riwayat/ringkasan");
        document.getElementById("ringkasan-box").innerHTML = `
            <div class="ringkasan-item hijau"><div class="angka">${r.berhasil}</div><div class="label">✅ Berhasil</div></div>
            <div class="ringkasan-item merah"><div class="angka">${r.gagal}</div><div class="label">❌ Gagal</div></div>
            <div class="ringkasan-item kuning"><div class="angka">${r.skip}</div><div class="label">⏭️ Skip</div></div>
            <div class="ringkasan-item biru"><div class="angka">${r.join_grup || 0}</div><div class="label">🔗 Join Grup</div></div>
            <div class="ringkasan-item abu"><div class="angka">${r.total}</div><div class="label">📋 Total</div></div>`;
    } catch {}
    setLoading("list-riwayat");
    try {
        const data = await _get("/riwayat");
        const el   = document.getElementById("list-riwayat");
        if (!data.length) {
            el.innerHTML = `<div class="empty-state"><div class="icon">📈</div><p>Belum ada riwayat.</p></div>`;
            return;
        }

        function _ikonRiwayat(status) {
            if (status === 'berhasil') return '✅';
            if (status === 'join')     return '🔗';
            if (status === 'skip')     return '⏭️';
            if (status === 'gagal')    return '❌';
            return '📋';
        }
        function _labelRiwayat(status) {
            if (status === 'berhasil') return 'Terkirim';
            if (status === 'join')     return 'Join Grup';
            if (status === 'skip')     return 'Skip';
            if (status === 'gagal')    return 'Gagal';
            return status;
        }

        el.innerHTML = [...data].reverse().map(r => `
            <div class="log-item ${r.status}">
                <div>
                    <strong>${_ikonRiwayat(r.status)} ${r.nama_grup || r.grup_id}</strong>
                    <small>Akun: ${r.phone}</small>
                    ${r.pesan_error ? `<small style="color:#ef4444;display:block">⚠️ ${r.pesan_error}</small>` : ''}
                </div>
                <div style="text-align:right;flex-shrink:0">
                    <span class="badge badge-${r.status === 'berhasil' ? 'aktif' : r.status === 'join' ? 'normal' : r.status === 'skip' ? 'skip' : 'offline'}" style="font-size:10px">${_labelRiwayat(r.status)}</span>
                    <div class="log-waktu">${r.waktu}</div>
                </div>
            </div>`).join("");
    } catch {}
}

// ── HELPERS ───────────────────────────────────────────────
async function _isiSelectAkun(id) {
    try {
        const data  = await _get("/akun");
        const aktif = data.filter(a => {
            const st = (a.status || '').toLowerCase();
            // Scraper boleh pakai akun restricted — SearchRequest tidak terpengaruh pembatasan kirim
            // Hanya exclude akun yang benar-benar tidak bisa dipakai sama sekali
            if (st === 'banned' || st === 'suspended' || st === 'session_expired') return false;
            return a.online || st === 'restricted';
        });
        const sel   = document.getElementById(id);
        if (!sel) return;
        const opsiAkun = aktif.map(a => {
            const st = (a.status || '').toLowerCase();
            const label = st === 'restricted' ? ' [Dibatasi]' : '';
            return `<option value="${a.phone}">${a.nama||a.phone} (${a.phone})${label}</option>`;
        }).join("");
        sel.innerHTML = `<option value="auto">🔄 Otomatis (sistem pilih &amp; rotasi)</option>` + opsiAkun;
    } catch {}
}

async function _isiSelectAkunTersedia(id) {
    try {
        const t   = await _get("/akun/tersedia");
        const s   = await _get("/akun");
        const sel = document.getElementById(id);
        if (!sel) return;
        sel.innerHTML = t.length
            ? t.map(p => {
                const info = s.find(a => a.phone === p) || {};
                return `<option value="${p}">${info.nama||p} (${p})</option>`;
              }).join("")
            : `<option value="">-- Tidak ada akun tersedia --</option>`;
    } catch {}
}

async function _isiSelectGrup(id) {
    try {
        const data = await _get("/grup/aktif");
        const sel  = document.getElementById(id);
        if (!sel) return;
        sel.innerHTML = data.length
            ? data.map(g => `<option value="${g.id}">${g.nama}</option>`).join("")
            : `<option value="">-- Belum ada grup --</option>`;
    } catch {}
}

muatSidebarSummary();
cekKesehatanBackend();
tampilTab("akun");
setInterval(muatSidebarSummary, 60000);
setInterval(cekKesehatanBackend, 30000);


async function cekKesehatanBackend() {
    const badge = document.getElementById('health-badge');
    if (!badge) return;
    try {
        const h = await _get('/health');
        badge.className = 'health-badge online';
        badge.textContent = `🟢 Backend aktif · ${h.online_accounts || 0} akun online`;
    } catch {
        badge.className = 'health-badge offline';
        badge.textContent = '🔴 Backend belum terhubung';
    }
}

function refreshTabAktif() {
    tampilTab(window.tabAktif || 'akun');
}

// ── Helpers API v2 (dipakai oleh permissions.js, assignments.js, dll) ────────
async function _getV2(path) {
    const r = await fetch(`${API_V2}${path}`);
    const json = await r.json();
    if (!r.ok) throw new Error(json.error || json.message || 'Error');
    return json.data !== undefined ? json.data : json;
}
async function _postV2(path, payload) {
    const r = await fetch(`${API_V2}${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
    const json = await r.json();
    if (!r.ok) throw new Error(json.error || json.message || 'Error');
    return json.data !== undefined ? json.data : json;
}
async function _patchV2(path, payload) {
    const r = await fetch(`${API_V2}${path}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
    const json = await r.json();
    if (!r.ok) throw new Error(json.error || json.message || 'Error');
    return json.data !== undefined ? json.data : json;
}
async function _deleteV2(path) {
    const r = await fetch(`${API_V2}${path}`, { method: 'DELETE' });
    const json = await r.json();
    if (!r.ok) throw new Error(json.error || json.message || 'Error');
    return json;
}

// ── Badge helper (dipakai di permissions.js, assignments.js, dll) ─────────────
function _badgeStatus(status) {
    const map = {
        valid: 'badge-aktif', active: 'badge-aktif', assigned: 'badge-aktif',
        recovered: 'badge-aktif', sent: 'badge-aktif', done: 'badge-aktif',
        expired: 'badge-warn', pending: 'badge-warn', retry_wait: 'badge-warn',
        partial: 'badge-warn', paused: 'badge-warn', blocked: 'badge-warn',
        revoked: 'badge-gagal', failed: 'badge-gagal', stopped: 'badge-gagal',
        abandoned: 'badge-gagal', banned: 'badge-gagal',
        queued: 'badge-info', running: 'badge-info', recoverable: 'badge-info',
        released: '', skipped: '', ignored: '',
    };
    return `<span class="${map[status] || 'badge-info'}">${status || '-'}</span>`;
}

// ── AUTO REFRESH GLOBAL ───────────────────────────────────
// Setiap tab punya interval refresh sendiri.
// Hanya berjalan kalau tab itu yang sedang aktif terbuka.
// Interval masing-masing tab:
//   assignments  → 15 detik (perlu tau kalau auto-assign background bekerja)
//   campaigns    → 20 detik (pantau status campaign & queue)
//   analisis     → 60 detik (score grup tidak berubah cepat)
//   permissions  → 60 detik (jarang berubah)
//   recovery     → 30 detik (perlu cepat tahu kalau ada masalah baru)
//   riwayat      → 30 detik (pantau kirim hari ini)

const _AUTO_REFRESH_CONFIG = {
    assignments: { fn: () => muatAssignments(),   interval: 15000 },
    automation:  { fn: () => muatStatusOtomasi(), interval: 15000 },
    groupstates: { fn: () => muatGroupStateDashboard(), interval: 20000 },
    campaigns:   { fn: () => muatCampaigns(),     interval: 20000 },
    analisis:    { fn: () => muatAnalisis(),       interval: 60000 },
    permissions: { fn: () => muatPermissions(),    interval: 60000 },
    recovery:    { fn: () => muatRecovery(),       interval: 30000 },
    riwayat:     { fn: () => muatRiwayat(),        interval: 30000 },
};

const _autoRefreshTimers = {};

function _startAutoRefresh(tabNama) {
    _stopAutoRefresh(tabNama);
    const cfg = _AUTO_REFRESH_CONFIG[tabNama];
    if (!cfg) return;
    _autoRefreshTimers[tabNama] = setInterval(() => {
        // Hanya refresh kalau tab ini yang aktif
        if (window.tabAktif === tabNama) {
            try { cfg.fn(); } catch {}
        }
    }, cfg.interval);
}

function _stopAutoRefresh(tabNama) {
    if (_autoRefreshTimers[tabNama]) {
        clearInterval(_autoRefreshTimers[tabNama]);
        delete _autoRefreshTimers[tabNama];
    }
}

// Hook ke fungsi tampilTab — setiap ganti tab, start/stop timer yang sesuai
const _tampilTabAsli = tampilTab;
tampilTab = function(nama) {
    // Hentikan semua timer tab lain yang ada di config
    Object.keys(_AUTO_REFRESH_CONFIG).forEach(t => {
        if (t !== nama) _stopAutoRefresh(t);
    });
    // Jalankan fungsi tampilTab asli
    _tampilTabAsli(nama);
    // Start timer untuk tab yang baru dibuka
    _startAutoRefresh(nama);
};

// Start untuk tab yang aktif saat halaman pertama kali dibuka
setTimeout(() => _startAutoRefresh(window.tabAktif || 'akun'), 2000);


let akunConfigAktifPhone = null;

async function bukaKonfigAkun(phone) {
    const panel = document.getElementById('panel-konfig-akun');
    const pesan = document.getElementById('pesan-konfig-akun');
    if (pesan) { pesan.textContent = ''; pesan.className = 'pesan-status'; }
    panel.style.display = 'block';
    panel.scrollIntoView({behavior:'smooth', block:'start'});
    try {
        const [cfg, defaults, akunList] = await Promise.all([
            _get(`/akun/${encodeURIComponent(phone)}/config`),
            _getAkunAssignmentDefaults(),
            _get('/akun')
        ]);
        akunConfigAktifPhone = phone;
        _renderAkunDefaultInfo(defaults);
        document.getElementById('judul-konfig-akun').textContent = `⚙️ Konfigurasi Assign Akun — ${cfg.nama || phone}`;
        document.getElementById('subjudul-konfig-akun').textContent =
            `Nomor: ${phone} · Login terakhir: ${_formatDateTimeText(cfg.last_login_at)} · Gunakan override ini bila akun sehat tetapi baru login.`;
        _applyAkunConfigValues(cfg);

        // Isi nilai score dan health_score dari data akun
        const akun = (akunList || []).find(a => a.phone === phone);
        const scoreEl  = document.getElementById('cfg-score');
        const healthEl = document.getElementById('cfg-health-score');
        if (scoreEl && akun)  scoreEl.value  = akun.score ?? 0;
        if (healthEl && akun) healthEl.value = akun.health_score ?? 100;
    } catch (err) {
        tampilPesan('pesan-konfig-akun', `❌ ${err.message}`, 'gagal');
    }
}

function isiPresetGraceAkun() {
    document.getElementById('cfg-auto-assign-enabled').checked = true;
    document.getElementById('cfg-manual-health-enabled').checked = false;
    document.getElementById('cfg-manual-warming-enabled').checked = false;
    document.getElementById('cfg-fresh-login-grace-enabled').checked = true;
    document.getElementById('cfg-fresh-login-grace-minutes').value = 180;
    document.getElementById('cfg-fresh-login-health-floor').value = 80;
    document.getElementById('cfg-fresh-login-warming-floor').value = 2;
    document.getElementById('cfg-assignment-notes').value = 'Preset: akun dianggap sehat sementara setelah login ulang agar lolos auto-assign.';
    tampilPesan('pesan-konfig-akun', 'Preset fresh login grace terisi.', 'info');
}

async function pakaiDefaultKonfigAkun() {
    const defaults = await _getAkunAssignmentDefaults();
    _applyAkunConfigValues(defaults);
    tampilPesan('pesan-konfig-akun', 'Nilai default sistem dimuat ke form.', 'info');
}

function tutupKonfigAkun() {
    const panel = document.getElementById('panel-konfig-akun');
    if (panel) panel.style.display = 'none';
    akunConfigAktifPhone = null;
}

async function simpanKonfigAkun() {
    if (!akunConfigAktifPhone) return;
    const payload = {
        auto_assign_enabled: document.getElementById('cfg-auto-assign-enabled').checked ? 1 : 0,
        priority_weight: Number(document.getElementById('cfg-priority-weight').value || 100),
        daily_new_group_cap: Number(document.getElementById('cfg-daily-new-group-cap').value || 0),
        manual_health_override_enabled: document.getElementById('cfg-manual-health-enabled').checked ? 1 : 0,
        manual_health_override_score: Number(document.getElementById('cfg-manual-health-score').value || 0),
        manual_warming_override_enabled: document.getElementById('cfg-manual-warming-enabled').checked ? 1 : 0,
        manual_warming_override_level: Number(document.getElementById('cfg-manual-warming-level').value || 1),
        fresh_login_grace_enabled: document.getElementById('cfg-fresh-login-grace-enabled').checked ? 1 : 0,
        fresh_login_grace_minutes: Number(document.getElementById('cfg-fresh-login-grace-minutes').value || 0),
        fresh_login_health_floor: Number(document.getElementById('cfg-fresh-login-health-floor').value || 0),
        fresh_login_warming_floor: Number(document.getElementById('cfg-fresh-login-warming-floor').value || 1),
        assignment_notes: document.getElementById('cfg-assignment-notes').value || '',
        // Edit score dan health_score langsung
        score: Number(document.getElementById('cfg-score').value ?? -1),
        health_score: Number(document.getElementById('cfg-health-score').value ?? -1),
    };
    // Hapus kalau -1 (tidak diubah user)
    if (payload.score < 0) delete payload.score;
    if (payload.health_score < 0) delete payload.health_score;
    try {
        await _post(`/akun/${encodeURIComponent(akunConfigAktifPhone)}/config`, payload);
        tampilPesan('pesan-konfig-akun', '✅ Konfigurasi akun disimpan.', 'berhasil');
        await muatAkun();
    } catch (err) {
        tampilPesan('pesan-konfig-akun', `❌ ${err.message}`, 'gagal');
    }
}

async function resetHealthScore(phone) {
    try {
        await _post(`/akun/${encodeURIComponent(phone)}/health`, { health_score: 100 });
        tampilPesan('pesan-konfig-akun', '✅ Health score direset ke 100.', 'berhasil');
        await muatAkun();
        await bukaKonfigAkun(phone);
    } catch (err) {
        tampilPesan('pesan-konfig-akun', `❌ ${err.message}`, 'gagal');
    }
}