const API = "http://localhost:5000/api";

// ── NAVIGASI ──────────────────────────────────────────────
function tampilTab(nama) {
  document.querySelectorAll(".tab").forEach(t => t.style.display = "none");
  document.querySelectorAll(".sidebar nav a").forEach(a => a.classList.remove("aktif-menu"));
  document.getElementById(`tab-${nama}`).style.display = "block";
  const m = document.getElementById(`menu-${nama}`);
  if (m) m.classList.add("aktif-menu");
  const fn = { akun:"muatAkun", grup:"muatTabGrup", draft:"muatDraft",
               antrian:"muatAntrian", kirim:"muatTabKirim", riwayat:"muatRiwayat" };
  if (fn[nama]) window[fn[nama]]();
}

function tampilPesan(id, teks, tipe="info") {
  const el = document.getElementById(id);
  el.textContent = teks;
  el.className = `pesan-status ${tipe}`;
}
function setLoading(id, teks="Memuat...") {
  document.getElementById(id).innerHTML =
    `<div class="loading"><span class="spinner"></span> ${teks}</div>`;
}

// ── HELPER API ────────────────────────────────────────────
async function _get(path) {
  const r = await fetch(`${API}${path}`); return r.json();
}
async function _post(path, body) {
  const r = await fetch(`${API}${path}`, {
    method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify(body)
  }); return r.json();
}
async function _del(path) { await fetch(`${API}${path}`, {method:"DELETE"}); }

// ── LIMIT BAR ─────────────────────────────────────────────
function buatLimitBar(sudah, batas) {
  const persen = Math.min(100, Math.round((sudah / batas) * 100));
  const sisa   = batas - sudah;
  const kelas  = persen < 60 ? "ok" : persen < 90 ? "warn" : "full";
  const sk     = persen < 60 ? "sisa-ok" : persen < 90 ? "sisa-warn" : "sisa-full";
  return `
    <div class="limit-bar-wrapper">
      <div class="limit-bar-label">
        <span>${sudah}/${batas} pesan hari ini</span>
        <span class="${sk}">sisa ${sisa}</span>
      </div>
      <div class="limit-bar-track">
        <div class="limit-bar-fill ${kelas}" style="width:${persen}%"></div>
      </div>
    </div>`;
}

// ── SIDEBAR SUMMARY ───────────────────────────────────────
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

// ── TAB AKUN ──────────────────────────────────────────────
async function muatAkun() {
  setLoading("list-akun");
  muatSidebarSummary();

  // Ringkasan limit
  try {
    const ring = await _get("/akun/ringkasan");
    const grid = document.getElementById("ringkasan-akun-grid");
    grid.innerHTML = ring.length ? ring.map(a => `
      <div class="akun-card">
        <div class="ac-nama">${a.phone.slice(-8)}</div>
        <div class="ac-phone">${a.phone}</div>
        <div class="ac-status">
          <span class="badge badge-${a.status_akun}">${a.status_akun}</span>
          <span style="font-size:11px;color:${a.boleh_kirim?'#16a34a':'#dc2626'}">
            ${a.boleh_kirim ? '✅ Bisa kirim' : '🚫 Limit'}
          </span>
        </div>
        ${buatLimitBar(a.sudah_kirim, a.batas)}
      </div>`).join("") : "";
  } catch {}

  // Daftar akun
  try {
    const data = await _get("/akun");
    const el   = document.getElementById("list-akun");
    if (!data.length) {
      el.innerHTML = `<div class="empty-state"><div class="icon">👤</div><p>Belum ada akun. Tambah di bawah.</p></div>`;
      return;
    }
    el.innerHTML = data.map(a => {
      const st = a.status || "active";
      return `
        <div class="kartu ${a.online ? 'aktif' : 'nonaktif'}">
          <div>
            <strong>${a.nama || 'Tanpa Nama'}</strong>
            <small>@${a.username||'-'} · ${a.phone}</small>
          </div>
          <div class="kartu-actions">
            <span class="badge badge-${st}">${st}</span>
            <span class="badge ${a.online?'badge-aktif':'badge-offline'}">
              ${a.online ? '🟢 Online' : '⚫ Offline'}
            </span>
            ${st !== 'active'
              ? `<button class="btn-sm btn-outline" onclick="pulihkanAkun('${a.phone}')">♻️ Pulihkan</button>`
              : `<button class="btn-sm btn-danger" onclick="logoutAkun('${a.phone}')">🔌 Logout</button>`}
          </div>
        </div>`;
    }).join("");
  } catch {
    document.getElementById("list-akun").innerHTML =
      `<div class="empty-state"><div class="icon">⚠️</div><p>Backend belum jalan.</p></div>`;
  }
}

// ── LOGIN & OTP FLOW ──────────────────────────────────────
async function loginAkun() {
  const phone = document.getElementById("input-phone").value.trim();
  if (!phone) { tampilPesan("pesan-akun","⚠️ Nomor HP kosong.","gagal"); return; }

  tampilPesan("pesan-akun","⏳ Mengirim OTP...","info");

  try {
    const data = await _post("/akun/login", { phone });

    if (data.status === "aktif") {
      // Langsung login (session sudah ada)
      tampilPesan("pesan-akun",`✅ Login: ${data.nama}`,"berhasil");
      muatAkun();

    } else if (data.status === "perlu_otp") {
      // Tampilkan form OTP
      document.getElementById("form-login").style.display = "none";
      document.getElementById("form-otp").style.display   = "block";
      document.getElementById("otp-phone").value          = phone;
      document.getElementById("input-otp").value          = "";
      document.getElementById("input-otp").focus();
      tampilPesan("pesan-otp","📱 Cek aplikasi Telegram kamu untuk kode OTP.","info");

    } else {
      tampilPesan("pesan-akun",`❌ ${data.pesan}`,"gagal");
    }
  } catch {
    tampilPesan("pesan-akun","❌ Backend belum jalan.","gagal");
  }
}

async function submitOtp() {
  const phone    = document.getElementById("otp-phone").value;
  const kode     = document.getElementById("input-otp").value.trim();
  const password = document.getElementById("input-2fa").value.trim();

  if (!kode) { tampilPesan("pesan-otp","⚠️ Masukkan kode OTP dulu.","gagal"); return; }

  tampilPesan("pesan-otp","⏳ Memverifikasi OTP...","info");

  try {
    const data = await _post("/akun/otp", { phone, kode, password: password||null });

    if (data.status === "aktif") {
      tampilPesan("pesan-otp",`✅ Login berhasil: ${data.nama}!`,"berhasil");
      setTimeout(() => {
        batalOtp();
        muatAkun();
      }, 1500);

    } else if (data.status === "perlu_2fa") {
      // Tampilkan input password 2FA
      document.getElementById("form-2fa").style.display = "block";
      document.getElementById("input-2fa").focus();
      tampilPesan("pesan-otp","🔐 Masukkan password 2FA Telegram kamu.","info");

    } else {
      tampilPesan("pesan-otp",`❌ ${data.pesan}`,"gagal");
    }
  } catch {
    tampilPesan("pesan-otp","❌ Gagal verifikasi. Coba lagi.","gagal");
  }
}

function batalOtp() {
  document.getElementById("form-otp").style.display   = "none";
  document.getElementById("form-login").style.display = "block";
  document.getElementById("form-2fa").style.display   = "none";
  document.getElementById("input-phone").value        = "";
  document.getElementById("input-otp").value          = "";
  document.getElementById("input-2fa").value          = "";
}

async function logoutAkun(phone) {
  await _post("/akun/logout", { phone });
  muatAkun();
}

async function pulihkanAkun(phone) {
  await _post("/akun/pulihkan", { phone });
  muatAkun();
}

// ── TAB GRUP ──────────────────────────────────────────────
async function muatTabGrup() { await _isiSelectAkun("pilih-akun-grup"); }

async function fetchGrup() {
  const phone = document.getElementById("pilih-akun-grup").value;
  if (!phone) { tampilPesan("pesan-grup","⚠️ Pilih akun dulu.","gagal"); return; }
  tampilPesan("pesan-grup","⏳ Mengambil grup...","info");
  setLoading("list-grup","Mengambil grup...");
  try {
    const data = await _post("/grup/fetch", { phone });
    tampilPesan("pesan-grup",`✅ ${data.length} grup ditemukan.`,"berhasil");
    const el = document.getElementById("list-grup");
    if (!data.length) { el.innerHTML=`<div class="empty-state"><div class="icon">👥</div><p>Tidak ada grup.</p></div>`; return; }
    el.innerHTML = data.map(g => {
      const st = g.status || "active";
      return `
        <div class="kartu">
          <div>
            <strong>${g.nama}</strong>
            <small>${g.tipe} · ${g.jumlah_member||'?'} member</small>
          </div>
          <div class="kartu-actions">
            <span class="badge badge-${st}">${st}</span>
            ${st !== 'active'
              ? `<button class="btn-sm btn-outline" onclick="pulihkanGrup(${g.id})">♻️</button>`
              : `<button class="btn-sm btn-danger"  onclick="skipGrup(${g.id})">⏭️ Skip</button>`}
          </div>
        </div>`;
    }).join("");
  } catch { tampilPesan("pesan-grup","❌ Gagal fetch.","gagal"); }
}

async function skipGrup(id)     { await _post("/grup/status",{grup_id:id,status:"skip"}); fetchGrup(); }
async function pulihkanGrup(id) { await _post("/grup/pulihkan",{grup_id:id}); fetchGrup(); }

// ── TAB DRAFT ─────────────────────────────────────────────
async function muatDraft() {
  setLoading("list-draft");
  try {
    const data = await _get("/draft");
    const el   = document.getElementById("list-draft");
    if (!data.length) { el.innerHTML=`<div class="empty-state"><div class="icon">📝</div><p>Belum ada draft.</p></div>`; return; }
    el.innerHTML = data.map(d => `
      <div class="draft-card">
        <div class="draft-judul">📝 ${d.judul}</div>
        <div class="draft-isi">${d.isi}</div>
        <div class="draft-footer">
          <span class="draft-tanggal">${d.dibuat}</span>
          <div style="display:flex;gap:6px">
            <button class="btn-sm btn-outline" onclick="pakaiDraft(${d.id})">📋 Pakai</button>
            <button class="btn-sm btn-danger"  onclick="hapusDraft(${d.id})">🗑️</button>
          </div>
        </div>
      </div>`).join("");
  } catch {}
}

async function simpanDraft() {
  const judul = document.getElementById("draft-judul").value.trim();
  const isi   = document.getElementById("draft-isi").value.trim();
  if (!judul||!isi) { tampilPesan("pesan-draft","⚠️ Judul dan isi wajib.","gagal"); return; }
  await _post("/draft",{judul,isi});
  tampilPesan("pesan-draft","✅ Draft tersimpan!","berhasil");
  document.getElementById("draft-judul").value="";
  document.getElementById("draft-isi").value="";
  muatDraft();
}

async function hapusDraft(id) {
  if (!confirm("Hapus draft ini?")) return;
  await _del(`/draft/${id}`);
  muatDraft();
}

async function pakaiDraft(id) {
  const data = await _get("/draft");
  const d    = data.find(x => x.id === id);
  if (!d) return;
  document.getElementById("antrian-pesan").value = d.isi;
  tampilTab("antrian");
}

// ── TAB ANTRIAN ───────────────────────────────────────────
async function muatAntrian() {
  await _isiSelectAkunTersedia("antrian-akun");
  await _isiSelectGrup("antrian-grup");
  setLoading("list-antrian");
  try {
    const data = await _get("/antrian");
    const el   = document.getElementById("list-antrian");
    if (!data.length) { el.innerHTML=`<div class="empty-state"><div class="icon">📋</div><p>Antrian kosong.</p></div>`; return; }
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
          <button class="btn-danger btn-sm" onclick="hapusAntrian(${a.id})">🗑️</button>
        </div>
      </div>`).join("");
  } catch {}
}

async function tambahAntrian() {
  const phone   = document.getElementById("antrian-akun").value;
  const grup_id = document.getElementById("antrian-grup").value;
  const pesan   = document.getElementById("antrian-pesan").value.trim();
  if (!phone||!grup_id||!pesan) { tampilPesan("pesan-antrian","⚠️ Semua field wajib.","gagal"); return; }
  await _post("/antrian",{phone,grup_id:parseInt(grup_id),pesan});
  tampilPesan("pesan-antrian","✅ Ditambahkan ke antrian!","berhasil");
  document.getElementById("antrian-pesan").value="";
  muatAntrian();
}

async function kirimDariAntrian(id, btn) {
  btn.textContent="⏳..."; btn.disabled=true;
  try {
    const data = await _post(`/antrian/${id}/kirim`,{});
    btn.textContent = data.status==="berhasil" ? "✅ Terkirim" : "❌ Gagal";
    if (data.status !== "berhasil") { btn.disabled=false; alert(data.pesan); }
    setTimeout(muatAntrian, 1000);
  } catch { btn.textContent="❌ Error"; btn.disabled=false; }
}

async function hapusAntrian(id) {
  if (!confirm("Hapus item ini?")) return;
  await _del(`/antrian/${id}`);
  muatAntrian();
}

// ── TAB KIRIM ─────────────────────────────────────────────
async function muatTabKirim() {
  await _isiSelectAkunTersedia("pilih-akun-kirim");
  await _isiSelectGrup("pilih-grup-kirim");
  muatIndikatorAkun();
}

async function muatIndikatorAkun() {
  try {
    const tersedia = await _get("/akun/tersedia");
    const semua    = await _get("/akun");
    const online   = semua.filter(a => a.online).length;
    document.getElementById("indikator-akun").innerHTML = `
      <div class="indikator-box">
        <div class="ind-item"><div class="ind-dot ok"></div>
          <span><strong>${tersedia.length}</strong> akun siap kirim</span></div>
        <div class="ind-item"><div class="ind-dot warn"></div>
          <span><strong>${online - tersedia.length}</strong> akun limit/offline</span></div>
      </div>`;
  } catch {}
}

async function cekStatusGrup() {
  const grup_id = document.getElementById("pilih-grup-kirim").value;
  const el      = document.getElementById("status-grup-tujuan");
  if (!grup_id) { el.textContent=""; return; }
  try {
    const data = await _get("/grup");
    const grup = data.find(g => String(g.id) === String(grup_id));
    if (!grup) { el.textContent=""; return; }
    const st   = grup.status || "active";
    const icon = {active:"✅",failed:"❌",skip:"⏭️"}[st] || "❓";
    el.textContent = `${icon} ${st}`;
    el.style.color = st==="active" ? "#16a34a" : "#dc2626";
  } catch {}
}

async function kirimPesan() {
  const phone   = document.getElementById("pilih-akun-kirim").value;
  const grup_id = document.getElementById("pilih-grup-kirim").value;
  const pesan   = document.getElementById("isi-pesan").value.trim();
  if (!phone)   { tampilPesan("hasil-kirim","⚠️ Pilih akun.","gagal"); return; }
  if (!grup_id) { tampilPesan("hasil-kirim","⚠️ Pilih grup.","gagal"); return; }
  if (!pesan)   { tampilPesan("hasil-kirim","⚠️ Pesan kosong.","gagal"); return; }
  tampilPesan("hasil-kirim","⏳ Mengirim...","info");
  try {
    const data = await _post("/pesan/kirim",{phone,grup_id:parseInt(grup_id),pesan});
    if (data.status==="berhasil") {
      tampilPesan("hasil-kirim",`✅ Terkirim ke: ${data.grup}`,"berhasil");
      document.getElementById("isi-pesan").value="";
      muatIndikatorAkun();
      muatSidebarSummary();
    } else {
      tampilPesan("hasil-kirim",`❌ ${data.pesan}`,"gagal");
    }
  } catch { tampilPesan("hasil-kirim","❌ Gagal. Cek backend.","gagal"); }
}

// ── TAB RIWAYAT ───────────────────────────────────────────
async function muatRiwayat() {
  try {
    const r = await _get("/riwayat/ringkasan");
    document.getElementById("ringkasan-box").innerHTML = `
      <div class="ringkasan-item hijau"><div class="angka">${r.berhasil}</div><div class="label">✅ Berhasil</div></div>
      <div class="ringkasan-item merah"><div class="angka">${r.gagal}</div><div class="label">❌ Gagal</div></div>
      <div class="ringkasan-item kuning"><div class="angka">${r.skip}</div><div class="label">⏭️ Skip</div></div>
      <div class="ringkasan-item biru"><div class="angka">${r.total}</div><div class="label">📋 Total</div></div>`;
  } catch {}
  setLoading("list-riwayat");
  try {
    const data = await _get("/riwayat");
    const el   = document.getElementById("list-riwayat");
    if (!data.length) { el.innerHTML=`<div class="empty-state"><div class="icon">📊</div><p>Belum ada riwayat.</p></div>`; return; }
    el.innerHTML = [...data].reverse().map(r => `
      <div class="log-item ${r.status}">
        <div>
          <strong>${r.status==='berhasil'?'✅':r.status==='skip'?'⏭️':'❌'} ${r.nama_grup||r.grup_id}</strong>
          <small>Akun: ${r.phone}</small>
        </div>
        <span class="log-waktu">${r.waktu}</span>
      </div>`).join("");
  } catch {}
}

// ── SELECT HELPER ─────────────────────────────────────────
async function _isiSelectAkun(id) {
  try {
    const data  = await _get("/akun");
    const aktif = data.filter(a => a.online);
    const sel   = document.getElementById(id);
    sel.innerHTML = aktif.length
      ? aktif.map(a=>`<option value="${a.phone}">${a.nama} (${a.phone})</option>`).join("")
      : `<option value="">-- Tidak ada akun online --</option>`;
  } catch {}
}

async function _isiSelectAkunTersedia(id) {
  try {
    const tersedia = await _get("/akun/tersedia");
    const semua    = await _get("/akun");
    const sel      = document.getElementById(id);
    if (!tersedia.length) {
      sel.innerHTML = `<option value="">-- Tidak ada akun tersedia --</option>`; return;
    }
    sel.innerHTML = tersedia.map(phone => {
      const info = semua.find(a => a.phone === phone) || {};
      return `<option value="${phone}">${info.nama||phone} (${phone})</option>`;
    }).join("");
  } catch {}
}

async function _isiSelectGrup(id) {
  try {
    const data = await _get("/grup/aktif");
    const sel  = document.getElementById(id);
    sel.innerHTML = data.length
      ? data.map(g=>`<option value="${g.id}">${g.nama}</option>`).join("")
      : `<option value="">-- Belum ada grup aktif --</option>`;
  } catch {}
}

// ── INIT ─────────────────────────────────────────────────
muatSidebarSummary();
tampilTab("akun");
setInterval(muatSidebarSummary, 60000);
