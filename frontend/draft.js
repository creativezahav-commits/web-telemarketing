async function muatDraft() {
    setLoading("list-draft");
    try {
        const data = await _get("/draft");
        const el   = document.getElementById("list-draft");
        if (!data.length) {
            el.innerHTML = `<div class="empty-state"><div class="icon">📝</div><p>Belum ada draft.</p></div>`;
            return;
        }
        el.innerHTML = data.map(d => {
            const isAktif = d.aktif === 1;
            return `
                <div class="draft-card ${isAktif ? 'draft-aktif-card' : ''}">
                    <div class="draft-header">
                        <div style="display:flex;align-items:center;gap:10px">
                            ${isAktif ? `<span class="badge-pesan-aktif">✅ PESAN AKTIF</span>` : ''}
                            <span class="draft-judul">${d.judul}</span>
                        </div>
                        <span class="draft-tanggal">${d.dibuat}</span>
                    </div>
                    <div class="draft-isi">${d.isi}</div>
                    <div class="draft-footer">
                        <div style="display:flex;gap:8px;flex-wrap:wrap">
                            ${!isAktif
                                ? `<button class="btn-success btn-sm" onclick="jadikanAktif(${d.id})">⚡ Jadikan Aktif</button>`
                                : `<span class="btn-sm" style="background:#dcfce7;color:#166534;border:1px solid #bbf7d0;padding:5px 10px;border-radius:8px;font-size:12px">✅ Sedang Aktif</span>`}
                            <button class="btn-outline btn-sm" onclick="salinIsi(${d.id})">📋 Salin</button>
                            <button class="btn-danger btn-sm" onclick="hapusDraftItem(${d.id})">🗑️ Hapus</button>
                        </div>
                    </div>
                </div>`;
        }).join("");
    } catch {
        document.getElementById("list-draft").innerHTML =
            `<div class="empty-state"><div class="icon">⚠️</div><p>Gagal muat draft.</p></div>`;
    }
}

async function simpanDraft() {
    const judul = document.getElementById("draft-judul").value.trim();
    const isi   = document.getElementById("draft-isi").value.trim();
    if (!judul) { tampilPesan("pesan-draft","⚠️ Judul wajib diisi.","gagal"); return; }
    if (!isi)   { tampilPesan("pesan-draft","⚠️ Isi pesan wajib diisi.","gagal"); return; }
    tampilPesan("pesan-draft","⏳ Menyimpan...","info");
    try {
        await _post("/draft", { judul, isi });
        tampilPesan("pesan-draft","✅ Draft tersimpan!","berhasil");
        document.getElementById("draft-judul").value = "";
        document.getElementById("draft-isi").value   = "";
        muatDraft();
    } catch {
        tampilPesan("pesan-draft","❌ Gagal simpan.","gagal");
    }
}

async function jadikanAktif(draft_id) {
    try {
        await _post(`/draft/${draft_id}/aktif`, {});
        muatDraft();
        await syncPesanAktif();
    } catch { alert("Gagal set pesan aktif."); }
}

async function hapusDraftItem(draft_id) {
    if (!confirm("Hapus draft ini?")) return;
    await _del(`/draft/${draft_id}`);
    muatDraft();
}

function salinIsi(draft_id) {
    _get("/draft").then(data => {
        const d = data.find(x => x.id === draft_id);
        if (!d) return;
        navigator.clipboard.writeText(d.isi).then(() => alert("✅ Disalin ke clipboard!"));
    });
}

async function syncPesanAktif() {
    try {
        const draft = await _get("/draft/aktif");
        const targets = ["isi-pesan","antrian-pesan","broadcast-pesan"];
        targets.forEach(id => {
            const el = document.getElementById(id);
            if (el && !el.dataset.manual && draft && draft.isi) {
                el.value = draft.isi;
            }
        });
        _updateIndikatorPesanAktif(draft);
    } catch {}
}

function _updateIndikatorPesanAktif(draft) {
    const map = {
        "indikator-pesan-kirim"     : "isi-pesan",
        "indikator-pesan-antrian"   : "antrian-pesan",
        "indikator-pesan-broadcast" : "broadcast-pesan",
    };
    Object.entries(map).forEach(([indId, taId]) => {
        const el = document.getElementById(indId);
        if (!el) return;
        if (draft && draft.isi) {
            el.innerHTML = `
                <div class="pesan-aktif-banner">
                    📝 Pakai pesan aktif: <strong>${draft.judul}</strong>
                    <button class="btn-sm btn-outline" style="margin-left:8px"
                            onclick="aktifkanEditManual('${taId}')">✏️ Edit Manual</button>
                    <button class="btn-sm btn-outline" style="margin-left:4px"
                            onclick="resetKePesanAktif('${taId}')">↩️ Reset</button>
                </div>`;
        } else {
            el.innerHTML = `<div style="font-size:12px;color:#aaa;padding:6px 0">
                Belum ada pesan aktif — set di tab Draft.</div>`;
        }
    });
}

function aktifkanEditManual(textareaId) {
    const el = document.getElementById(textareaId);
    if (el) {
        el.dataset.manual = "1";
        el.focus();
        el.style.borderColor = "#f59e0b";
        el.style.boxShadow = "0 0 0 3px rgba(245,158,11,0.2)";
    }
}

function resetKePesanAktif(textareaId) {
    const el = document.getElementById(textareaId);
    if (el) {
        delete el.dataset.manual;
        el.style.borderColor = "";
        el.style.boxShadow = "";
    }
    syncPesanAktif();
}

// muatTabDraft dipanggil sistem tab saat tab Draft dibuka
async function muatTabDraft() {
    await muatDraft();
    await syncPesanAktif();
}
