async function muatTabDiscovery() {
    await _isiSelectAkun("discovery-akun");
    await _isiSelectAkun("discovery-akun-manual");
}

async function fetchGrupBaru() {
    const phone = document.getElementById("discovery-akun").value;
    if (!phone) { tampilPesan("pesan-discovery","⚠️ Pilih akun dulu.","gagal"); return; }
    tampilPesan("pesan-discovery","⏳ Mencari grup baru...","info");
    setLoading("list-discovery","Mencari...");
    try {
        const data = await _post("/grup/fetch/baru", { phone });
        tampilPesan("pesan-discovery",`✅ Ditemukan ${data.length} grup baru.`,"berhasil");
        tampilHasilDiscovery(data);
    } catch {
        tampilPesan("pesan-discovery","❌ Gagal fetch.","gagal");
    }
}

async function tambahGrupManual() {
    const phone = document.getElementById("discovery-akun-manual").value;
    const link  = document.getElementById("discovery-link").value.trim();
    if (!phone) { tampilPesan("pesan-discovery-manual","⚠️ Pilih akun.","gagal"); return; }
    if (!link)  { tampilPesan("pesan-discovery-manual","⚠️ Isi username/link.","gagal"); return; }
    tampilPesan("pesan-discovery-manual","⏳ Mengambil info grup...","info");
    try {
        const r = await _post("/grup/tambah-manual", { phone, link });
        if (r.ok) {
            tampilPesan("pesan-discovery-manual",`✅ Berhasil tambah: ${r.grup.nama}`,"berhasil");
            document.getElementById("discovery-link").value = "";
        } else {
            tampilPesan("pesan-discovery-manual",`❌ ${r.error}`,"gagal");
        }
    } catch {
        tampilPesan("pesan-discovery-manual","❌ Gagal.","gagal");
    }
}

function tampilHasilDiscovery(data) {
    const el = document.getElementById("list-discovery");
    if (!data.length) {
        el.innerHTML = `<div class="empty-state"><div class="icon">✅</div><p>Semua grup sudah ada di database!</p></div>`;
        return;
    }
    el.innerHTML = `
        <div style="margin-bottom:12px;font-size:13px;color:#555">
            ${data.length} grup baru ditemukan — pilih yang ingin ditambahkan ke sistem:
        </div>` +
        data.map(g => `
            <div class="kartu" id="discovery-item-${g.id}">
                <div>
                    <strong>${g.nama}</strong>
                    <small>${g.tipe} · ${g.jumlah_member ? g.jumlah_member.toLocaleString()+' member' : '?'}</small>
                    ${g.link ? `<small><a href="${g.link}" target="_blank" style="color:#2563eb">${g.link}</a></small>` : ''}
                </div>
                <button class="btn-success btn-sm" onclick="tambahKeDB(decodeURIComponent('${encodeURIComponent(JSON.stringify(g))}'))">
                    ➕ Tambah ke Sistem
                </button>
            </div>`).join("");
}

async function tambahKeDB(rawGrup) {
    const grup = typeof rawGrup === 'string' ? JSON.parse(rawGrup) : rawGrup;
    try {
        const r = await _post("/grup/tambah", { ...grup, sumber: "discovery" });
        if (!r.ok) throw new Error(r.error || "Gagal menyimpan grup");

        const el = document.getElementById(`discovery-item-${grup.id}`);
        if (el) {
            el.style.opacity = "0.5";
            const btn = el.querySelector("button");
            if (btn) {
                btn.textContent = "✅ Ditambahkan";
                btn.disabled = true;
            }
        }
    } catch (err) {
        alert(err.message || "Gagal menambahkan grup");
    }
}
