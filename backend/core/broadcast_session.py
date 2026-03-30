# ============================================================
# core/broadcast_session.py
# Tugasnya: kelola sesi kirim massal
# Operator pilih grup → sistem kirim satu per satu dengan jeda
# ============================================================

import asyncio
import random
from datetime import datetime

# Simpan sesi aktif di memory
# Format: { session_id: { status, progress, hasil, ... } }
_sesi_aktif: dict = {}


def buat_sesi(phone: str, grup_list: list, pesan: str, jeda: int) -> str:
    """
    Buat sesi kirim baru.
    Return: session_id unik
    """
    session_id = f"sesi_{datetime.now().strftime('%H%M%S')}_{phone[-4:]}"

    _sesi_aktif[session_id] = {
        "session_id" : session_id,
        "phone"      : phone,
        "pesan"      : pesan,
        "jeda"       : jeda,
        "status"     : "menunggu",   # menunggu / berjalan / selesai / dihentikan
        "total"      : len(grup_list),
        "selesai"    : 0,
        "hasil"      : [],           # log per grup
        "mulai"      : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "grup_list"  : grup_list,    # list of { id, nama }
        "stop"       : False         # flag untuk hentikan
    }

    return session_id


def get_sesi(session_id: str) -> dict:
    """Ambil info sesi."""
    return _sesi_aktif.get(session_id)


def get_semua_sesi() -> list:
    """Ambil semua sesi (aktif maupun selesai)."""
    return list(_sesi_aktif.values())


def stop_sesi(session_id: str):
    """Tandai sesi untuk dihentikan."""
    if session_id in _sesi_aktif:
        _sesi_aktif[session_id]["stop"] = True


def hapus_sesi(session_id: str):
    """Hapus sesi dari memory."""
    if session_id in _sesi_aktif:
        del _sesi_aktif[session_id]


async def jalankan_sesi(session_id: str, client):
    """
    Jalankan pengiriman satu per satu dengan jeda.
    Dipanggil sebagai background task.
    """
    sesi = _sesi_aktif.get(session_id)
    if not sesi:
        return

    sesi["status"] = "berjalan"
    jeda           = sesi["jeda"]

    for i, grup in enumerate(sesi["grup_list"]):

        # Cek apakah operator minta stop
        if sesi["stop"]:
            sesi["status"] = "dihentikan"
            _catat_hasil(sesi, grup, "dihentikan", "Dihentikan oleh operator")
            break

        grup_id   = grup["id"]
        nama_grup = grup["nama"]

        # Tandai sedang dikirim
        _catat_hasil(sesi, grup, "mengirim", "Sedang mengirim...")

        try:
            entity = await client.get_entity(int(grup_id))
            await client.send_message(entity, sesi["pesan"])

            # Berhasil
            _update_hasil(sesi, grup_id, "berhasil", "Terkirim")
            sesi["selesai"] += 1

        except Exception as e:
            # Gagal — lanjut ke grup berikutnya
            _update_hasil(sesi, grup_id, "gagal", str(e))

        # Jeda sebelum grup berikutnya (kecuali grup terakhir)
        if i < len(sesi["grup_list"]) - 1 and not sesi["stop"]:
            # Tambah variasi kecil agar lebih natural
            jeda_aktual = jeda + random.randint(-3, 5)
            jeda_aktual = max(10, jeda_aktual)  # minimal 10 detik

            # Update countdown di sesi
            sesi["countdown"] = jeda_aktual
            await asyncio.sleep(jeda_aktual)
            sesi["countdown"] = 0

    # Selesai
    if sesi["status"] == "berjalan":
        sesi["status"] = "selesai"

    sesi["selesai_pada"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ── INTERNAL ──────────────────────────────────────────────
def _catat_hasil(sesi, grup, status, pesan):
    """Tambah atau update hasil per grup."""
    for h in sesi["hasil"]:
        if h["grup_id"] == grup["id"]:
            h["status"] = status
            h["pesan"]  = pesan
            h["waktu"]  = datetime.now().strftime("%H:%M:%S")
            return
    sesi["hasil"].append({
        "grup_id"  : grup["id"],
        "nama_grup": grup["nama"],
        "status"   : status,
        "pesan"    : pesan,
        "waktu"    : datetime.now().strftime("%H:%M:%S")
    })


def _update_hasil(sesi, grup_id, status, pesan):
    """Update status hasil yang sudah ada."""
    for h in sesi["hasil"]:
        if h["grup_id"] == grup_id:
            h["status"] = status
            h["pesan"]  = pesan
            h["waktu"]  = datetime.now().strftime("%H:%M:%S")
            return
