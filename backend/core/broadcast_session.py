# ============================================================
# core/broadcast_session.py v4
# Semi-paralel: setiap akun punya thread & loop sendiri
# ============================================================

import asyncio, random, threading
from datetime import datetime
from utils.settings_manager import get_int as get_setting_int
from utils.storage_db import catat_riwayat, tandai_grup_masa_istirahat, update_last_kirim_grup

_sesi_aktif: dict = {}


async def _resolve_entity(client, gid: int):
    """
    Resolve grup_id ke entity Telethon dengan benar.

    Masalah: SearchRequest / scraper menyimpan ID Channel sebagai angka positif
    (misal 1366400674), tapi Telethon butuh PeerChannel atau ID negatif (-1001366400674).
    Kalau langsung get_entity(1366400674) → Telethon anggap PeerUser → error.

    Solusi: coba beberapa cara secara berurutan sampai berhasil.
    """
    from telethon.tl.types import PeerChannel, PeerChat

    gid = int(gid)

    # Cara 1: coba langsung (works kalau ID sudah dalam cache Telethon)
    try:
        return await client.get_entity(gid)
    except Exception:
        pass

    # Cara 2: coba sebagai PeerChannel (supergroup/channel positif)
    if gid > 0:
        try:
            return await client.get_entity(PeerChannel(gid))
        except Exception:
            pass

    # Cara 3: coba ID negatif format channel (-1001234567890)
    if gid > 0:
        try:
            return await client.get_entity(-(1000000000000 + gid))
        except Exception:
            pass

    # Cara 4: coba sebagai PeerChat (basic group, ID negatif kecil)
    if gid < 0:
        try:
            return await client.get_entity(PeerChat(-gid))
        except Exception:
            pass

    # Cara 5: coba username dari database kalau ada
    from utils.storage_db import get_semua_grup
    try:
        grups = get_semua_grup()
        target = next((g for g in grups if g.get("id") == gid), None)
        if target and target.get("username"):
            return await client.get_entity(target["username"])
    except Exception:
        pass

    raise ValueError(f"Tidak bisa resolve entity untuk grup ID {gid}")


def buat_sesi(daftar_phone, grup_list, pesan, jeda):
    sid = f"sesi_{datetime.now().strftime('%H%M%S')}_{daftar_phone[0][-4:]}"
    _sesi_aktif[sid] = {
        "session_id"   : sid,
        "daftar_phone" : daftar_phone,
        "pesan"        : pesan,
        "jeda"         : jeda,
        "status"       : "menunggu",
        "total"        : len(grup_list),
        "selesai"      : 0,
        "hasil"        : [],
        "mulai"        : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "grup_list"    : grup_list,
        "grup_per_akun": {},
        "stop"         : False,
        "countdown"    : 0
    }
    return sid


def get_sesi(sid):    return _sesi_aktif.get(sid)
def get_semua_sesi(): return list(_sesi_aktif.values())
def stop_sesi(sid):
    if sid in _sesi_aktif: _sesi_aktif[sid]["stop"] = True
def hapus_sesi(sid):
    if sid in _sesi_aktif: del _sesi_aktif[sid]


def jalankan_sesi_thread(session_id: str, semua_client: dict):
    """
    Semi-paralel: setiap akun berjalan di thread terpisah.
    Masing-masing kirim ke grup miliknya sendiri.
    """
    from services.account_manager import _loop

    sesi = _sesi_aktif.get(session_id)
    if not sesi: return

    grup_per_akun = sesi.get("grup_per_akun", {})

    if grup_per_akun:
        # Mode per-akun: setiap akun punya thread sendiri
        sesi["status"] = "berjalan"
        threads = []
        for phone, grup_list in grup_per_akun.items():
            client = semua_client.get(phone)
            if not client:
                print(f"[Broadcast] Akun {phone} tidak online, skip")
                continue
            future = asyncio.run_coroutine_threadsafe(
                _kirim_per_akun(session_id, phone, grup_list, client),
                _loop
            )
            def watch(f=future, p=phone):
                try: f.result(timeout=7200)
                except Exception as e: print(f"[Broadcast] {p} error: {e}")
            t = threading.Thread(target=watch, daemon=True)
            t.start()
            threads.append(t)
    else:
        # Mode lama (fallback)
        future = asyncio.run_coroutine_threadsafe(
            _kirim(session_id, semua_client), _loop
        )
        def watch():
            try: future.result(timeout=7200)
            except Exception as e: print(f"[Broadcast] Error: {e}")
        threading.Thread(target=watch, daemon=True).start()


async def _kirim_per_akun(session_id: str, phone: str, grup_list: list, client):
    """Kirim pesan ke daftar grup menggunakan 1 akun."""
    sesi = _sesi_aktif.get(session_id)
    if not sesi: return

    jeda_min = 20
    jeda_max = 60

    print(f"[Broadcast] {phone} mulai kirim ke {len(grup_list)} grup")

    for i, grup in enumerate(grup_list):
        if sesi["stop"]: break

        gid  = grup["id"]
        nama = grup["nama"]

        # Cek batas warming sebelum kirim
        try:
            from core.warming import get_daily_capacity
            kapasitas = get_daily_capacity(phone)
            sudah = int(kapasitas.get("kirim", {}).get("used") or 0)
            batas = int(kapasitas.get("kirim", {}).get("limit") or 0)
            sisa = int(kapasitas.get("kirim", {}).get("remaining") or 0)
            if batas > 0 and sudah >= batas:
                print(f"[Broadcast] {phone} sudah mencapai batas harian ({sudah}/{batas}, sisa {sisa}), berhenti")
                _update(sesi, {"id": gid, "nama": nama}, "skip", phone, f"Batas harian tercapai ({sudah}/{batas}, sisa {sisa})")
                break  # akun ini sudah habis kuota, berhenti
        except Exception:
            pass

        print(f"[Broadcast] {phone} [{i+1}/{len(grup_list)}] {nama}")

        _update(sesi, {"id":gid,"nama":nama}, "mengirim", phone, "Mengirim...")

        try:
            entity = await _resolve_entity(client, gid)
            await client.send_message(entity, sesi["pesan"])
            print(f"[Broadcast] ✅ {nama}")
            _update(sesi, {"id":gid,"nama":nama}, "berhasil", phone, "Terkirim")
            sesi["selesai"] += 1
            catat_riwayat(phone, gid, nama, "send_success")
            update_last_kirim_grup(gid)
            cooldown_minutes = get_setting_int('broadcast_cooldown_grup_menit', get_setting_int('campaign_group_cooldown_minutes', 0))
            cooldown_hours = get_setting_int('broadcast_cooldown_grup_jam', get_setting_int('campaign_group_cooldown_hours', 0))
            tandai_grup_masa_istirahat(gid, cooldown_hours=cooldown_hours, cooldown_minutes=cooldown_minutes)
            catat_riwayat(phone, gid, nama, 'cooldown_started')

        except Exception as e:
            err = str(e)[:150]
            print(f"[Broadcast] ❌ {nama} — {err}")
            _update(sesi, {"id":gid,"nama":nama}, "gagal", phone, err)
            catat_riwayat(phone, gid, nama, "send_failed", err)
            # Kalau error karena tidak bisa kirim (channel/belum join) → skip jeda
            # supaya tidak buang waktu tunggu 30-60 detik untuk grup yang memang tidak bisa
            err_lower = err.lower()
            if any(x in err_lower for x in [
                "can't write", "write_forbidden", "forbidden",
                "not a member", "chat_write_forbidden"
            ]):
                print(f"[Broadcast] ⏭️ {nama} dilewati (tidak bisa kirim), skip jeda")
                continue  # langsung ke grup berikutnya tanpa jeda

        if i < len(grup_list) - 1 and not sesi["stop"]:
            jeda = random.randint(jeda_min, jeda_max)
            print(f"[Broadcast] {phone} jeda {jeda}s...")
            for d in range(jeda, 0, -1):
                if sesi["stop"]: break
                sesi["countdown"] = d
                await asyncio.sleep(1)
            sesi["countdown"] = 0

    print(f"[Broadcast] {phone} selesai!")

    # Cek apakah semua akun sudah selesai
    total_hasil = len(sesi["hasil"])
    if total_hasil >= sesi["total"]:
        sesi["status"] = "selesai"
        sesi["selesai_pada"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def _kirim(session_id: str, semua_client: dict):
    """Fallback mode lama."""
    sesi = _sesi_aktif.get(session_id)
    if not sesi: return
    sesi["status"] = "berjalan"
    phones = sesi["daftar_phone"]
    n      = len(phones)
    jeda   = sesi["jeda"]

    for i, grup in enumerate(sesi["grup_list"]):
        if sesi["stop"]: sesi["status"] = "dihentikan"; break
        phone  = phones[i % n]
        client = semua_client.get(phone)
        gid, nama = grup["id"], grup["nama"]
        if not client:
            _update(sesi, grup, "skip", phone, "Akun tidak tersedia")
            continue
        _update(sesi, grup, "mengirim", phone, "Mengirim...")
        try:
            entity = await _resolve_entity(client, gid)
            await client.send_message(entity, sesi["pesan"])
            _update(sesi, grup, "berhasil", phone, "Terkirim")
            sesi["selesai"] += 1
            catat_riwayat(phone, gid, nama, "send_success")
            update_last_kirim_grup(gid)
            cooldown_minutes = get_setting_int('broadcast_cooldown_grup_menit', get_setting_int('campaign_group_cooldown_minutes', 0))
            cooldown_hours = get_setting_int('broadcast_cooldown_grup_jam', get_setting_int('campaign_group_cooldown_hours', 0))
            tandai_grup_masa_istirahat(gid, cooldown_hours=cooldown_hours, cooldown_minutes=cooldown_minutes)
            catat_riwayat(phone, gid, nama, 'cooldown_started')
        except Exception as e:
            err = str(e)[:150]
            _update(sesi, grup, "gagal", phone, err)
            catat_riwayat(phone, gid, nama, "send_failed", err)
        if i < len(sesi["grup_list"]) - 1 and not sesi["stop"]:
            j = random.randint(max(1,jeda-5), jeda+5)
            for d in range(j, 0, -1):
                if sesi["stop"]: break
                sesi["countdown"] = d
                await asyncio.sleep(1)
            sesi["countdown"] = 0

    if sesi["status"] == "berjalan": sesi["status"] = "selesai"
    sesi["selesai_pada"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def jalankan_sesi(sid, clients):
    await _kirim(sid, clients)


def _update(sesi, grup, status, phone, pesan):
    for h in sesi["hasil"]:
        if h["grup_id"] == grup["id"]:
            h.update({"status":status,"phone":phone,"pesan":pesan,
                      "waktu":datetime.now().strftime("%H:%M:%S")}); return
    sesi["hasil"].append({"grup_id":grup["id"],"nama_grup":grup["nama"],
        "status":status,"phone":phone,"pesan":pesan,
        "waktu":datetime.now().strftime("%H:%M:%S")})
