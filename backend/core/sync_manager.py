# Sinkronisasi akun – auto-join grup Hot yang belum diikuti
import asyncio
from utils.storage_db import get_grup_hot, catat_riwayat, hitung_join_hari_ini, set_join_throttle
from core.warming import get_batas_join, get_jeda_join
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import FloodWaitError, UserAlreadyParticipantError

_sesi_sync: dict = {}

def buat_sesi_sync(phone: str, grup_list: list) -> str:
    sid = f"sync_{phone[-4:]}"
    _sesi_sync[sid] = {
        "phone": phone, "total": len(grup_list),
        "selesai": 0, "status": "menunggu",
        "hasil": [], "grup_list": grup_list, "stop": False
    }
    return sid

def get_sesi_sync(sid): return _sesi_sync.get(sid)
def stop_sesi_sync(sid):
    if sid in _sesi_sync: _sesi_sync[sid]["stop"] = True

async def jalankan_sync(sid: str, client):
    sesi  = _sesi_sync.get(sid)
    if not sesi: return
    sesi["status"] = "berjalan"
    phone = sesi["phone"]
    jeda  = get_jeda_join(phone)

    for i, grup in enumerate(sesi["grup_list"]):
        if sesi["stop"]: sesi["status"] = "dihentikan"; break
        batas = get_batas_join(phone)
        if hitung_join_hari_ini(phone) >= batas:
            sesi["status"] = "selesai_batas"
            break
        try:
            if grup.get("username"):
                entity = await client.get_entity(grup["username"])
                await client(JoinChannelRequest(entity))
            sesi["hasil"].append({"grup": grup["nama"], "status": "join"})
            sesi["selesai"] += 1
            catat_riwayat(phone, grup["id"], grup["nama"], "join_success")

            # ✅ BARU: catat throttle join setelah berhasil join
            set_join_throttle(phone, grup["nama"], jeda)

        except UserAlreadyParticipantError:
            sesi["hasil"].append({"grup": grup["nama"], "status": "sudah_join"})
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
            continue
        except Exception as e:
            sesi["hasil"].append({"grup": grup["nama"], "status": "gagal", "pesan": str(e)})
            catat_riwayat(phone, grup["id"], grup["nama"], "join_failed", str(e)[:150])

        if i < len(sesi["grup_list"]) - 1:
            await asyncio.sleep(jeda)

    if sesi["status"] == "berjalan": sesi["status"] = "selesai"
