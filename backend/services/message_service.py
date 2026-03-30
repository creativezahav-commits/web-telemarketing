from datetime import datetime
from telethon.errors import FloodWaitError, ChatWriteForbiddenError, UserBannedInChannelError, ChannelPrivateError
from services.account_manager import get_client
from utils.storage_db import catat_riwayat, get_riwayat_hari_ini
from utils.storage_db import hitung_kirim_hari_ini
from core.smart_sender import delay_sebelum_kirim

BATAS = 30

async def kirim_pesan_manual(phone: str, grup_id: int, pesan: str) -> dict:
    client = get_client(phone)
    if not client:
        return {"status": "error", "pesan": "Akun tidak aktif. Login dulu."}

    if hitung_kirim_hari_ini(phone) >= BATAS:
        return {"status": "gagal", "pesan": "Batas harian tercapai."}

    try:
        await delay_sebelum_kirim()
        entity    = await client.get_entity(int(grup_id))
        await client.send_message(entity, pesan)
        nama_grup = getattr(entity, "title", str(grup_id))
        catat_riwayat(phone, grup_id, nama_grup, "berhasil")
        return {
            "status": "berhasil",
            "akun"  : phone,
            "grup"  : nama_grup,
            "waktu" : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except FloodWaitError as e:
        return {"status": "gagal", "pesan": f"Flood wait {e.seconds} detik."}
    except ChatWriteForbiddenError:
        catat_riwayat(phone, grup_id, str(grup_id), "gagal")
        return {"status": "gagal", "pesan": "Tidak punya izin kirim."}
    except UserBannedInChannelError:
        return {"status": "gagal", "pesan": "Akun dibanned di grup ini."}
    except ChannelPrivateError:
        return {"status": "gagal", "pesan": "Grup sudah private."}
    except Exception as e:
        catat_riwayat(phone, grup_id, str(grup_id), "gagal")
        return {"status": "error", "pesan": str(e)}
