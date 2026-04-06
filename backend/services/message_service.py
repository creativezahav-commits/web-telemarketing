from __future__ import annotations

import asyncio
import random
from datetime import datetime

from telethon.errors import (
    ChannelPrivateError,
    ChatWriteForbiddenError,
    FloodWaitError,
    UserBannedInChannelError,
)
from telethon.tl.types import PeerChannel, PeerChat

from core.warming import get_daily_capacity, get_jeda_kirim
from services.account_manager import get_client
from utils.settings_manager import get_int as get_setting_int
from utils.storage_db import (
    catat_riwayat,
    tandai_grup_masa_istirahat,
    update_last_kirim_grup,
    set_broadcast_throttle,   # ✅ BARU
)


async def _resolve_entity(client, grup_id: int):
    """
    Resolve grup_id ke entity Telethon dengan benar.
    Scraper menyimpan ID Channel sebagai positif (1366400674),
    tapi Telethon butuh PeerChannel atau -1001366400674.
    """
    gid = int(grup_id)
    try:
        return await client.get_entity(gid)
    except Exception:
        pass
    if gid > 0:
        try:
            return await client.get_entity(PeerChannel(gid))
        except Exception:
            pass
    if gid > 0:
        try:
            return await client.get_entity(-(1000000000000 + gid))
        except Exception:
            pass
    if gid < 0:
        try:
            return await client.get_entity(PeerChat(-gid))
        except Exception:
            pass
    from utils.storage_db import get_semua_grup
    try:
        grups = get_semua_grup()
        target = next((g for g in grups if g.get("id") == gid), None)
        if target and target.get("username"):
            return await client.get_entity(target["username"])
    except Exception:
        pass
    raise ValueError(f"Grup ID {gid} tidak ditemukan – akun mungkin belum join grup ini")


async def kirim_pesan_manual(phone: str, grup_id: int, pesan: str) -> dict:
    client = get_client(phone)
    if not client:
        return {"status": "error", "pesan": "Akun tidak aktif. Login dulu."}

    kapasitas = get_daily_capacity(phone)
    batas = int(kapasitas.get("kirim", {}).get("limit") or 0)
    sudah = int(kapasitas.get("kirim", {}).get("used") or 0)
    sisa  = int(kapasitas.get("kirim", {}).get("remaining") or 0)
    if batas > 0 and sudah >= batas:
        return {"status": "gagal", "pesan": f"Batas harian tercapai ({sudah}/{batas}, sisa {sisa})."}

    nama_grup = str(grup_id)
    try:
        jeda = max(1, random.uniform(1, get_jeda_kirim(phone)))
        await asyncio.sleep(jeda)
        entity    = await _resolve_entity(client, int(grup_id))
        nama_grup = getattr(entity, "title", str(grup_id))
        await client.send_message(entity, pesan)

        catat_riwayat(phone, grup_id, nama_grup, "send_success")
        update_last_kirim_grup(grup_id)

        cooldown_minutes = get_setting_int('broadcast_cooldown_grup_menit',
                           get_setting_int('campaign_group_cooldown_minutes', 0))
        cooldown_hours   = get_setting_int('broadcast_cooldown_grup_jam',
                           get_setting_int('campaign_group_cooldown_hours', 0))
        tandai_grup_masa_istirahat(grup_id,
                                   cooldown_hours=cooldown_hours,
                                   cooldown_minutes=cooldown_minutes)
        catat_riwayat(phone, grup_id, nama_grup, 'cooldown_started')

        # ✅ BARU: catat throttle broadcast per-akun setelah kirim sukses
        jeda_next = max(1, random.uniform(1, get_jeda_kirim(phone)))
        set_broadcast_throttle(phone, nama_grup, jeda_next)

        return {
            "status": "berhasil",
            "akun":   phone,
            "grup":   nama_grup,
            "waktu":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    except FloodWaitError as e:
        catat_riwayat(phone, grup_id, nama_grup, "send_failed", f"Flood wait {e.seconds} detik")
        return {"status": "gagal", "pesan": f"Flood wait {e.seconds} detik."}
    except ChatWriteForbiddenError:
        catat_riwayat(phone, grup_id, nama_grup, "send_failed", "Tidak punya izin kirim")
        return {"status": "gagal", "pesan": "Tidak punya izin kirim."}
    except UserBannedInChannelError:
        catat_riwayat(phone, grup_id, nama_grup, "send_failed", "Akun dibanned di grup ini")
        return {"status": "gagal", "pesan": "Akun dibanned di grup ini."}
    except ChannelPrivateError:
        catat_riwayat(phone, grup_id, nama_grup, "send_failed", "Grup sudah private")
        return {"status": "gagal", "pesan": "Grup sudah private."}
    except Exception as e:
        catat_riwayat(phone, grup_id, nama_grup, "send_failed", str(e))
        return {"status": "error", "pesan": str(e)}
