from __future__ import annotations
from datetime import date as _date

import asyncio
import threading
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import Channel, Chat

import config

_clients: dict = {}
_pending_otp: dict = {}

# Loop Telethon berjalan di thread terpisah.
_loop = asyncio.new_event_loop()


def _start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


_thread = threading.Thread(target=_start_loop, args=(_loop,), daemon=True)
_thread.start()


def run_sync(coro, timeout: int = 120):
    future = asyncio.run_coroutine_threadsafe(coro, _loop)
    try:
        return future.result(timeout=timeout)
    except Exception:
        future.cancel()
        raise


def _normalize_phone(phone: str) -> str:
    return config.normalize_phone(phone)


def _is_supported_group(entity) -> bool:
    if isinstance(entity, Chat):
        return True
    if isinstance(entity, Channel):
        return bool(getattr(entity, "megagroup", False))
    return False


async def _auto_fetch_grup(phone: str, client):
    """Auto-fetch dan sinkronkan grup saat akun login atau reconnect."""
    try:
        from utils.storage_db import simpan_banyak_grup, sinkronkan_relasi_akun_grup

        print(f"   Fetch grup untuk {phone}...")
        semua = []
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            if not _is_supported_group(entity):
                continue

            username = getattr(entity, "username", None)
            semua.append(
                {
                    "id": entity.id,
                    "nama": getattr(entity, "title", str(entity.id)),
                    "username": username,
                    "tipe": "supergroup" if isinstance(entity, Channel) and getattr(entity, "megagroup", False) else "group",
                    "jumlah_member": getattr(entity, "participants_count", 0) or 0,
                    "link": f"https://t.me/{username}" if username else None,
                    "status": "active",
                    "sumber": "auto",
                }
            )

        simpan_banyak_grup(semua, sumber="auto")
        sinkronkan_relasi_akun_grup(phone, [g["id"] for g in semua])
        print(f"   {len(semua)} grup disinkronkan untuk {phone}")
    except Exception as e:
        print(f"   Gagal fetch grup {phone}: {e}")


async def auto_reconnect_semua():
    if not config.has_telegram_credentials():
        print("Lewati auto-reconnect: API_ID/API_HASH belum diatur.")
        return

    session_files = sorted(Path(config.SESSION_DIR).glob("akun_*.session"))
    if not session_files:
        print("Tidak ada session tersimpan.")
        return

    print(f"Auto-reconnect {len(session_files)} akun...")
    for sf in session_files:
        phone = _normalize_phone(sf.stem.replace("akun_", ""))
        session_name = str(sf.with_suffix(""))
        try:
            client = TelegramClient(session_name, config.API_ID, config.API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                me = await client.get_me()
                # Skip akun yang statusnya banned di database
                from utils.database import get_conn
                _conn_chk = get_conn()
                _status_row = _conn_chk.execute(
                    "SELECT status FROM akun WHERE phone=%s", (phone,)
                ).fetchone()
                _conn_chk.close()
                if _status_row and str(_status_row['status']).lower() == 'banned':
                    print(f"   SKIP: {phone} status banned — tidak di-connect")
                    await client.disconnect()
                    continue
                _clients[phone] = client
                from utils.storage_db import simpan_akun

                simpan_akun(phone, me.first_name, me.username,
                            tanggal_buat=str(_date.today()))
                # Update level warming dan hitung score setelah login
                try:

                    from core.warming import update_level_otomatis

                    update_level_otomatis(phone)

                except Exception:

                    pass

                try:

                    from core.scoring import update_score_akun

                    update_score_akun(phone)

                except Exception:

                    pass
                print(f"   OK: {me.first_name} ({phone}) online")
                await _auto_fetch_grup(phone, client)
            else:
                print(f"   SKIP: {phone} session expired")
                await client.disconnect()
        except Exception as e:
            print(f"   GAGAL: {phone} - {str(e)}")
    print(f"Selesai. {len(_clients)} akun online.")


async def login_akun(phone: str) -> dict:
    if not config.has_telegram_credentials():
        return {"status": "error", "pesan": "API_ID / API_HASH belum diatur di file .env"}

    phone = _normalize_phone(phone)
    if not phone:
        return {"status": "error", "pesan": "Nomor HP tidak valid."}

    if phone in _clients:
        try:
            me = await _clients[phone].get_me()
            return {
                "status": "sudah_ada",
                "pesan": f"Akun {me.first_name} ({phone}) sudah login dan online.",
                "phone": phone,
                "nama": me.first_name,
            }
        except Exception:
            pass

    session = config.get_session_name(phone)
    client = _pending_otp.get(phone) or TelegramClient(session, config.API_ID, config.API_HASH)
    try:
        if not client.is_connected():
            await client.connect()
        if await client.is_user_authorized():
            me = await client.get_me()
            _clients[phone] = client
            from utils.storage_db import simpan_akun

            simpan_akun(phone, me.first_name, me.username,
                        tanggal_buat=str(_date.today()))
            # Update level warming dan hitung score setelah login
            try:
                from core.warming import update_level_otomatis
                update_level_otomatis(phone)
            except Exception:
                pass
            try:
                from core.scoring import update_score_akun
                update_score_akun(phone)
            except Exception:
                pass
            await _auto_fetch_grup(phone, client)
            return {"status": "aktif", "phone": phone, "nama": me.first_name, "username": me.username or "-"}
        await client.send_code_request(phone)
        _pending_otp[phone] = client
        return {"status": "perlu_otp", "pesan": "Kode OTP sudah dikirim.", "phone": phone}
    except Exception as e:
        return {"status": "error", "pesan": str(e)}


async def submit_otp(phone: str, kode: str, password: str = None) -> dict:
    phone = _normalize_phone(phone)
    client = _pending_otp.get(phone)
    if not client:
        return {"status": "error", "pesan": "Sesi tidak ditemukan. Coba login ulang."}
    try:
        if not client.is_connected():
            await client.connect()
        await client.sign_in(phone, kode)
        me = await client.get_me()
        _clients[phone] = client
        _pending_otp.pop(phone, None)
        from utils.storage_db import simpan_akun

        simpan_akun(phone, me.first_name, me.username,
                    tanggal_buat=str(_date.today()))
        # Update level warming dan hitung score setelah login
        try:
            from core.warming import update_level_otomatis
            update_level_otomatis(phone)
        except Exception:
            pass
        try:
            from core.scoring import update_score_akun
            update_score_akun(phone)
        except Exception:
            pass
        await _auto_fetch_grup(phone, client)
        return {"status": "aktif", "phone": phone, "nama": me.first_name, "username": me.username or "-"}
    except SessionPasswordNeededError:
        if password:
            try:
                await client.sign_in(password=password)
                me = await client.get_me()
                _clients[phone] = client
                _pending_otp.pop(phone, None)
                from utils.storage_db import simpan_akun

                simpan_akun(phone, me.first_name, me.username,
                            tanggal_buat=str(_date.today()))
                # Update level warming dan hitung score setelah login
                try:

                    from core.warming import update_level_otomatis

                    update_level_otomatis(phone)

                except Exception:

                    pass

                try:

                    from core.scoring import update_score_akun

                    update_score_akun(phone)

                except Exception:

                    pass
                await _auto_fetch_grup(phone, client)
                return {"status": "aktif", "phone": phone, "nama": me.first_name, "username": me.username or "-"}
            except Exception as e:
                return {"status": "error", "pesan": f"Password 2FA salah: {str(e)}"}
        return {"status": "perlu_2fa", "pesan": "Masukkan password 2FA.", "phone": phone}
    except Exception as e:
        return {"status": "error", "pesan": f"OTP salah: {str(e)}"}


async def logout_akun(phone: str) -> dict:
    phone = _normalize_phone(phone)
    client = _clients.pop(phone, None)
    _pending_otp.pop(phone, None)
    if client:
        await client.disconnect()
    return {"status": "ok", "pesan": f"{phone} logout."}


async def delete_akun_permanen(phone: str, *, remove_session_files: bool = True) -> dict:
    from utils.storage_db import delete_akun

    phone = _normalize_phone(phone)
    client = _clients.pop(phone, None)
    pending = _pending_otp.pop(phone, None)
    if client:
        await client.disconnect()
    if pending and pending is not client:
        try:
            await pending.disconnect()
        except Exception:
            pass
    delete_akun(phone)
    deleted_files: list[str] = []
    if remove_session_files:
        session_base = Path(config.get_session_name(phone))
        patterns = [session_base.name + '*']
        for pattern in patterns:
            for fp in session_base.parent.glob(pattern):
                try:
                    fp.unlink(missing_ok=True)
                    deleted_files.append(fp.name)
                except Exception:
                    pass
    return {"status": "ok", "pesan": f"{phone} dihapus dari database.", "deleted_session_files": deleted_files}


def get_client(phone: str):
    return _clients.get(_normalize_phone(phone))
