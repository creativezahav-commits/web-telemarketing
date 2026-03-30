# ============================================================
# services/account_manager.py
# Login akun Telegram dengan support OTP via dashboard
# ============================================================

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
import config
from utils.storage_db import simpan_akun, get_semua_akun, set_status_akun

# Simpan client aktif di memory
_clients: dict = {}

# Simpan client yang sedang menunggu OTP
# Format: { phone: client }
_pending_otp: dict = {}


async def login_akun(phone: str) -> dict:
    """
    Step 1: Kirim OTP ke nomor HP.
    Kalau session sudah ada → langsung login tanpa OTP.
    """
    session = config.get_session_name(phone)
    client  = TelegramClient(session, config.API_ID, config.API_HASH)

    try:
        await client.connect()

        # Kalau sudah pernah login sebelumnya
        if await client.is_user_authorized():
            me = await client.get_me()
            _clients[phone] = client
            simpan_akun(phone, me.first_name, me.username)
            return {
                "status"  : "aktif",
                "phone"   : phone,
                "nama"    : me.first_name,
                "username": me.username or "-"
            }

        # Belum login → kirim OTP
        await client.send_code_request(phone)

        # Simpan client di pending sambil tunggu OTP
        _pending_otp[phone] = client

        return {
            "status": "perlu_otp",
            "pesan" : "Kode OTP sudah dikirim ke Telegram kamu.",
            "phone" : phone
        }

    except Exception as e:
        return {"status": "error", "pesan": str(e)}


async def submit_otp(phone: str, kode: str, password: str = None) -> dict:
    """
    Step 2: Submit kode OTP yang diterima user.
    Kalau akun punya 2FA → perlu password juga.
    """
    client = _pending_otp.get(phone)
    if not client:
        return {
            "status": "error",
            "pesan" : "Sesi login tidak ditemukan. Coba login ulang."
        }

    try:
        # Submit kode OTP
        await client.sign_in(phone, kode)

        # Berhasil login
        me = await client.get_me()
        _clients[phone] = client

        # Hapus dari pending
        del _pending_otp[phone]

        # Simpan ke database
        simpan_akun(phone, me.first_name, me.username)

        return {
            "status"  : "aktif",
            "phone"   : phone,
            "nama"    : me.first_name,
            "username": me.username or "-"
        }

    except SessionPasswordNeededError:
        # Akun punya 2FA — minta password
        if password:
            try:
                await client.sign_in(password=password)
                me = await client.get_me()
                _clients[phone] = client
                del _pending_otp[phone]
                simpan_akun(phone, me.first_name, me.username)
                return {
                    "status"  : "aktif",
                    "phone"   : phone,
                    "nama"    : me.first_name,
                    "username": me.username or "-"
                }
            except Exception as e:
                return {"status": "error", "pesan": f"Password 2FA salah: {str(e)}"}
        else:
            return {
                "status": "perlu_2fa",
                "pesan" : "Akun ini punya password 2FA. Masukkan password Telegram kamu.",
                "phone" : phone
            }

    except Exception as e:
        return {"status": "error", "pesan": f"OTP salah atau expired: {str(e)}"}


async def logout_akun(phone: str) -> dict:
    if phone in _clients:
        await _clients[phone].disconnect()
        del _clients[phone]
    return {"status": "ok", "pesan": f"{phone} logout."}


def cek_status_semua() -> list:
    data = get_semua_akun()
    for a in data:
        a["online"] = a["phone"] in _clients
    return data


def get_client(phone: str):
    return _clients.get(phone)
