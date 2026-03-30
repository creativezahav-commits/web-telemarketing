from telethon.tl.types import Channel, Chat
from services.account_manager import get_client
from utils.storage_db import simpan_banyak_grup

async def fetch_grup_dari_akun(phone: str) -> list:
    client = get_client(phone)
    if not client:
        return []
    semua = []
    async for dialog in client.iter_dialogs():
        e = dialog.entity
        if isinstance(e, (Channel, Chat)):
            username = getattr(e, "username", None)
            semua.append({
                "id"           : e.id,
                "nama"         : e.title,
                "username"     : username,
                "tipe"         : _tipe(e),
                "jumlah_member": getattr(e, "participants_count", None),
                "link"         : f"https://t.me/{username}" if username else None,
                "status"       : "active"
            })
    return semua

def _tipe(e) -> str:
    if isinstance(e, Channel):
        if e.megagroup: return "supergroup"
        if e.broadcast: return "channel"
    return "group"
