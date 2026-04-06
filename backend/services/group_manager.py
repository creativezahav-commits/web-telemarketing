from __future__ import annotations

from telethon.tl.types import Channel, Chat
from services.account_manager import get_client


def _is_supported_group(entity) -> bool:
    if isinstance(entity, Chat):
        return True
    if isinstance(entity, Channel):
        return bool(getattr(entity, "megagroup", False))
    return False


async def fetch_grup_dari_akun(phone: str) -> list:
    client = get_client(phone)
    if not client:
        return []

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
                "tipe": _tipe(entity),
                "jumlah_member": getattr(entity, "participants_count", 0) or 0,
                "link": f"https://t.me/{username}" if username else None,
                "status": "active",
            }
        )
    return semua


def _tipe(entity) -> str:
    if isinstance(entity, Channel) and getattr(entity, "megagroup", False):
        return "supergroup"
    return "group"
