from __future__ import annotations

from datetime import datetime
from typing import Any

from utils.database import get_conn
from utils.settings_manager import get, get_int


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    return str(value or '').strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def get_send_guard_settings(overrides: dict | None = None) -> dict[str, Any]:
    overrides = overrides or {}
    return {
        'skip_inactive_groups': _to_bool(overrides.get('skip_inactive_groups', get('campaign_skip_inactive_groups_enabled', 1))),
        'inactive_threshold_days': max(0, int(overrides.get('inactive_threshold_days', get_int('campaign_inactive_threshold_days', 14)) or 0)),
        'skip_if_last_chat_is_ours': _to_bool(overrides.get('skip_if_last_chat_is_ours', get('campaign_skip_if_last_chat_is_ours', 1))),
    }


def evaluate_group_send_guard(group_row: dict | Any, *, overrides: dict | None = None) -> dict[str, Any]:
    row = dict(group_row or {})
    cfg = get_send_guard_settings(overrides)
    now = datetime.now()
    last_chat = _parse_dt(row.get('last_chat'))
    last_kirim = _parse_dt(row.get('last_kirim'))
    idle_days = None
    if last_chat:
        idle_days = max(0, int((now - last_chat).total_seconds() // 86400))
    is_our_latest = bool(last_chat and last_kirim and last_chat <= last_kirim)

    status = 'sendable'
    reason_code = 'eligible'
    reason_text = 'Grup layak dikirimi.'
    eligible = True

    if cfg['skip_if_last_chat_is_ours'] and is_our_latest:
        status = 'hold_waiting_response'
        reason_code = 'last_chat_is_our_message'
        reason_text = 'Chat terakhir di grup masih pesan kita sendiri; tunggu respons baru agar hemat kuota.'
        eligible = False
    elif cfg['skip_inactive_groups'] and last_chat and idle_days is not None and idle_days >= cfg['inactive_threshold_days']:
        status = 'hold_inactive'
        reason_code = 'inactive_group'
        reason_text = f"Tidak ada chat baru selama {idle_days} hari; grup ditahan dari pengiriman."
        eligible = False
    elif cfg['skip_inactive_groups'] and not last_chat:
        status = 'unknown'
        reason_code = 'last_chat_unknown'
        reason_text = 'Belum ada data last chat; grup belum bisa dinilai sepenuhnya.'
        eligible = True

    return {
        'send_eligible': eligible,
        'send_guard_status': status,
        'send_guard_reason': reason_text,
        'send_guard_reason_code': reason_code,
        'send_guard_checked_at': now.strftime('%Y-%m-%d %H:%M:%S'),
        'idle_days': idle_days,
        'is_our_latest_message': is_our_latest,
        'last_chat': row.get('last_chat'),
        'last_kirim': row.get('last_kirim'),
        'settings': cfg,
    }


def persist_group_send_guard(group_id: int, guard: dict[str, Any]) -> None:
    conn = get_conn()
    conn.execute(
        """
        UPDATE grup
        SET send_guard_status=%s,
            send_guard_reason=%s,
            send_guard_checked_at=%s,
            idle_days=%s
        WHERE id=%s
        """,
        (
            guard.get('send_guard_status'),
            guard.get('send_guard_reason'),
            guard.get('send_guard_checked_at'),
            guard.get('idle_days'),
            int(group_id),
        ),
    )
    conn.commit()
    conn.close()


def annotate_group_row(group_row: dict | Any, *, overrides: dict | None = None) -> dict[str, Any]:
    row = dict(group_row or {})
    guard = evaluate_group_send_guard(row, overrides=overrides)
    row.update(guard)
    return row
