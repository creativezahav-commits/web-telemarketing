from __future__ import annotations

from flask import Blueprint

from utils.api import body, fail, ok
from utils.settings_manager import get_semua, update_banyak
from utils.storage_db import add_audit_log
from utils.settings_defaults import defaults_for_scope, SETTINGS_SCOPE_KEYS

bp = Blueprint('settings_v2', __name__, url_prefix='/api/v2')

_SCOPE_KEYS = SETTINGS_SCOPE_KEYS


def _group_settings(rows: list[dict]) -> dict:
    groups = {k.replace('-', '_'): {} for k in _SCOPE_KEYS}
    groups['legacy'] = {}
    for row in rows:
        key = row['key']
        matched = False
        for scope, keys in _SCOPE_KEYS.items():
            if key in keys:
                groups[scope.replace('-', '_')][key] = row
                matched = True
                break
        if not matched:
            groups['legacy'][key] = row
    return groups


@bp.get('/settings')
def get_settings_v2():
    rows = get_semua()
    return ok(_group_settings(rows))


@bp.get('/settings/grouped')
def get_settings_grouped_v2():
    rows = get_semua()
    return ok(_group_settings(rows))


@bp.patch('/settings/<scope>')
def patch_settings_v2(scope: str):
    data = body()
    if not isinstance(data, dict) or not data:
        return fail('Payload settings kosong')
    allowed = _SCOPE_KEYS.get(scope)
    if allowed is None:
        return fail('Scope settings tidak dikenal', 404)
    cleaned = {k: v for k, v in data.items() if k in allowed}
    if not cleaned:
        return fail('Tidak ada key settings yang cocok dengan scope ini')
    update_banyak(cleaned)
    add_audit_log('info', 'settings', 'settings_updated', f'Settings scope {scope} diperbarui', entity_type='settings', entity_id=scope, result='success')
    return ok({}, f'Settings scope {scope} berhasil diperbarui')


@bp.post('/settings/restore-defaults')
def restore_defaults_v2():
    scope = (body().get('scope') or 'all').strip()
    values = defaults_for_scope(scope)
    if scope != 'all' and scope not in _SCOPE_KEYS:
        return fail('Scope settings tidak dikenal', 404)
    if not values:
        return fail('Tidak ada default untuk scope ini')
    update_banyak(values)
    add_audit_log('warning', 'settings', 'settings_restored_defaults', f'Settings dikembalikan ke default untuk {scope}', entity_type='settings', entity_id=scope, result='success')
    return ok({'scope': scope, 'restored_count': len(values)}, 'Settings berhasil dikembalikan ke default')


@bp.get('/settings/export')
def export_settings_v2():
    return ok({'items': get_semua()})


@bp.post('/settings/lock')
def settings_lock_v2():
    enabled = 1 if body().get('enabled') else 0
    update_banyak({'maintenance_mode': enabled, 'pipeline_maintenance_mode': enabled})
    add_audit_log('warning', 'settings', 'settings_lock_changed', 'Settings lock diperbarui', entity_type='settings', entity_id='global', result='success')
    return ok({'locked': bool(enabled)}, 'Status lock settings diperbarui')


@bp.post('/settings/emergency-pause')
def emergency_pause_v2():
    update_banyak({
        'pause_all_automation': 1,
        'pipeline_pause_semua': 1,
        'auto_campaign_enabled': 0,
        'broadcast_enabled': 0,
    })
    add_audit_log('warning', 'settings', 'emergency_pause', 'Emergency pause diaktifkan', entity_type='settings', entity_id='global', result='success')
    return ok({}, 'Emergency pause diaktifkan')
