from __future__ import annotations

from flask import Blueprint

from services.account_manager import _clients, delete_akun_permanen, run_sync
from utils.api import body, fail, ok, pagination_args, parse_bool
from utils.storage_db import get_semua_akun, set_status_akun

bp = Blueprint('accounts_v2', __name__, url_prefix='/api/v2')


def _enrich(row: dict) -> dict:
    row = dict(row)
    status = str(row.get('status') or '').lower()
    # Akun restricted/banned tidak dianggap online meski koneksi Telegram masih aktif
    if status in ('restricted', 'banned', 'suspended'):
        row['online'] = False
    else:
        row['online'] = row['phone'] in _clients
    row['auto_assign_enabled'] = bool(row.get('auto_assign_enabled', 1))
    row['auto_send_enabled'] = bool(row.get('auto_send_enabled', 1))
    row['manual_health_override_enabled'] = bool(row.get('manual_health_override_enabled', 0))
    row['manual_warming_override_enabled'] = bool(row.get('manual_warming_override_enabled', 0))
    row['fresh_login_grace_enabled'] = bool(row.get('fresh_login_grace_enabled', 1))
    return row


@bp.get('/account-pools')
def get_account_pools_v2():
    rows = get_semua_akun()
    pools = sorted({(r.get('pool') or 'default') for r in rows})
    if 'default' not in pools:
        pools.insert(0, 'default')
    return ok({'items': [{'key': p, 'label': p.title()} for p in pools]})


@bp.get('/accounts/summary')
def accounts_summary_v2():
    rows = [_enrich(r) for r in get_semua_akun()]
    return ok({
        'total_accounts': len(rows),
        'online_accounts': sum(1 for r in rows if r['online']),
        'offline_accounts': sum(1 for r in rows if not r['online']),
        'limited_accounts': sum(1 for r in rows if r.get('status') == 'limited'),
        'banned_accounts': sum(1 for r in rows if r.get('status') == 'banned'),
        'cooling_accounts': sum(1 for r in rows if r.get('status') == 'cooling'),
        'auto_assign_enabled': sum(1 for r in rows if r['auto_assign_enabled']),
        'auto_send_enabled': sum(1 for r in rows if r['auto_send_enabled']),
    })


@bp.get('/accounts')
def accounts_list_v2():
    page, page_size = pagination_args()
    rows = [_enrich(r) for r in get_semua_akun()]
    search = (__import__('flask').request.args.get('search') or '').lower().strip()
    role = __import__('flask').request.args.get('role')
    pool = __import__('flask').request.args.get('pool')
    status = __import__('flask').request.args.get('status')
    auto_assign = __import__('flask').request.args.get('auto_assign_enabled')
    auto_send = __import__('flask').request.args.get('auto_send_enabled')
    if search:
        rows = [r for r in rows if search in (r.get('nama') or '').lower() or search in (r.get('phone') or '').lower()]
    if role:
        rows = [r for r in rows if (r.get('role') or 'hybrid') == role]
    if pool:
        rows = [r for r in rows if (r.get('pool') or 'default') == pool]
    if status:
        rows = [r for r in rows if (r.get('status') or 'active') == status]
    if auto_assign is not None:
        wanted = parse_bool(auto_assign)
        rows = [r for r in rows if bool(r.get('auto_assign_enabled', 1)) == wanted]
    if auto_send is not None:
        wanted = parse_bool(auto_send)
        rows = [r for r in rows if bool(r.get('auto_send_enabled', 1)) == wanted]
    total = len(rows)
    start = (page - 1) * page_size
    items = rows[start:start + page_size]
    return ok({'items': items}, meta={'page': page, 'page_size': page_size, 'total': total})


@bp.get('/accounts/<path:account_id>')
def account_detail_v2(account_id: str):
    row = next((r for r in get_semua_akun() if r['phone'] == account_id), None)
    if not row:
        return fail('Akun tidak ditemukan', 404)
    row = _enrich(row)
    data = {
        'overview': row,
        'assignment_profile': {
            'manual_health_override_enabled': bool(row.get('manual_health_override_enabled', 0)),
            'manual_health_override_score': row.get('manual_health_override_score', 80),
            'manual_warming_override_enabled': bool(row.get('manual_warming_override_enabled', 0)),
            'manual_warming_override_level': row.get('manual_warming_override_level', 2),
            'fresh_login_grace_enabled': bool(row.get('fresh_login_grace_enabled', 1)),
            'fresh_login_grace_minutes': row.get('fresh_login_grace_minutes', 180),
            'fresh_login_health_floor': row.get('fresh_login_health_floor', 80),
            'fresh_login_warming_floor': row.get('fresh_login_warming_floor', 2),
            'assignment_notes': row.get('assignment_notes'),
        },
        'capacity': {
            'warming_level': row.get('level_warming', 1),
            'daily_new_group_cap': row.get('daily_new_group_cap', 10),
            'daily_send_cap': row.get('daily_send_cap', 20),
            'concurrent_cap': row.get('concurrent_cap', 3),
            'priority_weight': row.get('priority_weight', 100),
            'auto_assign_enabled': bool(row.get('auto_assign_enabled', 1)),
            'auto_send_enabled': bool(row.get('auto_send_enabled', 1)),
        },
        'activity': {
            'last_activity_at': row.get('last_activity_at'),
            'today_send_count': row.get('total_kirim', 0),
            'success_total': row.get('total_berhasil', 0),
            'flood_total': row.get('total_flood', 0),
            'banned_total': row.get('total_banned', 0),
        },
        'errors': {
            'last_error_code': row.get('last_error_code'),
            'last_error_message': row.get('last_error_message'),
        },
    }
    return ok(data)


@bp.patch('/accounts/<path:account_id>')
def account_patch_v2(account_id: str):
    payload = body()
    rows = get_semua_akun()
    if not next((r for r in rows if r['phone'] == account_id), None):
        return fail('Akun tidak ditemukan', 404)
    # lightweight update via direct SQL to avoid large rewrite
    from utils.database import get_conn
    conn = get_conn()
    allowed = {
        'role', 'pool', 'level_warming', 'daily_new_group_cap', 'daily_send_cap',
        'concurrent_cap', 'priority_weight', 'auto_assign_enabled', 'auto_send_enabled',
        'health_score', 'cooldown_until', 'last_error_code', 'last_error_message',
        'manual_health_override_enabled', 'manual_health_override_score',
        'manual_warming_override_enabled', 'manual_warming_override_level',
        'fresh_login_grace_enabled', 'fresh_login_grace_minutes',
        'fresh_login_health_floor', 'fresh_login_warming_floor', 'assignment_notes'
    }
    fields = []
    values = []
    for key, value in payload.items():
        if key in allowed:
            fields.append(f"{key}=%s")
            values.append(value)
    if not fields:
        conn.close()
        return fail('Tidak ada field yang dapat diperbarui')
    values.append(account_id)
    conn.execute(f"UPDATE akun SET {', '.join(fields)} WHERE phone=%s", values)
    conn.commit()
    conn.close()
    return ok({}, 'Akun berhasil diperbarui')


@bp.post('/accounts/<path:account_id>/suspend')
def account_suspend_v2(account_id: str):
    set_status_akun(account_id, 'disabled')
    return ok({}, 'Akun disuspend')


@bp.post('/accounts/<path:account_id>/resume')
def account_resume_v2(account_id: str):
    set_status_akun(account_id, 'active')
    return ok({}, 'Akun diaktifkan kembali')


@bp.delete('/accounts/<path:account_id>')
def account_delete_v2(account_id: str):
    row = next((r for r in get_semua_akun() if r['phone'] == account_id), None)
    if not row:
        return fail('Akun tidak ditemukan', 404)
    result = run_sync(delete_akun_permanen(account_id), timeout=180)
    return ok(result, 'Akun dihapus permanen')


@bp.get('/accounts/<path:account_id>/assignment-config')
def account_assignment_config_v2(account_id: str):
    row = next((r for r in get_semua_akun() if r['phone'] == account_id), None)
    if not row:
        return fail('Akun tidak ditemukan', 404)
    row = _enrich(row)
    return ok({
        'phone': row['phone'],
        'name': row.get('nama') or row['phone'],
        'auto_assign_enabled': bool(row.get('auto_assign_enabled', 1)),
        'priority_weight': row.get('priority_weight', 100),
        'daily_new_group_cap': row.get('daily_new_group_cap', 10),
        'manual_health_override_enabled': bool(row.get('manual_health_override_enabled', 0)),
        'manual_health_override_score': row.get('manual_health_override_score', 80),
        'manual_warming_override_enabled': bool(row.get('manual_warming_override_enabled', 0)),
        'manual_warming_override_level': row.get('manual_warming_override_level', 2),
        'fresh_login_grace_enabled': bool(row.get('fresh_login_grace_enabled', 1)),
        'fresh_login_grace_minutes': row.get('fresh_login_grace_minutes', 180),
        'fresh_login_health_floor': row.get('fresh_login_health_floor', 80),
        'fresh_login_warming_floor': row.get('fresh_login_warming_floor', 2),
        'assignment_notes': row.get('assignment_notes') or '',
        'last_login_at': row.get('last_login_at'),
    })