from __future__ import annotations

from flask import Blueprint, request

from utils.api import body, fail, ok, pagination_args, parse_bool
from utils.database import get_conn
from services.group_send_guard import annotate_group_row
from utils.storage_db import get_semua_grup, set_status_grup, set_status_grup_massal

bp = Blueprint('groups_v2', __name__, url_prefix='/api/v2')


@bp.get('/groups/summary')
def groups_summary_v2():
    rows = get_semua_grup()
    return ok({
        'total_groups': len(rows),
        'eligible_count': sum(1 for r in rows if (r.get('eligibility_status') or 'eligible') == 'eligible' and r.get('status') == 'active'),
        'ready_assign_count': sum(1 for r in rows if (r.get('assignment_status') or 'ready_assign') == 'ready_assign'),
        'assigned_count': sum(1 for r in rows if (r.get('assignment_status') or '') == 'assigned'),
        'managed_count': sum(1 for r in rows if (r.get('assignment_status') or '') == 'managed'),
        'broadcast_eligible_count': sum(1 for r in rows if (r.get('broadcast_status') or '') == 'broadcast_eligible'),
        'hold_count': sum(1 for r in rows if r.get('status') == 'hold'),
        'archived_count': sum(1 for r in rows if r.get('status') == 'archived'),
    })


@bp.get('/groups')
def groups_list_v2():
    rows = get_semua_grup()
    search = (request.args.get('search') or '').lower().strip()
    if search:
        rows = [r for r in rows if search in (r.get('nama') or '').lower() or search in (r.get('username') or '').lower()]
    permission_status = request.args.get('permission_status')
    if permission_status:
        rows = [r for r in rows if (r.get('permission_status') or 'unknown') == permission_status]
    assignment_status = request.args.get('assignment_status')
    if assignment_status:
        rows = [r for r in rows if (r.get('assignment_status') or 'ready_assign') == assignment_status]
    archived = request.args.get('archived')
    if archived is not None:
        wanted = parse_bool(archived)
        rows = [r for r in rows if ((r.get('status') == 'archived') == wanted)]
    page, page_size = pagination_args()
    total = len(rows)
    start = (page - 1) * page_size
    items = [annotate_group_row(r) for r in rows[start:start + page_size]]
    return ok({'items': items}, meta={'page': page, 'page_size': page_size, 'total': total})


@bp.get('/groups/<int:group_id>')
def group_detail_v2(group_id: int):
    row = next((r for r in get_semua_grup() if int(r['id']) == group_id), None)
    if not row:
        return fail('Group tidak ditemukan', 404)
    row = annotate_group_row(row)
    data = {
        'overview': row,
        'permission': {
            'permission_status': row.get('permission_status', 'unknown'),
            'permission_basis': row.get('permission_basis'),
            'approved_by': row.get('approved_by'),
            'approved_at': row.get('approved_at'),
            'expires_at': row.get('permission_expires_at'),
        },
        'assignment': {
            'owner_phone': row.get('owner_phone'),
            'assignment_status': row.get('assignment_status', 'ready_assign'),
        },
        'broadcast': {
            'broadcast_status': row.get('broadcast_status', 'hold'),
            'notes': row.get('notes'),
        },
    }
    return ok(data)


@bp.patch('/groups/<int:group_id>')
def group_patch_v2(group_id: int):
    payload = body()
    allowed = {'permission_status', 'eligibility_status', 'assignment_status', 'broadcast_status', 'owner_phone', 'notes', 'source_keyword'}
    fields = []
    values = []
    for key, value in payload.items():
        if key in allowed:
            fields.append(f"{key}=%s")
            values.append(value)
    if not fields:
        return fail('Tidak ada field yang dapat diperbarui')
    conn = get_conn()
    values.append(group_id)
    conn.execute(f"UPDATE grup SET {', '.join(fields)}, diupdate=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS') WHERE id=%s", values)
    conn.commit()
    conn.close()
    return ok({}, 'Group berhasil diperbarui')


@bp.post('/groups/<int:group_id>/archive')
def group_archive_v2(group_id: int):
    set_status_grup(group_id, 'archived')
    return ok({}, 'Group diarsipkan')


@bp.post('/groups/<int:group_id>/unarchive')
def group_unarchive_v2(group_id: int):
    set_status_grup(group_id, 'active')
    return ok({}, 'Group diaktifkan kembali')


@bp.post('/groups/<int:group_id>/block-broadcast')
def group_block_broadcast_v2(group_id: int):
    conn = get_conn()
    conn.execute("UPDATE grup SET broadcast_status='broadcast_blocked', diupdate=TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS') WHERE id=%s", (group_id,))
    conn.commit()
    conn.close()
    return ok({}, 'Broadcast group diblok')


@bp.post('/groups/bulk-action')
def groups_bulk_action_v2():
    payload = body()
    ids = payload.get('group_ids') or []
    action = payload.get('action')
    if not ids:
        return fail('group_ids wajib diisi')
    if action == 'archive':
        set_status_grup_massal(ids, 'archived')
    elif action == 'activate':
        set_status_grup_massal(ids, 'active')
    else:
        return fail('Action tidak didukung')
    return ok({}, 'Bulk action berhasil')
