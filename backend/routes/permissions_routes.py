from __future__ import annotations

from flask import Blueprint, request

from utils.api import body, fail, ok, pagination_args, parse_bool
from utils.storage_db import add_audit_log, create_permission, get_permission, get_permission_summary, get_permissions, update_permission

bp = Blueprint('permissions_v2', __name__, url_prefix='/api/v2')


@bp.get('/permissions/summary')
def permissions_summary_v2():
    return ok(get_permission_summary())


@bp.get('/permissions')
def permissions_list_v2():
    page, page_size = pagination_args()
    items, total = get_permissions(
        search=(request.args.get('search') or '').strip(),
        status=request.args.get('status'),
        basis=request.args.get('basis'),
        expiring_soon=parse_bool(request.args.get('expiring_soon')),
        approved_by=request.args.get('approved_by'),
        page=page,
        page_size=page_size,
    )
    return ok({'items': items}, meta={'page': page, 'page_size': page_size, 'total': total})


@bp.get('/permissions/<int:permission_id>')
def permission_detail_v2(permission_id: int):
    row = get_permission(permission_id)
    if not row:
        return fail('Permission tidak ditemukan', 404)
    return ok(row)


@bp.post('/permissions')
def permission_create_v2():
    payload = body()
    group_id = payload.get('group_id')
    permission_basis = (payload.get('permission_basis') or '').strip()
    if not group_id:
        return fail('group_id wajib diisi')
    if not permission_basis:
        return fail('permission_basis wajib diisi')
    pid = create_permission(
        int(group_id),
        permission_basis,
        payload.get('approval_source'),
        payload.get('approved_by'),
        payload.get('approved_at'),
        payload.get('expires_at'),
        payload.get('notes'),
        payload.get('status') or 'valid',
    )
    add_audit_log('info', 'permissions', 'permission_created', 'Permission dibuat', entity_type='permission', entity_id=str(pid), result='success')
    return ok({'permission_id': pid}, 'Permission berhasil dibuat', status_code=201)


@bp.patch('/permissions/<int:permission_id>')
def permission_patch_v2(permission_id: int):
    if not get_permission(permission_id):
        return fail('Permission tidak ditemukan', 404)
    payload = body()
    update_permission(permission_id, **payload)
    add_audit_log('info', 'permissions', 'permission_updated', 'Permission diperbarui', entity_type='permission', entity_id=str(permission_id), result='success')
    return ok({}, 'Permission berhasil diperbarui')


@bp.post('/permissions/<int:permission_id>/approve')
def permission_approve_v2(permission_id: int):
    row = get_permission(permission_id)
    if not row:
        return fail('Permission tidak ditemukan', 404)
    payload = body()
    update_permission(permission_id, status='valid', approved_by=payload.get('approved_by') or row.get('approved_by'), approved_at=payload.get('approved_at') or row.get('approved_at'))
    add_audit_log('info', 'permissions', 'permission_approved', 'Permission diset valid', entity_type='permission', entity_id=str(permission_id), result='success')
    return ok({}, 'Permission diset valid')


@bp.post('/permissions/<int:permission_id>/revoke')
def permission_revoke_v2(permission_id: int):
    if not get_permission(permission_id):
        return fail('Permission tidak ditemukan', 404)
    payload = body()
    update_permission(permission_id, status='revoked', notes=payload.get('reason'))
    add_audit_log('warning', 'permissions', 'permission_revoked', 'Permission dicabut', entity_type='permission', entity_id=str(permission_id), result='success', payload=str({'reason': payload.get('reason')}))
    return ok({}, 'Permission dicabut')


@bp.post('/permissions/<int:permission_id>/extend')
def permission_extend_v2(permission_id: int):
    if not get_permission(permission_id):
        return fail('Permission tidak ditemukan', 404)
    payload = body()
    expires_at = payload.get('expires_at')
    if not expires_at:
        return fail('expires_at wajib diisi')
    update_permission(permission_id, expires_at=expires_at, status='valid')
    add_audit_log('info', 'permissions', 'permission_extended', 'Permission diperpanjang', entity_type='permission', entity_id=str(permission_id), result='success', payload=str({'expires_at': expires_at}))
    return ok({}, 'Permission diperpanjang')


@bp.post('/permissions/bulk-approve')
def permissions_bulk_approve_v2():
    payload = body()
    ids = payload.get('permission_ids') or []
    if not ids:
        return fail('permission_ids wajib diisi')
    for pid in ids:
        update_permission(int(pid), status='valid', permission_basis=payload.get('permission_basis'), approved_by=payload.get('approved_by'))
    add_audit_log('info', 'permissions', 'permissions_bulk_approved', 'Bulk approve permission', entity_type='permission', entity_id='bulk', result='success', payload=str({'count': len(ids)}))
    return ok({'updated_count': len(ids)}, 'Bulk approve selesai')


@bp.post('/permissions/recheck-expired')
def permissions_recheck_expired_v2():
    # lightweight: status will be recalculated by simple SQL update
    from utils.database import get_conn
    conn = get_conn()
    conn.execute("UPDATE group_permission SET status='expired' WHERE expires_at IS NOT NULL AND expires_at <= TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS') AND status='valid'")
    conn.execute("UPDATE grup SET permission_status='expired' WHERE permission_expires_at IS NOT NULL AND permission_expires_at <= TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS') AND permission_status='valid'")
    changed = conn.total_changes
    conn.commit(); conn.close()
    add_audit_log('info', 'permissions', 'permissions_rechecked', 'Expired permission diperiksa ulang', entity_type='permission', entity_id='bulk', result='success', payload=str({'changed': changed}))
    return ok({'changed': changed}, 'Expired permission diperiksa ulang')
