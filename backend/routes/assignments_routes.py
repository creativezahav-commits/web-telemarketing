from __future__ import annotations

import json
from datetime import datetime
from flask import Blueprint, request

from utils.api import body, fail, ok, pagination_args, parse_bool, parse_int
from utils.settings_manager import get_int
from utils.storage_db import (
    add_audit_log,
    create_assignment,
    get_assignment,
    get_assignment_candidates,
    get_assignment_summary,
    get_assignments,
    get_semua_grup,
    update_assignment,
)

bp = Blueprint('assignments_v2', __name__, url_prefix='/api/v2')


def _pick_best_candidate(group_id: int):
    candidates = get_assignment_candidates(group_id)
    return candidates[0] if candidates else None, candidates


@bp.get('/assignments/summary')
def assignments_summary_v2():
    return ok(get_assignment_summary())


@bp.get('/assignments/criteria')
def assignments_criteria_v2():
    min_health = int(get_int('assignment_min_health_score', 50) or 0)
    min_warming = int(get_int('assignment_min_warming_level', 1) or 0)
    retry_count = int(get_int('assignment_retry_count', 2) or 0)
    reassign_count = int(get_int('assignment_reassign_count', 1) or 0)
    return ok({
        'filters': {
            'status_allowed': ['active', 'online'],
            'auto_assign_enabled_required': True,
            'min_health_score': min_health,
            'min_warming_level': min_warming,
            'cooldown_must_be_clear': True,
            'capacity_rule': 'active_assignment_count < daily_new_group_cap',
            'per_account_overrides_supported': ['manual_health_override', 'manual_warming_override', 'fresh_login_grace'],
        },
        'ranking': {
            'formula': 'priority_weight + health_score + (warming_level * 10) - (active_assignment_count * 5)',
            'order': ['priority_weight', 'health_score', 'warming_level', 'beban assignment'],
            'prefer_joined_owner': True,
        },
        'retries': {
            'assignment_retry_count': retry_count,
            'assignment_reassign_count': reassign_count,
        },
        'where_to_change': {
            'tab': 'Settings',
            'scope': 'assignment-rules',
            'keys': ['assignment_min_health_score', 'assignment_min_warming_level', 'assignment_retry_count', 'assignment_reassign_count'],
        },
        'per_account_config': {
            'tab': 'Akun',
            'action': 'Konfigurasi Assign per akun',
            'why': 'Pakai ini bila akun sebenarnya sehat tetapi baru login dan belum lolos health/warming global.',
        },
    })


@bp.get('/assignments')
def assignments_list_v2():
    page, page_size = pagination_args()
    items, total = get_assignments(
        search=(request.args.get('search') or '').strip(),
        status=request.args.get('status'),
        pool=request.args.get('pool'),
        assignment_type=request.args.get('assignment_type'),
        retry_due=parse_bool(request.args.get('retry_due')),
        page=page,
        page_size=page_size,
    )
    return ok({'items': items}, meta={'page': page, 'page_size': page_size, 'total': total})


@bp.get('/assignments/<int:assignment_id>')
def assignment_detail_v2(assignment_id: int):
    row = get_assignment(assignment_id)
    if not row:
        return fail('Assignment tidak ditemukan', 404)
    candidates = get_assignment_candidates(int(row['group_id']))
    data = {
        'overview': row,
        'retry_history': [],
        'reassign_history': [],
        'candidate_ranking': candidates,
        'rule_evaluation': {
            'selected_owner': row.get('assigned_account_id'),
            'selection_reason': row.get('assign_reason') or 'Belum ada alasan tersimpan',
        },
    }
    return ok(data)


@bp.get('/assignments/<int:assignment_id>/candidates')
def assignment_candidates_v2(assignment_id: int):
    row = get_assignment(assignment_id)
    if not row:
        return fail('Assignment tidak ditemukan', 404)
    return ok({'items': get_assignment_candidates(int(row['group_id']))})


@bp.post('/assignments/run-auto')
def assignments_run_auto_v2():
    payload = body()
    limit = parse_int(payload.get('limit'), 20, minimum=1, maximum=500)
    groups = [g for g in get_semua_grup() if (g.get('assignment_status') or 'ready_assign') == 'ready_assign' and g.get('status') == 'active']
    created = []
    skipped = []
    for group in groups[:limit]:
        best, candidates = _pick_best_candidate(int(group['id']))
        if not best:
            skipped.append({'group_id': group['id'], 'reason': 'no_candidate'})
            continue
        snapshot = json.dumps(candidates[:5], ensure_ascii=False)
        aid = create_assignment(int(group['id']), str(best['account_id']), status='assigned', assign_reason='auto_assign_v2', assign_score_snapshot=snapshot)
        created.append({'assignment_id': aid, 'group_id': group['id'], 'account_id': best['account_id']})
    add_audit_log('info', 'assignments', 'run_auto_assign', 'Auto assign dijalankan', entity_type='assignment', entity_id='bulk', result='success', payload=json.dumps({'created': len(created), 'skipped': len(skipped)}))
    return ok({'created': created, 'skipped': skipped}, 'Auto assign selesai')


@bp.post('/assignments/reassign-failed')
def assignments_reassign_failed_v2():
    payload = body()
    limit = parse_int(payload.get('limit'), 20, minimum=1, maximum=500)
    items, _ = get_assignments(status='failed', page=1, page_size=limit)
    moved = 0
    for item in items:
        best, _ = _pick_best_candidate(int(item['group_id']))
        if not best:
            continue
        update_assignment(int(item['id']), assigned_account_id=str(best['account_id']), status='assigned', reassign_count=int(item.get('reassign_count') or 0) + 1, failure_reason=None)
        moved += 1
    add_audit_log('info', 'assignments', 'reassign_failed', 'Reassign failed dijalankan', entity_type='assignment', entity_id='bulk', result='success', payload=json.dumps({'moved': moved}))
    return ok({'reassigned_count': moved}, 'Reassign failed selesai')


@bp.post('/assignments/<int:assignment_id>/retry')
def assignment_retry_v2(assignment_id: int):
    row = get_assignment(assignment_id)
    if not row:
        return fail('Assignment tidak ditemukan', 404)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    update_assignment(assignment_id, status='retry_wait', retry_count=int(row.get('retry_count') or 0) + 1, last_attempt_at=now)
    add_audit_log('info', 'assignments', 'assignment_retry', 'Assignment dipindah ke retry_wait', entity_type='assignment', entity_id=str(assignment_id), result='success')
    return ok({}, 'Assignment masuk retry_wait')


@bp.post('/assignments/<int:assignment_id>/reassign')
def assignment_reassign_v2(assignment_id: int):
    row = get_assignment(assignment_id)
    if not row:
        return fail('Assignment tidak ditemukan', 404)
    payload = body()
    target_account = payload.get('target_account_id')
    if not target_account:
        best, _ = _pick_best_candidate(int(row['group_id']))
        if not best:
            return fail('Tidak ada kandidat akun')
        target_account = best['account_id']
    update_assignment(assignment_id, assigned_account_id=str(target_account), status='assigned', reassign_count=int(row.get('reassign_count') or 0) + 1, failure_reason=payload.get('reason'))
    add_audit_log('warning', 'assignments', 'assignment_reassigned', 'Assignment dipindahkan', entity_type='assignment', entity_id=str(assignment_id), result='success', payload=json.dumps({'target_account_id': target_account}))
    return ok({'assigned_account_id': target_account}, 'Assignment berhasil dipindahkan')


@bp.post('/assignments/<int:assignment_id>/release')
def assignment_release_v2(assignment_id: int):
    if not get_assignment(assignment_id):
        return fail('Assignment tidak ditemukan', 404)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    update_assignment(assignment_id, status='released', released_at=now)
    add_audit_log('warning', 'assignments', 'assignment_released', 'Assignment dilepas', entity_type='assignment', entity_id=str(assignment_id), result='success')
    return ok({}, 'Assignment dilepas')


@bp.post('/assignments/<int:assignment_id>/force-assign')
def assignment_force_assign_v2(assignment_id: int):
    row = get_assignment(assignment_id)
    if not row:
        return fail('Assignment tidak ditemukan', 404)
    payload = body()
    account_id = payload.get('account_id')
    if not account_id:
        return fail('account_id wajib diisi')
    update_assignment(assignment_id, assigned_account_id=str(account_id), status='assigned', assign_reason=payload.get('reason') or 'manual_force_assign')
    add_audit_log('warning', 'assignments', 'assignment_force_assigned', 'Assignment dipaksa ke akun tertentu', entity_type='assignment', entity_id=str(assignment_id), result='success', payload=json.dumps({'account_id': account_id}))
    return ok({'assigned_account_id': account_id}, 'Force assign berhasil')


@bp.post('/assignments/bulk-release')
def assignment_bulk_release_v2():
    payload = body()
    ids = payload.get('assignment_ids') or []
    if not ids:
        return fail('assignment_ids wajib diisi')
    for assignment_id in ids:
        update_assignment(int(assignment_id), status='released')
    add_audit_log('warning', 'assignments', 'assignment_bulk_released', 'Bulk release assignment', entity_type='assignment', entity_id='bulk', result='success', payload=json.dumps({'count': len(ids)}))
    return ok({'released_count': len(ids)}, 'Bulk release selesai')
