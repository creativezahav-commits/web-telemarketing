from __future__ import annotations

import json
from flask import Blueprint, request

from utils.api import body, fail, ok, pagination_args, parse_bool
from utils.settings_manager import update_banyak
from services.orchestrator_service import execute_recovery_safe, scan_recovery_items
from utils.storage_db import add_audit_log, create_or_update_recovery_item, get_recovery_item, get_recovery_items, get_recovery_summary

bp = Blueprint('recovery_v2', __name__, url_prefix='/api/v2')


@bp.get('/recovery/summary')
def recovery_summary_v2():
    return ok(get_recovery_summary())


@bp.get('/recovery/items')
def recovery_items_v2():
    page, page_size = pagination_args()
    items, total = get_recovery_items(
        entity_type=request.args.get('entity_type'),
        status=request.args.get('status'),
        severity=request.args.get('severity'),
        recoverable_only=parse_bool(request.args.get('recoverable_only')),
        page=page,
        page_size=page_size,
    )
    return ok({'items': items}, meta={'page': page, 'page_size': page_size, 'total': total})


@bp.get('/recovery/items/<int:item_id>')
def recovery_item_detail_v2(item_id: int):
    row = get_recovery_item(item_id)
    if not row:
        return fail('Recovery item tidak ditemukan', 404)
    return ok(row)


@bp.post('/recovery/scan')
def recovery_scan_v2():
    result = scan_recovery_items()
    add_audit_log('info', 'recovery', 'recovery_scan_started', 'Recovery scan dijalankan', entity_type='recovery_item', entity_id='bulk', result='success', payload=json.dumps(result))
    return ok(result, 'Recovery scan dijalankan')


@bp.post('/recovery/items/<int:item_id>/recover')
def recovery_item_recover_v2(item_id: int):
    row = get_recovery_item(item_id)
    if not row:
        return fail('Recovery item tidak ditemukan', 404)
    create_or_update_recovery_item(row['entity_type'], row['entity_id'], recovery_status='recovered', last_recovery_result='recover_now')
    add_audit_log('info', 'recovery', 'recovery_executed', 'Recovery item dipulihkan', entity_type='recovery_item', entity_id=str(item_id), result='success')
    return ok({}, 'Recovery berhasil dijalankan')


@bp.post('/recovery/items/<int:item_id>/mark-partial')
def recovery_item_mark_partial_v2(item_id: int):
    row = get_recovery_item(item_id)
    if not row:
        return fail('Recovery item tidak ditemukan', 404)
    payload = body()
    create_or_update_recovery_item(row['entity_type'], row['entity_id'], recovery_status='partial', note=payload.get('reason'))
    add_audit_log('warning', 'recovery', 'recovery_marked_partial', 'Recovery item ditandai partial', entity_type='recovery_item', entity_id=str(item_id), result='success', payload=json.dumps({'reason': payload.get('reason')}))
    return ok({}, 'Recovery item ditandai partial')


@bp.post('/recovery/items/<int:item_id>/requeue')
def recovery_item_requeue_v2(item_id: int):
    row = get_recovery_item(item_id)
    if not row:
        return fail('Recovery item tidak ditemukan', 404)
    create_or_update_recovery_item(row['entity_type'], row['entity_id'], recovery_status='recoverable', last_recovery_result='requeue')
    add_audit_log('info', 'recovery', 'recovery_requeued', 'Recovery item dimasukkan lagi ke antrian', entity_type='recovery_item', entity_id=str(item_id), result='success')
    return ok({}, 'Recovery item direqueue')


@bp.post('/recovery/items/<int:item_id>/stop')
def recovery_item_stop_v2(item_id: int):
    row = get_recovery_item(item_id)
    if not row:
        return fail('Recovery item tidak ditemukan', 404)
    payload = body()
    create_or_update_recovery_item(row['entity_type'], row['entity_id'], recovery_status='abandoned', note=payload.get('reason'))
    add_audit_log('warning', 'recovery', 'recovery_stopped', 'Recovery item dihentikan', entity_type='recovery_item', entity_id=str(item_id), result='success', payload=json.dumps({'reason': payload.get('reason')}))
    return ok({}, 'Recovery item dihentikan')


@bp.post('/recovery/items/<int:item_id>/ignore')
def recovery_item_ignore_v2(item_id: int):
    row = get_recovery_item(item_id)
    if not row:
        return fail('Recovery item tidak ditemukan', 404)
    payload = body()
    create_or_update_recovery_item(row['entity_type'], row['entity_id'], recovery_status='ignored', note=payload.get('reason'))
    add_audit_log('warning', 'recovery', 'recovery_ignored', 'Recovery item diabaikan', entity_type='recovery_item', entity_id=str(item_id), result='success', payload=json.dumps({'reason': payload.get('reason')}))
    return ok({}, 'Recovery item diabaikan')


@bp.post('/recovery/recover-all-safe')
def recovery_recover_all_safe_v2():
    result = execute_recovery_safe()
    add_audit_log('info', 'recovery', 'recover_all_safe', 'Recover all safe dijalankan', entity_type='recovery_item', entity_id='bulk', result='success', payload=json.dumps(result))
    return ok(result, 'Recover all safe selesai')


@bp.post('/recovery/pause-engine')
def recovery_pause_engine_v2():
    payload = body()
    enabled = parse_bool(payload.get('enabled'))
    update_banyak({'auto_recovery_enabled': 1 if enabled else 0})
    add_audit_log('warning', 'recovery', 'recovery_engine_toggled', 'Recovery engine diubah', entity_type='recovery_item', entity_id='engine', result='success', payload=json.dumps({'enabled': enabled}))
    return ok({'enabled': enabled}, 'Recovery engine diperbarui')
