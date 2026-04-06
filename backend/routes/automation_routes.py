from __future__ import annotations

import json
from flask import Blueprint, request

from utils.api import body, fail, ok, pagination_args, parse_bool
from services.automation_rule_engine import (
    canonical_rule_type,
    ensure_default_rules,
    evaluate_rule,
    get_normalized_rule,
    get_normalized_rules,
    get_rule_editor_meta,
    get_rule_overview,
    get_stage_default_config,
    validate_rule_payload,
)
from utils.storage_db import (
    add_audit_log,
    create_automation_rule,
    delete_automation_rule,
    get_automation_rule,
    get_automation_rule_summary,
    get_automation_rules,
    update_automation_rule,
)

bp = Blueprint('automation_v2', __name__, url_prefix='/api/v2')


def _json_object_field(payload: dict, key: str, default):
    if key not in payload:
        return default
    value = payload.get(key)
    if isinstance(value, str):
        raw = value.strip()
        if raw == '':
            return {}
        try:
            value = json.loads(raw)
        except Exception as exc:
            raise ValueError(f'{key} harus berupa JSON valid: {exc}') from exc
    if not isinstance(value, dict):
        raise ValueError(f'{key} harus berupa objek JSON')
    return value


def _validated_rule_parts(payload: dict, stage: str, existing: dict | None = None):
    existing = existing or {}
    existing_rule = get_normalized_rule(existing['id']) if existing.get('id') else None
    defaults = get_stage_default_config(stage)
    condition = _json_object_field(payload, 'condition_json', (existing_rule or {}).get('condition') or defaults.get('condition') or {})
    action = _json_object_field(payload, 'action_json', (existing_rule or {}).get('action') or defaults.get('action') or {})
    scope = _json_object_field(payload, 'scope_json', (existing_rule or {}).get('scope') or defaults.get('scope') or {})
    errors = validate_rule_payload(stage, condition=condition, action=action, scope=scope)
    if errors:
        raise ValueError('; '.join(errors))
    return condition, action, scope



@bp.get('/automation-rules/summary')
def automation_rules_summary_v2():
    ensure_default_rules()
    return ok(get_automation_rule_summary())




@bp.get('/automation-rules/meta')
def automation_rules_meta_v2():
    ensure_default_rules()
    stage = request.args.get('stage') or request.args.get('rule_type')
    return ok(get_rule_editor_meta(stage))


@bp.post('/automation-rules/validate')
def automation_rule_validate_v2():
    payload = body()
    stage = canonical_rule_type((payload.get('rule_type') or payload.get('stage') or '').strip())
    if not stage:
        return fail('stage atau rule_type wajib diisi')
    try:
        condition, action, scope = _validated_rule_parts(payload, stage)
    except ValueError as exc:
        return fail(str(exc), 400)
    return ok({
        'valid': True,
        'condition': condition,
        'action': action,
        'scope': scope,
        'stage_meta': get_rule_editor_meta(stage)['stages'][stage],
    })

@bp.get('/automation-rules')
def automation_rules_list_v2():
    ensure_default_rules()
    page, page_size = pagination_args()
    enabled = request.args.get('enabled')
    items, total = get_normalized_rules(
        rule_type=request.args.get('rule_type'),
        enabled=parse_bool(enabled) if enabled is not None else None,
        page=page,
        page_size=page_size,
    )
    return ok({'items': items}, meta={'page': page, 'page_size': page_size, 'total': total})


@bp.get('/automation-rules/<int:rule_id>')
def automation_rule_detail_v2(rule_id: int):
    ensure_default_rules()
    row = get_normalized_rule(rule_id)
    if not row:
        return fail('Rule tidak ditemukan', 404)
    return ok(row)


@bp.post('/automation-rules')
def automation_rule_create_v2():
    payload = body()
    name = (payload.get('name') or '').strip()
    rule_type = canonical_rule_type((payload.get('rule_type') or '').strip())
    if not name or not rule_type:
        return fail('name dan rule_type wajib diisi')
    try:
        condition_payload, action_payload, scope_payload = _validated_rule_parts(payload, rule_type)
    except ValueError as exc:
        return fail(str(exc), 400)
    rid = create_automation_rule(
        name=name,
        rule_type=rule_type,
        enabled=1 if parse_bool(payload.get('enabled'), True) else 0,
        priority=int(payload.get('priority') or 100),
        condition_json=json.dumps(condition_payload or {}, ensure_ascii=False),
        action_json=json.dumps(action_payload or {}, ensure_ascii=False),
        cooldown_seconds=int(payload.get('cooldown_seconds') or 0),
        scope_json=json.dumps(scope_payload or {}, ensure_ascii=False),
    )
    add_audit_log('info', 'automation', 'rule_created', 'Rule dibuat', entity_type='automation_rule', entity_id=str(rid), result='success')
    return ok({'rule_id': rid, 'rule': get_normalized_rule(rid)}, 'Rule berhasil dibuat', status_code=201)


@bp.patch('/automation-rules/<int:rule_id>')
def automation_rule_patch_v2(rule_id: int):
    row = get_automation_rule(rule_id)
    if not row:
        return fail('Rule tidak ditemukan', 404)
    payload = body()
    stage = canonical_rule_type((payload.get('rule_type') or row.get('rule_type') or '').strip())
    try:
        condition_payload, action_payload, scope_payload = _validated_rule_parts(payload, stage, existing=row)
    except ValueError as exc:
        return fail(str(exc), 400)
    if 'condition_json' in payload:
        payload['condition_json'] = json.dumps(condition_payload, ensure_ascii=False)
    if 'action_json' in payload:
        payload['action_json'] = json.dumps(action_payload, ensure_ascii=False)
    if 'scope_json' in payload:
        payload['scope_json'] = json.dumps(scope_payload, ensure_ascii=False)
    if 'rule_type' in payload:
        payload['rule_type'] = stage
    update_automation_rule(rule_id, **payload)
    add_audit_log('info', 'automation', 'rule_updated', 'Rule diperbarui', entity_type='automation_rule', entity_id=str(rule_id), result='success')
    return ok({'rule': get_normalized_rule(rule_id)}, 'Rule diperbarui')


@bp.post('/automation-rules/<int:rule_id>/toggle')
def automation_rule_toggle_v2(rule_id: int):
    if not get_automation_rule(rule_id):
        return fail('Rule tidak ditemukan', 404)
    enabled = 1 if parse_bool(body().get('enabled'), True) else 0
    update_automation_rule(rule_id, enabled=enabled)
    add_audit_log('info', 'automation', 'rule_toggled', 'Rule toggle diperbarui', entity_type='automation_rule', entity_id=str(rule_id), result='success', payload=json.dumps({'enabled': enabled}))
    return ok({'enabled': bool(enabled)}, 'Rule diperbarui')


@bp.post('/automation-rules/<int:rule_id>/test')
def automation_rule_test_v2(rule_id: int):
    row = get_normalized_rule(rule_id)
    if not row:
        return fail('Rule tidak ditemukan', 404)
    result = evaluate_rule(rule_id)
    add_audit_log('info', 'automation', 'rule_tested', 'Rule diuji', entity_type='automation_rule', entity_id=str(rule_id), result='success', payload=json.dumps({'canonical_stage': result.get('canonical_stage'), 'matched': result.get('matched')}))
    return ok(result)


@bp.post('/automation-rules/<int:rule_id>/duplicate')
def automation_rule_duplicate_v2(rule_id: int):
    row = get_automation_rule(rule_id)
    if not row:
        return fail('Rule tidak ditemukan', 404)
    new_id = create_automation_rule(
        name=f"{row['name']} Copy",
        rule_type=row['rule_type'],
        enabled=row.get('enabled', 1),
        priority=row.get('priority', 100),
        condition_json=row.get('condition_json'),
        action_json=row.get('action_json'),
        cooldown_seconds=row.get('cooldown_seconds', 0),
        scope_json=row.get('scope_json'),
    )
    add_audit_log('info', 'automation', 'rule_duplicated', 'Rule diduplikasi', entity_type='automation_rule', entity_id=str(new_id), result='success', payload=json.dumps({'source_rule_id': rule_id}))
    return ok({'rule_id': new_id}, 'Rule berhasil diduplikasi')


@bp.delete('/automation-rules/<int:rule_id>')
def automation_rule_delete_v2(rule_id: int):
    if not get_automation_rule(rule_id):
        return fail('Rule tidak ditemukan', 404)
    delete_automation_rule(rule_id)
    add_audit_log('warning', 'automation', 'rule_deleted', 'Rule dihapus', entity_type='automation_rule', entity_id=str(rule_id), result='success')
    return ok({}, 'Rule dihapus')




@bp.get('/automation-rules/overview')
def automation_rules_overview_v2():
    ensure_default_rules()
    return ok(get_rule_overview())

@bp.post('/automation-rules/pause-all')
def automation_rules_pause_all_v2():
    items, _ = get_automation_rules(page=1, page_size=1000)
    for item in items:
        update_automation_rule(int(item['id']), enabled=0)
    add_audit_log('warning', 'automation', 'rules_paused_all', 'Semua rule dimatikan', entity_type='automation_rule', entity_id='bulk', result='success', payload=json.dumps({'count': len(items)}))
    return ok({'updated_count': len(items)}, 'Semua rule dimatikan')
