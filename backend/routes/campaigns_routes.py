from __future__ import annotations

import json
from datetime import datetime
from flask import Blueprint, request

from utils.api import body, fail, ok, pagination_args, parse_bool, parse_int
from utils.settings_manager import update_banyak
from utils.storage_db import (
    add_audit_log,
    create_campaign,
    create_campaign_targets,
    get_broadcast_queue,
    get_broadcast_queue_summary,
    get_campaign,
    get_campaign_summary,
    get_campaigns,
    get_queue_target,
    get_semua_grup,
    update_campaign,
    update_queue_target,
)

bp = Blueprint('campaigns_v2', __name__, url_prefix='/api/v2')


@bp.get('/campaigns/summary')
def campaigns_summary_v2():
    return ok(get_campaign_summary())


@bp.get('/campaigns')
def campaigns_list_v2():
    page, page_size = pagination_args()
    items, total = get_campaigns(
        search=(request.args.get('search') or '').strip(),
        status=request.args.get('status'),
        sender_pool=request.args.get('sender_pool'),
        page=page,
        page_size=page_size,
    )
    return ok({'items': items}, meta={'page': page, 'page_size': page_size, 'total': total})


@bp.get('/campaigns/<int:campaign_id>')
def campaign_detail_v2(campaign_id: int):
    row = get_campaign(campaign_id)
    if not row:
        return fail('Campaign tidak ditemukan', 404)
    queue_items, _ = get_broadcast_queue(campaign_id=campaign_id, page=1, page_size=10)
    data = {
        'overview': row,
        'rules': {
            'required_permission_status': row.get('required_permission_status'),
            'required_group_status': row.get('required_group_status'),
            'sender_pool': row.get('sender_pool'),
            'auto_start_enabled': bool(row.get('auto_start_enabled', 0)),
        },
        'target_summary': get_broadcast_queue_summary(),
        'delivery_summary': {
            'queue_preview': queue_items,
        },
        'failures': [item for item in queue_items if item.get('status') == 'failed'],
    }
    return ok(data)


@bp.post('/campaigns')
def campaign_create_v2():
    payload = body()
    name = (payload.get('name') or '').strip()
    if not name:
        return fail('name wajib diisi')
    cid = create_campaign(
        name=name,
        template_id=payload.get('template_id'),
        sender_pool=payload.get('sender_pool') or 'default',
        target_mode=payload.get('target_mode') or 'rule_based',
        auto_start_enabled=1 if parse_bool(payload.get('auto_start_enabled')) else 0,
        required_permission_status=payload.get('required_permission_status') or 'valid',
        required_group_status=payload.get('required_group_status') or 'managed',
    )
    add_audit_log('info', 'campaigns', 'campaign_created', 'Campaign dibuat', entity_type='campaign', entity_id=str(cid), result='success')
    return ok({'campaign_id': cid}, 'Campaign berhasil dibuat', status_code=201)


@bp.patch('/campaigns/<int:campaign_id>')
def campaign_patch_v2(campaign_id: int):
    if not get_campaign(campaign_id):
        return fail('Campaign tidak ditemukan', 404)
    update_campaign(campaign_id, **body())
    add_audit_log('info', 'campaigns', 'campaign_updated', 'Campaign diperbarui', entity_type='campaign', entity_id=str(campaign_id), result='success')
    return ok({}, 'Campaign diperbarui')


@bp.post('/campaigns/<int:campaign_id>/start')
def campaign_start_v2(campaign_id: int):
    if not get_campaign(campaign_id):
        return fail('Campaign tidak ditemukan', 404)
    update_campaign(campaign_id, status='running', started_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    add_audit_log('info', 'campaigns', 'campaign_started', 'Campaign dimulai', entity_type='campaign', entity_id=str(campaign_id), result='success')
    return ok({}, 'Campaign dimulai')


@bp.post('/campaigns/<int:campaign_id>/pause')
def campaign_pause_v2(campaign_id: int):
    if not get_campaign(campaign_id):
        return fail('Campaign tidak ditemukan', 404)
    update_campaign(campaign_id, status='paused')
    add_audit_log('warning', 'campaigns', 'campaign_paused', 'Campaign dijeda', entity_type='campaign', entity_id=str(campaign_id), result='success')
    return ok({}, 'Campaign dijeda')


@bp.post('/campaigns/<int:campaign_id>/resume')
def campaign_resume_v2(campaign_id: int):
    if not get_campaign(campaign_id):
        return fail('Campaign tidak ditemukan', 404)
    update_campaign(campaign_id, status='running')
    add_audit_log('info', 'campaigns', 'campaign_resumed', 'Campaign dilanjutkan', entity_type='campaign', entity_id=str(campaign_id), result='success')
    return ok({}, 'Campaign dilanjutkan')


@bp.post('/campaigns/<int:campaign_id>/stop')
def campaign_stop_v2(campaign_id: int):
    if not get_campaign(campaign_id):
        return fail('Campaign tidak ditemukan', 404)
    update_campaign(campaign_id, status='stopped', finished_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    add_audit_log('warning', 'campaigns', 'campaign_stopped', 'Campaign dihentikan', entity_type='campaign', entity_id=str(campaign_id), result='success')
    return ok({}, 'Campaign dihentikan')


@bp.post('/campaigns/<int:campaign_id>/duplicate')
def campaign_duplicate_v2(campaign_id: int):
    row = get_campaign(campaign_id)
    if not row:
        return fail('Campaign tidak ditemukan', 404)
    new_id = create_campaign(name=f"{row['name']} Copy", template_id=row.get('template_id'), sender_pool=row.get('sender_pool') or 'default', target_mode=row.get('target_mode') or 'rule_based', auto_start_enabled=row.get('auto_start_enabled') or 0, required_permission_status=row.get('required_permission_status') or 'valid', required_group_status=row.get('required_group_status') or 'managed')
    add_audit_log('info', 'campaigns', 'campaign_duplicated', 'Campaign diduplikasi', entity_type='campaign', entity_id=str(new_id), result='success', payload=json.dumps({'source_campaign_id': campaign_id}))
    return ok({'campaign_id': new_id}, 'Campaign berhasil diduplikasi')


@bp.post('/campaigns/auto-create')
def campaign_auto_create_v2():
    payload = body()
    sender_pool = payload.get('sender_pool') or 'default'
    template_id = payload.get('template_id')
    groups = [g for g in get_semua_grup() if (g.get('broadcast_status') == 'broadcast_eligible' or (g.get('permission_status') in {'valid', 'owned', 'admin', 'partner_approved', 'opt_in'} and g.get('assignment_status') == 'managed')) and g.get('status') == 'active']
    if not groups:
        return fail('Tidak ada group eligible untuk campaign otomatis')
    cid = create_campaign(name=payload.get('name') or 'Auto Campaign', template_id=template_id, sender_pool=sender_pool, target_mode='auto_from_groups', auto_start_enabled=1 if parse_bool(payload.get('auto_start_enabled'), True) else 0)
    count = create_campaign_targets(cid, [int(g['id']) for g in groups], sender_account_id=payload.get('sender_account_id'))
    update_campaign(cid, status='queued', total_targets=count, eligible_targets=count)
    add_audit_log('info', 'campaigns', 'campaign_auto_created', 'Campaign otomatis dibuat dari group eligible', entity_type='campaign', entity_id=str(cid), result='success', payload=json.dumps({'target_count': count}))
    return ok({'campaign_id': cid, 'target_count': count}, 'Campaign otomatis dibuat')


@bp.get('/broadcast-queue/summary')
def broadcast_queue_summary_v2():
    return ok(get_broadcast_queue_summary())


@bp.get('/broadcast-queue')
def broadcast_queue_list_v2():
    page, page_size = pagination_args()
    items, total = get_broadcast_queue(
        campaign_id=parse_int(request.args.get('campaign_id'), 0) or None,
        sender_account_id=request.args.get('sender_account_id'),
        status=request.args.get('status'),
        blocked_only=parse_bool(request.args.get('blocked_only')),
        page=page,
        page_size=page_size,
    )
    return ok({'items': items}, meta={'page': page, 'page_size': page_size, 'total': total})


@bp.get('/broadcast-queue/<int:target_id>')
def queue_target_detail_v2(target_id: int):
    row = get_queue_target(target_id)
    if not row:
        return fail('Target queue tidak ditemukan', 404)
    return ok({
        'campaign_info': {'campaign_id': row['campaign_id'], 'campaign_name': row.get('campaign_name')},
        'group_info': {'group_id': row['group_id'], 'group_name': row.get('group_name')},
        'sender_info': {'sender_account_id': row.get('sender_account_id'), 'sender_name': row.get('sender_name')},
        'eligibility_reason': row.get('eligibility_reason'),
        'attempt_history': [],
        'last_delivery_result': row.get('delivery_result'),
        'suggested_next_action': 'retry' if row.get('status') == 'failed' else 'none',
        'raw': row,
    })


@bp.post('/broadcast-queue/<int:target_id>/retry')
def queue_target_retry_v2(target_id: int):
    row = get_queue_target(target_id)
    if not row:
        return fail('Target tidak ditemukan', 404)
    update_queue_target(target_id, status='queued', next_attempt_at=None, failure_reason=None)
    add_audit_log('info', 'delivery', 'target_retry', 'Target queue diulang', entity_type='campaign_target', entity_id=str(target_id), result='success')
    return ok({}, 'Target masuk queue kembali')


@bp.post('/broadcast-queue/<int:target_id>/move-sender')
def queue_target_move_sender_v2(target_id: int):
    row = get_queue_target(target_id)
    if not row:
        return fail('Target tidak ditemukan', 404)
    payload = body()
    sender_account_id = payload.get('sender_account_id')
    if not sender_account_id:
        return fail('sender_account_id wajib diisi')
    update_queue_target(target_id, sender_account_id=sender_account_id, status='queued')
    add_audit_log('warning', 'delivery', 'target_move_sender', 'Sender target dipindahkan', entity_type='campaign_target', entity_id=str(target_id), result='success', payload=json.dumps({'sender_account_id': sender_account_id}))
    return ok({}, 'Sender target diperbarui')


@bp.post('/broadcast-queue/<int:target_id>/skip')
def queue_target_skip_v2(target_id: int):
    if not get_queue_target(target_id):
        return fail('Target tidak ditemukan', 404)
    payload = body()
    update_queue_target(target_id, status='skipped', failure_reason=payload.get('reason'))
    add_audit_log('warning', 'delivery', 'target_skipped', 'Target queue dilewati', entity_type='campaign_target', entity_id=str(target_id), result='success', payload=json.dumps({'reason': payload.get('reason')}))
    return ok({}, 'Target dilewati')


@bp.post('/broadcast-queue/<int:target_id>/block')
def queue_target_block_v2(target_id: int):
    if not get_queue_target(target_id):
        return fail('Target tidak ditemukan', 404)
    payload = body()
    update_queue_target(target_id, status='blocked', blocked_reason=payload.get('reason'))
    add_audit_log('warning', 'delivery', 'target_blocked', 'Target queue diblok', entity_type='campaign_target', entity_id=str(target_id), result='success', payload=json.dumps({'reason': payload.get('reason')}))
    return ok({}, 'Target diblok')


@bp.post('/broadcast-queue/retry-failed')
def queue_retry_failed_v2():
    items, _ = get_broadcast_queue(status='failed', page=1, page_size=parse_int(body().get('limit'), 50, minimum=1, maximum=500))
    for item in items:
        update_queue_target(int(item['id']), status='queued', failure_reason=None)
    add_audit_log('info', 'delivery', 'retry_failed_targets', 'Retry failed targets dijalankan', entity_type='campaign_target', entity_id='bulk', result='success', payload=json.dumps({'count': len(items)}))
    return ok({'requeued_count': len(items)}, 'Failed targets masuk queue kembali')


@bp.post('/broadcast-queue/pause')
def queue_pause_v2():
    update_banyak({'auto_campaign_enabled': 0})
    add_audit_log('warning', 'delivery', 'queue_paused', 'Broadcast queue dijeda', entity_type='campaign_target', entity_id='global', result='success')
    return ok({}, 'Queue dijeda')


@bp.post('/broadcast-queue/resume')
def queue_resume_v2():
    update_banyak({'pause_all_automation': 0, 'auto_campaign_enabled': 1})
    add_audit_log('info', 'delivery', 'queue_resumed', 'Broadcast queue dilanjutkan', entity_type='campaign_target', entity_id='global', result='success')
    return ok({}, 'Queue dilanjutkan')


@bp.post('/broadcast-queue/rebalance-sender')

def queue_rebalance_sender_v2():
    items, _ = get_broadcast_queue(status='queued', page=1, page_size=500)
    moved = 0
    for item in items:
        sender = item.get('sender_account_id') or item.get('sender_name')
        if sender:
            continue
        row = next((g for g in get_semua_grup() if int(g['id']) == int(item['group_id'])), None)
        sender = row.get('owner_phone') if row else None
        if sender:
            update_queue_target(int(item['id']), sender_account_id=sender)
            moved += 1
    add_audit_log('info', 'delivery', 'queue_rebalanced', 'Rebalance sender diminta', entity_type='campaign_target', entity_id='bulk', result='success', payload=json.dumps({'moved': moved}))
    return ok({'moved': moved}, 'Rebalance sender selesai')


@bp.get('/broadcast-queue/throttle-status')
def get_throttle_status():
    from utils.database import get_conn
    from datetime import datetime as _dt
    conn = get_conn()
    try:
        row = conn.execute('SELECT * FROM broadcast_throttle WHERE id=1').fetchone()
        data = dict(row) if row else {}
        next_allowed = data.get('next_allowed_at')
        if next_allowed:
            try:
                next_dt = _dt.strptime(str(next_allowed)[:19], '%Y-%m-%d %H:%M:%S')
                sisa_detik = max(0, int((next_dt - _dt.now()).total_seconds()))
                data['sisa_detik'] = sisa_detik
                data['siap'] = sisa_detik <= 0
            except Exception:
                data['sisa_detik'] = 0
                data['siap'] = True
        else:
            data['sisa_detik'] = 0
            data['siap'] = True
        return ok(data)
    except Exception as e:
        return fail(str(e))
    finally:
        conn.close()


@bp.post('/broadcast-queue/reset-throttle')
def queue_reset_throttle():
    from utils.database import get_conn
    conn = get_conn()
    try:
        conn.execute("UPDATE broadcast_throttle SET next_allowed_at=NULL, last_broadcast_at=NULL WHERE id=1")
        conn.commit()
        add_audit_log('info', 'delivery', 'reset_broadcast_throttle', 'Throttle broadcast direset manual', entity_type='broadcast_throttle', entity_id='1', result='success')
        return ok({}, 'Throttle broadcast berhasil direset')
    except Exception as e:
        return fail(str(e))
    finally:
        conn.close()


@bp.post('/akun/<phone>/reset-join-throttle')
def reset_join_throttle(phone):
    from utils.database import get_conn
    conn = get_conn()
    try:
        phone = phone.strip()
        r = conn.execute("UPDATE akun SET next_join_at=NULL WHERE phone=%s", (phone,))
        conn.commit()
        if r.rowcount == 0:
            return fail(f'Akun {phone} tidak ditemukan')
        add_audit_log('info', 'join', 'reset_join_throttle', f'Throttle join {phone} direset manual', entity_type='akun', entity_id=phone, result='success')
        return ok({'phone': phone}, f'Throttle join {phone} berhasil direset')
    except Exception as e:
        return fail(str(e))
    finally:
        conn.close()


@bp.post('/broadcast-queue/reset-stuck')
def queue_reset_stuck():
    """Reset semua target queued yang next_attempt_at-nya stuck."""
    from utils.database import get_conn
    conn = get_conn()
    try:
        # Reset queued yang punya next_attempt_at (stuck menunggu)
        r2 = conn.execute(
            """UPDATE campaign_target
               SET next_attempt_at=NULL, hold_reason=NULL
               WHERE status='queued'
                 AND next_attempt_at IS NOT NULL""",
        )
        # Reset failed yang belum final
        r3 = conn.execute(
            """UPDATE campaign_target
               SET status='queued', next_attempt_at=NULL, failure_reason=NULL, finalized_at=NULL
               WHERE status='failed'
                 AND finalized_at IS NULL""",
        )
        # Reset grup broadcast_status yang stuck di queued/hold
        r4 = conn.execute(
            """UPDATE grup
               SET broadcast_status='broadcast_eligible', broadcast_hold_reason=NULL, broadcast_ready_at=NULL
               WHERE broadcast_status IN ('queued','hold')
                 AND assignment_status='managed'""",
        )
        conn.commit()
        total = r2.rowcount + r3.rowcount
        add_audit_log('info', 'delivery', 'reset_stuck_targets',
                      f'Reset {total} target stuck, {r4.rowcount} grup direset',
                      entity_type='campaign_target', entity_id='bulk', result='success',
                      payload=json.dumps({'queued_reset': r2.rowcount, 'failed_reset': r3.rowcount, 'grup_reset': r4.rowcount}))
        return ok({'reset_count': total, 'grup_reset': r4.rowcount},
                  f'{total} target + {r4.rowcount} grup direset ke antrian')
    except Exception as e:
        return fail(str(e))
    finally:
        conn.close()
